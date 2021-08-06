import * as R from 'rambda';
import moment from 'moment';
import { KEYSPACE } from '../constants';
import { types as CassandraTypes } from 'cassandra-driver';
import { cassandraClient, toLong } from '../database/cassandra.database';
import { topHeight, topTxIndex } from '../database/sync.database';
import graphqlFields from 'graphql-fields';
import { config } from 'dotenv';
import {
  QueryTransactionsArgs,
  QueryBlockArgs,
  QueryBlocksArgs,
} from './types';
import {
  ownerToAddress,
  ISO8601DateTimeString,
  winstonToAr,
  utf8DecodeTag,
} from '../utility/encoding.utility';
import { TransactionHeader } from '../types/arweave.types';
import {
  QueryParams,
  generateTransactionQuery,
  generateBlockQuery,
  generateDeferedTxQuery,
  generateDeferedTxBlockQuery,
  generateDeferedBlockQuery,
  generateTagQuery,
} from './query.graphql';
import * as DbMapper from '../database/mapper.database';

process.env.NODE_ENV !== 'test' && config();

function sortByTxIndexAsc(
  result1: { tx_index: CassandraTypes.Long },
  result2: { tx_index: CassandraTypes.Long }
) {
  return result2.tx_index.compare(result1.tx_index);
}

function sortByTxIndexDesc(
  result1: { tx_index: CassandraTypes.Long },
  result2: { tx_index: CassandraTypes.Long }
) {
  return result1.tx_index.compare(result2.tx_index);
}

const DEFAULT_PAGE_SIZE = parseInt(process.env.DEFAULT_PAGE_SIZE || '10');
const MAX_PAGE_SIZE = parseInt(process.env.MAX_PAGE_SIZE || '100');

interface FieldMap {
  indep_hash: string;
  id: string;
  tx_id: string;
  anchor: string;
  recipient: string;
  tags: any[];
  fee: string;
  height: CassandraTypes.Long;
  quantity: string;
  data_size: number;
  data_type: string;
  parent: FieldMap;
  owner: string;
  owner_address: string;
  signature: string;
  timestamp: CassandraTypes.Long;
  previous: string;
  block: any;
  block_id: string;
  block_timestamp: string;
  block_height: string;
  block_previous: string;
}

const fieldMap = {
  id: 'transactions.id',
  anchor: 'transactions.last_tx',
  recipient: 'transactions.target',
  tags: 'transactions.tags',
  fee: 'transactions.reward',
  quantity: 'transactions.quantity',
  data_size: 'transactions.data_size',
  data_type: 'transactions.content_type',
  parent: 'transactions.parent',
  owner: 'transactions.owner',
  owner_address: 'transactions.owner_address',
  // signature: 'transactions.signature',
  block_id: 'blocks.id',
  block_timestamp: 'blocks.mined_at',
  block_height: 'blocks.height',
  block_previous: 'blocks.previous_block',
};

const edgeFieldMapTx = {
  'edges.node.id': 'tx_id',
  'edges.node.last_tx': 'anchor',
  'edges.node.target': 'recipient',
  'edges.node.tags': 'tags',
  // 'edges.node.reward': 'fee',
  'edges.node.quantity': 'quantity',
  'edges.node.data_size': 'data_size',
  'edges.node.content_type': 'data_type',
  'edges.node.parent': 'parent',
  'edges.node.owner': 'owner',
  // 'edges.node.owner_address': 'owner_address',
};

const edgeFieldMapBlock = {
  'edges.node.id': 'indep_hash',
  'edges.node.timestamp': 'timestamp',
  'edges.node.height': 'height',
  'edges.node.previous': 'previous_block',
};

const blockFieldMap = {
  id: 'blocks.indep_hash',
  timestamp: 'blocks.mined_at',
  height: 'blocks.height',
  // extended: 'blocks.extended',
};

const hydrateGqlTx = async (tx) => {
  const block = await DbMapper.txIdToBlockMapper.get({
    tx_id: tx.id,
  });
  const tags = await DbMapper.tagsByTxId(tx.id);
  const hydrated = R.reduce((acc: FieldMap, key: string) => {
    const txKeyKey = fieldMap[key].split('.');
    const txScope = txKeyKey[0];
    const txKey = txKeyKey[1];
    const val =
      txScope === 'blocks'
        ? (block[txKey] || '').toString()
        : (tx[txKey] || '').toString();
    return R.assoc(key, val, acc);
  }, {} as FieldMap)(R.keys(fieldMap));
  return R.assoc('tags', tags, hydrated);
};

const resolveGqlTxSelect = (
  userFields: any,
  singleTx: boolean = false
): string[] => {
  const select = [];
  R.keys(edgeFieldMapTx).forEach((keyPath) => {
    if (
      R.hasPath(
        singleTx ? keyPath.replace('edges.node.', '') : keyPath,
        userFields
      )
    ) {
      select.push(edgeFieldMapTx[keyPath]);
    }
  });
  return select;
};

const resolveGqlBlockSelect = (userFields: any): string[] => {
  const select: string[] = [];

  R.keys(edgeFieldMapBlock).forEach((keyPath) => {
    if (R.hasPath(keyPath as string, userFields)) {
      select.push(edgeFieldMapBlock[keyPath]);
    }
  });

  return select;
};

export const resolvers = {
  Query: {
    transaction: async (
      parent: FieldMap,
      queryParams: any,
      { req, connection }: any,
      info: any
    ) => {
      const { timestamp, offset } = parseCursor(
        queryParams.after || newCursor()
      );
      const fieldsWithSubFields = graphqlFields(info);

      const fetchSize =
        Math.min(queryParams.first || DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE) + 1;

      let ids: Array<string> = [];
      let minHeight = toLong(0);
      let maxHeight = toLong(topTxIndex);

      if (queryParams.block && queryParams.block.min) {
        minHeight = toLong(queryParams.block.min).mul(1000);
      }

      if (queryParams.block && queryParams.block.max) {
        maxHeight = toLong(queryParams.block.max).mul(1000);
      }

      const params: any = {
        id: queryParams.id || undefined,
        select: resolveGqlTxSelect(fieldsWithSubFields, true),
      };
      // No selection = no search
      if (R.isEmpty(params.select)) {
        return { data: { transaction: null } };
      }
      // todo, elide selectors not selected from user
      if (!params.select.includes('tx_id')) {
        params.select = R.append('tx_id', params.select);
      }

      params.select = R.append('tx_index', params.select);
      const txQuery = generateTransactionQuery(params);

      let { rows: resultArray } = await cassandraClient.execute(
        txQuery.query,
        txQuery.params,
        { prepare: true, executionProfile: 'gql' }
      );

      if (R.isEmpty(resultArray)) {
        return null;
      }

      const result = resultArray[0];

      if (fieldsWithSubFields.block !== undefined) {
        let selectParams = [];
        const userSelectKeys = R.keys(fieldsWithSubFields.block);
        ['id', 'timestamp', 'height', 'previous'].forEach((selectKey) => {
          if (userSelectKeys.includes(selectKey)) {
            switch (selectKey) {
              case 'id': {
                selectParams = R.append('indep_hash', selectParams);
                break;
              }
              case 'previous': {
                selectParams = R.append('previous_block', selectParams);
                break;
              }
              default: {
                selectParams = R.append(selectKey, selectParams);
              }
            }
          }
        });

        const blockQuery = generateDeferedTxBlockQuery(
          result.tx_index.divide(1000),
          selectParams
        );

        let { rows: blockResult } = await cassandraClient.execute(
          blockQuery.query,
          blockQuery.params,
          {
            prepare: true,
            executionProfile: 'gql',
          }
        );
        if (R.isEmpty(blockResult)) {
          result.block = null;
        } else {
          result.block = blockResult[0];
        }
      }

      const selectedDeferedKeysUser = [];
      R.keys(fieldsWithSubFields).forEach(
        (k: any) =>
          ['anchor', 'fee', 'signature'].includes(k) &&
          selectedDeferedKeysUser.push(
            R.find(R.equals(k))(['anchor', 'fee', 'signature'])
          )
      );
      if (!R.isEmpty(selectedDeferedKeysUser)) {
        const selectedDeferedKeysDb = [];
        selectedDeferedKeysUser.forEach((k) => {
          switch (k) {
            case 'anchor': {
              selectedDeferedKeysDb.push('last_tx');
              break;
            }
            case 'fee': {
              selectedDeferedKeysDb.push('reward');
              break;
            }
            default: {
              selectedDeferedKeysDb.push(k);
            }
          }
        });

        const deferedTxQ = generateDeferedTxQuery({
          deferedSelect: selectedDeferedKeysDb,
          tx_id: result.tx_id,
        });

        let { rows: deferedTxResult } = await cassandraClient.execute(
          deferedTxQ.query,
          deferedTxQ.params,
          {
            prepare: true,
            executionProfile: 'gql',
          }
        );
        if (deferedTxResult[0].last_tx) {
          result.anchor = deferedTxResult[0].last_tx || '';
        }
        if (deferedTxResult[0].reward) {
          result.fee = deferedTxResult[0].reward || '';
        }
        if (deferedTxResult[0].signature) {
          result.signature = deferedTxResult[0].signature || '';
        }
      }

      return result as any;
    },
    transactions: async (
      parent: string,
      queryParams: QueryTransactionsArgs,
      { req, connection }: any,
      info: any
    ) => {
      const { timestamp, offset } = parseCursor(
        queryParams.after || newCursor()
      );
      const fieldsWithSubFields = graphqlFields(info);

      const fetchSize =
        Math.min(queryParams.first || DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE) + 1;

      let ids: Array<string> = [];
      let minHeight = toLong(0);
      let maxHeight = toLong(topTxIndex);

      if (queryParams.block && queryParams.block.min) {
        minHeight = toLong(queryParams.block.min).mul(1000);
      }

      if (queryParams.block && queryParams.block.max) {
        maxHeight = toLong(queryParams.block.max).mul(1000);
      }

      const selectsBlock = R.hasPath('edges.node.block', fieldsWithSubFields);
      const params: Partial<Omit<QueryParams, 'after'> & { before: string }> = {
        limit: fetchSize,
        offset: offset,
        ids: queryParams.ids || undefined,
        to: queryParams.recipients || undefined,
        from: queryParams.owners || undefined,
        tags: queryParams.tags || undefined,
        blocks: true,
        before: timestamp,
        select: resolveGqlTxSelect(fieldsWithSubFields, false),
        minHeight,
        maxHeight,
        sortOrder: queryParams.sort || undefined,
      };

      // No selection = no search
      if (R.isEmpty(params.select) && !selectsBlock) {
        return {
          pageInfo: {
            hasNextPage: false,
          },
          edges: [],
        };
      }

      // todo, elide selectors not selected from user
      if (!params.select.includes('tx_id')) {
        params.select = R.append('tx_id', params.select);
      }

      params.select = R.append('tx_index', params.select);

      const txQuery = generateTransactionQuery(params);

      let { rows: result } = await cassandraClient.execute(
        txQuery.query,
        txQuery.params,
        { prepare: true, executionProfile: 'gql' }
      );

      let hasNextPage = false;

      if (result.length === fetchSize) {
        hasNextPage = true;
        result = R.dropLast(1, result);
      }

      if (selectsBlock) {
        let selectParams = [];
        // let resultWithBlock = [];
        const userSelectKeys = Object.keys(
          R.path('edges.node.block', fieldsWithSubFields)
        );
        ['id', 'timestamp', 'height', 'previous'].forEach((selectKey) => {
          if (userSelectKeys.includes(selectKey)) {
            switch (selectKey) {
              case 'id': {
                selectParams = R.append('indep_hash', selectParams);
                break;
              }
              case 'previous': {
                selectParams = R.append('previous_block', selectParams);
                break;
              }
              default: {
                selectParams = R.append(selectKey, selectParams);
              }
            }
          }
        });
        for (const res of result) {
          const userSelectKeys = R.keys(fieldsWithSubFields.edges.node.block);

          const blockQuery = generateDeferedTxBlockQuery(
            res.tx_index.divide(1000),
            selectParams
          );

          let { rows: blockResult } = await cassandraClient.execute(
            blockQuery.query,
            blockQuery.params,
            {
              prepare: true,
              executionProfile: 'gql',
            }
          );

          if (R.isEmpty(blockResult)) {
            res.block = null;
          } else {
            res.block = blockResult[0];
          }
        }
      }

      const selectedDeferedKeysUser = [];
      R.keys(fieldsWithSubFields.edges.node).forEach(
        (k: any) =>
          ['anchor', 'fee', 'signature'].includes(k) &&
          selectedDeferedKeysUser.push(
            R.find(R.equals(k))(['anchor', 'fee', 'signature'])
          )
      );
      if (!R.isEmpty(selectedDeferedKeysUser)) {
        const selectedDeferedKeysDb = [];
        selectedDeferedKeysUser.forEach((k) => {
          switch (k) {
            case 'anchor': {
              selectedDeferedKeysDb.push('last_tx');
              break;
            }
            case 'fee': {
              selectedDeferedKeysDb.push('reward');
              break;
            }
            default: {
              selectedDeferedKeysDb.push(k);
            }
          }
        });

        for (const tx of result) {
          const deferedTxQ = generateDeferedTxQuery({
            deferedSelect: selectedDeferedKeysDb,
            tx_id: tx.tx_id,
          });

          let { rows: deferedTxResult } = await cassandraClient.execute(
            deferedTxQ.query,
            deferedTxQ.params,
            {
              prepare: true,
              executionProfile: 'gql',
            }
          );

          if (deferedTxResult[0].last_tx) {
            tx.anchor = deferedTxResult[0].last_tx || '';
          }
          if (deferedTxResult[0].reward) {
            tx.fee = deferedTxResult[0].reward || '';
          }
          if (deferedTxResult[0].signature) {
            tx.signature = deferedTxResult[0].signature || '';
          }
        }
      }

      return {
        pageInfo: {
          hasNextPage,
        },
        edges: R.sort(
          params.select.sort === 'HEIGHT_ASC'
            ? (sortByTxIndexAsc as any)
            : (sortByTxIndexDesc as any),
          result as any
        ).map((tx, index) => ({
          cursor: encodeCursor({ timestamp, offset: offset + index + 1 }),
          node: tx,
        })),
      };
    },
    block: async (
      parent: string,
      queryParams: QueryBlockArgs,
      { req, connection }: any
    ) => {
      if (queryParams.id) {
        return (
          await generateBlockQuery({
            select: blockFieldMap,
            id: queryParams.id,
            offset: 0,
            fetchSize: 100,
          })
        ).first();
      } else {
        return null;
      }
    },
    blocks: async (
      parent: FieldMap,
      queryParams: QueryBlocksArgs,
      { req, connection }: any,
      info: any
    ) => {
      const fieldsWithSubFields = graphqlFields(info);

      const { timestamp, offset } = parseCursor(
        queryParams.after || newCursor()
      );
      const fetchSize =
        Math.min(queryParams.first || DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE) + 1;

      let ids: Array<string> = [];
      let minHeight = toLong(0);
      let maxHeight = toLong(topHeight);

      if (queryParams.ids) {
        ids = queryParams.ids;
      }

      if (queryParams.height && queryParams.height.min) {
        minHeight = toLong(queryParams.height.min);
      }

      if (queryParams.height && queryParams.height.max) {
        maxHeight = toLong(queryParams.height.max);
      }

      const select = resolveGqlBlockSelect(fieldsWithSubFields);

      // No selection = no search
      if (R.isEmpty(select)) {
        return {
          pageInfo: {
            hasNextPage: false,
          },
          edges: [],
        };
      }

      const blockQuery = generateBlockQuery({
        ids,
        select,
        minHeight,
        maxHeight,
        before: timestamp,
        offset,
        fetchSize,
        sortOrder: queryParams.sort || undefined,
      });

      let hasNextPage = false;

      let { rows: result } = await cassandraClient.execute(
        blockQuery.query,
        blockQuery.params,
        { prepare: true, executionProfile: 'gql' }
      );

      if (result.length === fetchSize) {
        hasNextPage = true;
        result = R.dropLast(1, result);
      }

      return {
        pageInfo: {
          hasNextPage,
        },
        edges: result.map((block, index) => ({
          cursor: encodeCursor({ timestamp, offset: offset + index + 1 }),
          node: block,
        })),
      };
    },
  },
  Transaction: {
    id: (parent: FieldMap) => {
      return parent.tx_id;
    },
    anchor: (parent: any) => {
      return parent.anchor || '';
    },
    signature: (parent: any) => {
      return parent.signature || '';
    },
    tags: (parent: FieldMap) => {
      return parent.tags.map(utf8DecodeTag);
    },
    recipient: (parent: FieldMap) => {
      return parent.recipient.trim() || '';
    },
    data: (parent: FieldMap) => {
      return {
        size: parent.data_size || 0,
        type: parent.data_type,
      };
    },
    quantity: (parent: FieldMap) => {
      return {
        ar: winstonToAr(parent.quantity || ('0' as const)),
        winston: parent.quantity || '0',
      };
    },
    fee: (parent: FieldMap) => {
      return {
        ar: winstonToAr(parent.fee || '0'),
        winston: parent.fee || '0',
      };
    },
    block: (parent: FieldMap) => {
      return parent.block;
      // if (parent.tx_id) {
      //   return parent.block;
      // } else if (parent.block_id) {
      //   return {
      //     id: parent.block_id,
      //     previous: parent.block_previous,
      //     timestamp: moment(parent.block_timestamp).unix(),
      //     height: parent.block_height,
      //   };
      // }
    },
    owner: (parent: FieldMap) => {
      return {
        address: ownerToAddress(parent.owner),
        key: parent.owner,
      };
    },
    parent: (parent: FieldMap) => {
      if (parent.parent) {
        return {
          id: parent.parent,
        };
      }
    },
  },
  Block: {
    /*
    reward: (parent) => {
      return {
        address: parent.extended.reward_addr,
        pool: parent.extended.reward_pool,
      };
    },
    size: (parent) => {
      return parent.extended?.block_size;
    },
    */
    height: (parent: FieldMap) => {
      return parent.height.toInt();
    },
    id: (parent: FieldMap) => {
      return parent.indep_hash;
    },
    previous: (parent: FieldMap) => {
      return parent.previous;
    },
    timestamp: (parent: FieldMap) => {
      return parent.timestamp.toString();
    },
  },
};

export interface Cursor {
  timestamp: string;
  offset: number;
}

export function newCursor(): string {
  return encodeCursor({
    timestamp: moment(new Date()).unix().toString(),
    offset: 0,
  });
}

export function encodeCursor({ timestamp, offset }: Cursor): string {
  const string = JSON.stringify([timestamp, offset]);
  return Buffer.from(string).toString('base64');
}

export function parseCursor(cursor: string): Cursor {
  try {
    const [timestamp, offset] = JSON.parse(
      Buffer.from(cursor, 'base64').toString()
    ) as [string, number];
    return { timestamp, offset };
  } catch (error) {
    throw new Error('invalid cursor');
  }
}
