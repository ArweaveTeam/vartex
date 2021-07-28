import * as R from 'rambda';
import moment from 'moment';
import { types as CassandraTypes } from 'cassandra-driver';
import { cassandraClient } from '../database/cassandra.database.js';
import graphqlFields from 'graphql-fields';
import { config } from 'dotenv';
import {
  QueryTransactionsArgs,
  QueryBlockArgs,
  QueryBlocksArgs,
} from './types.js';
import {
  ISO8601DateTimeString,
  winstonToAr,
  utf8DecodeTag,
} from '../utility/encoding.utility.js';
import { TransactionHeader } from '../types/arweave.types.js';
import {
  QueryParams,
  generateTransactionQuery,
  generateBlockQuery,
  generateTagQuery,
} from './query.graphql.js';
import * as DbMapper from '../database/mapper.database.js';

config();

const DEFAULT_PAGE_SIZE = parseInt(process.env.DEFAULT_PAGE_SIZE || '10');
const MAX_PAGE_SIZE = parseInt(process.env.MAX_PAGE_SIZE || '100');

interface FieldMap {
  id: string;
  anchor: string;
  recipient: string;
  tags: any[];
  fee: string;
  quantity: string;
  data_size: number;
  data_type: string;
  parent: FieldMap;
  owner: string;
  owner_address: string;
  signature: string;
  timestamp: number;
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
  signature: 'transactions.signature',
  block_id: 'blocks.id',
  block_timestamp: 'blocks.mined_at',
  block_height: 'blocks.height',
  block_previous: 'blocks.previous_block',
};

const edgeFieldMapTx = {
  'edges.node.id': 'id',
  'edges.node.last_tx': 'anchor',
  'edges.node.target': 'recipient',
  'edges.node.tags': 'id', // note, tags are stored elsewhere, but for getting those we need the txid
  'edges.node.reward': 'fee',
  'edges.node.quantity': 'quantity',
  'edges.node.data_size': 'data_size',
  'edges.node.content_type': 'data_type',
  'edges.node.parent': 'parent',
  'edges.node.owner': 'owner',
  'edges.node.owner_address': 'owner_address',
  'edges.node.signature': 'signature',
};

const edgeFieldMapBlock = {
  'edges.node.id': 'indep_hash',
  'edges.node.timestamp': 'timestamp',
  'edges.node.height': 'height',
  'edges.node.previous': 'previous_block',
};

const blockFieldMap = {
  id: 'blocks.id',
  timestamp: 'blocks.mined_at',
  height: 'blocks.height',
  previous: 'blocks.previous_block',
  extended: 'blocks.extended',
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

const resolveGqlTxSelect = (userFields: any): string[] => {
  const select = [];
  R.keys(edgeFieldMapTx).forEach((keyPath) => {
    if (R.hasPath(keyPath, userFields)) {
      select.push(edgeFieldMapTx[keyPath]);
    }
  });
  return select;
};

const resolveGqlBlockSelect = (userFields: any): string[] => {
  const select = [];
  R.keys(edgeFieldMapBlock).forEach((keyPath) => {
    if (R.hasPath(keyPath, userFields)) {
      select.push(edgeFieldMapBlock[keyPath]);
    }
  });
  return select;
};

const runPaginatedSearch = ({
  fetchSize,
  query,
  offset,
}: {
  fetchSize: number;
  query: { query: string; params: any[] };
  offset: number;
}): Promise<{ result: any; hasNextPage: boolean }> => {
  const result = [];
  let cnt = -1;
  return new Promise(
    (resolve: (val?: any) => void, reject: (err: string) => void) => {
      const stream = cassandraClient.stream(query.query, query.params, {
        autoPage: false,
        prepare: true,
      });
      stream.on('readable', function streamReadable() {
        let item = '';
        if (cnt < offset + fetchSize) {
          while ((item = (stream as any).read())) {
            if (offset < cnt && cnt < offset + fetchSize) {
              result.push(item);
            }
            cnt += 1;
          }
        }
      });
      stream.on('end', function streamEnd() {
        resolve({ hasNextPage: cnt >= offset + fetchSize, result });
      });
      stream.on('error', function onError(reason: any) {
        reject((reason || '').toString());
      });
      // })
      //   function (n, row) {
      //     if (n + 1 > offset) {
      //       console.log(
      //         row.height.toInt(),
      //         'bigger than last?',
      //         last < row.height.toInt()
      //       );
      //       last = row.height.toInt();
      //       result.push(row);
      //     }
      //   },
      //   function (err, res) {
      //     if (err) {

      //     } else {
      //       const hasNextPage = res.nextPage !== undefined;

      //     }
      //   }
      // );
    }
  );
};

export const resolvers = {
  Query: {
    transaction: async (
      parent: FieldMap,
      queryParams: { id: string },
      { req, connection }: any
    ) => {
      const tx = await DbMapper.transactionMapper.get({ id: queryParams.id });
      // map tx to fieldMap
      if (!tx) {
        throw new Error(`id: "${queryParams.id}" does not exist!`);
      } else {
        return await hydrateGqlTx(tx);
      }
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

      const fetchSize = Math.min(
        queryParams.first || DEFAULT_PAGE_SIZE,
        MAX_PAGE_SIZE
      );

      let maybeResolvedTags;

      if (queryParams.tags !== undefined) {
        const searchTagsQuery = generateTagQuery(queryParams.tags);
        const tagsResult = await cassandraClient.execute(
          searchTagsQuery.query,
          searchTagsQuery.params,
          { prepare: true }
        );
        if (!R.isEmpty(tagsResult.rows)) {
          maybeResolvedTags = tagsResult.rows.map((t) => t.tx_id);
        }
      }

      const ids = maybeResolvedTags
        ? queryParams.ids || undefined
        : R.concat(queryParams.ids || [], maybeResolvedTags);

      const params: Partial<Omit<QueryParams, 'after'> & { before: string }> = {
        limit: fetchSize + 1,
        offset: offset,
        ids,
        to: queryParams.recipients || undefined,
        from: queryParams.owners || undefined,
        tags: queryParams.tags || undefined,
        blocks: true,
        before: timestamp,
        select: resolveGqlTxSelect(fieldsWithSubFields),
        minHeight: queryParams.block?.min || undefined,
        maxHeight: queryParams.block?.max || undefined,
        sortOrder: queryParams.sort || undefined,
      };

      // No selection = no search
      if (R.isEmpty(params.select)) {
        return {
          pageInfo: {
            hasNextPage: false,
          },
          edges: {},
        };
      }
      const txQuery = generateTransactionQuery(params);

      const { result, hasNextPage = false } = await runPaginatedSearch({
        query: R.dissoc('tags', txQuery),
        fetchSize,
        offset,
      });

      const resultWithTags = await Promise.all(
        result.map(async (tx) =>
          R.assoc(
            'tags',
            txQuery.tags !== undefined
              ? await DbMapper.tagsByTxId(result.id)
              : [],
            tx
          )
        )
      );

      return {
        pageInfo: {
          hasNextPage,
        },
        edges: resultWithTags.map((tx, index) => ({
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
      const fetchSize = Math.min(
        queryParams.first || DEFAULT_PAGE_SIZE,
        MAX_PAGE_SIZE
      );

      let ids: Array<string> = [];
      let minHeight = 0;
      let maxHeight = MAX_PAGE_SIZE;

      if (queryParams.ids) {
        ids = queryParams.ids;
      }

      if (queryParams.height && queryParams.height.min) {
        minHeight = queryParams.height.min;
      }

      if (queryParams.height && queryParams.height.max) {
        maxHeight = queryParams.height.max;
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
        sortOrder: queryParams.sort || undefined,
      });

      const { result, hasNextPage = false } = await runPaginatedSearch({
        query: blockQuery,
        fetchSize,
        offset,
      });

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
    tags: (parent: FieldMap) => {
      return parent.tags.map(utf8DecodeTag);
    },
    recipient: (parent: FieldMap) => {
      return parent.recipient.trim();
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
      if (parent.block_id) {
        return {
          id: parent.block_id,
          previous: parent.block_previous,
          timestamp: moment(parent.block_timestamp).unix(),
          height: parent.block_height,
        };
      }
    },
    owner: (parent: FieldMap) => {
      return {
        address: parent.owner_address,
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
    timestamp: (parent: FieldMap) => {
      return parent?.timestamp.toString();
    },
  },
};

export interface Cursor {
  timestamp: string;
  offset: number;
}

export function newCursor(): string {
  return encodeCursor({
    timestamp: CassandraTypes.TimeUuid.now().toString(),
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
