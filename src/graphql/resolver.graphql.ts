import * as R from 'rambda';
import moment from 'moment';
import { types as CassandraTypes } from 'cassandra-driver';
import { cassandraClient, toLong } from '../database/cassandra.database.js';
import { topHeight, topTxIndex } from '../database/sync.database.js';
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
  generateDeferedBlockQuery,
  generateTagQuery,
} from './query.graphql.js';
import * as DbMapper from '../database/mapper.database.js';

config();

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
  quantity: string;
  data_size: number;
  data_type: string;
  parent: FieldMap;
  owner: string;
  owner_address: string;
  signature: string;
  timestamp: number | CassandraTypes.TimeUuid;
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
  'edges.node.id': 'tx_id',
  'edges.node.last_tx': 'anchor',
  'edges.node.target': 'recipient',
  'edges.node.tags': 'tags', // note, tags are stored elsewhere, but for getting those we need the txid
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
};

const edgeFieldDeferedMapBlock = {
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

const resolveGqlTxSelect = (userFields: any): string[] => {
  const select = [];
  R.keys(edgeFieldMapTx).forEach((keyPath) => {
    if (R.hasPath(keyPath, userFields)) {
      select.push(edgeFieldMapTx[keyPath]);
    }
  });
  return select;
};

const resolveGqlBlockSelect = (userFields: any): string[][] => {
  const select: string[] = [];
  const deferedSelect: string[] = [];
  [
    [select, edgeFieldMapBlock],
    [deferedSelect, edgeFieldDeferedMapBlock],
  ].forEach(([arr, fieldMap]: [string[], any]) => {
    R.keys(fieldMap).forEach((keyPath) => {
      if (R.hasPath(keyPath as string, userFields)) {
        arr.push(fieldMap[keyPath]);
      }
    });
  });

  return [select, deferedSelect];
};

// const runPaginatedSearch = ({
//   fetchSize,
//   query,
//   offset,
// }: {
//   fetchSize: number;
//   query: { query: string; params: any[] };
//   offset: number;
// }): Promise<{ result: any; hasNextPage: boolean }> => {
//   const result = [];
//   let last = 0;
//   return new Promise(
//     (resolve: (val?: any) => void, reject: (err: string) => void) => {
//       cassandraClient.eachRow(
//         query.query,
//         query.params,
//         {
//           autoPage: false,
//           fetchSize,
//           prepare: true,
//           executionProfile: 'gql',
//         },
//         function (n, row) {
//           if (n + 1 > offset) {
//             console.log(
//               row.height.toInt(),
//               'bigger than last?',
//               last < row.height.toInt()
//             );
//             last = row.height.toInt();
//             result.push(row);
//           }
//         },
//         function (err, res) {
//           if (err) {
//             reject((err || '').toString());
//           } else {
//             const hasNextPage = res.nextPage !== undefined;
//             resolve({ hasNextPage, result });
//           }
//         }
//       );
//     }
//   );
// };

// const runPaginatedSearch = ({
//   fetchSize,
//   query,
//   offset,
// }: {
//   fetchSize: number;
//   query: { query: string; params: any[] };
//   offset: number;
// }): Promise<{ result: any; hasNextPage: boolean }> => {
//   const result = [];
//   let cnt = -1;

//   return new Promise(
//     (resolve: (val?: any) => void, reject: (err: string) => void) => {
//       const stream: any = cassandraClient.stream(query.query, query.params, {
//         autoPage: true,
//         prepare: true,
//       });
//       stream.on('readable', function streamReadable() {
//         let item = '';
//         // console.log(stream, Object.keys(stream));
//         if (cnt < offset + fetchSize) {
//           while ((item = (stream as any).read())) {
//             if (offset < cnt && cnt < offset + fetchSize) {
//               console.error(item);
//               result.push(item);
//               stream.pause();
//               return true;
//             } else {
//               console.error('DONE', result);
//               resolve({ hasNextPage: false, result });
//               stream.pause();
//               return false;
//               // stream.close();
//             }
//             cnt += 1;
//           }
//         } else {
//           console.error('DONE', result);
//           stream.pause();
//           resolve({ hasNextPage: false, result });
//           return false;
//           // stream.close();
//         }
//       });
//       stream.on('end', function streamEnd() {
//         console.error('END');
//         resolve({ hasNextPage: cnt >= offset + fetchSize, result });
//       });
//       stream.on('error', function onError(reason: any) {
//         console.error('ERROR');
//         reject((reason || '').toString());
//       });
//     }
//   );
// };

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

      let ids: Array<string> = [];
      let minHeight = toLong(0);
      let maxHeight = toLong(topTxIndex);

      if (queryParams.block && queryParams.block.min) {
        minHeight = toLong(queryParams.block.min).mul(1000);
      }

      if (queryParams.block && queryParams.block.max) {
        maxHeight = toLong(queryParams.block.max).mul(1000);
      }

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

      const params: Partial<Omit<QueryParams, 'after'> & { before: string }> = {
        limit: fetchSize + 1,
        offset: offset,
        ids: queryParams.ids || undefined,
        to: queryParams.recipients || undefined,
        from: queryParams.owners || undefined,
        tags: queryParams.tags || undefined,
        blocks: true,
        before: timestamp,
        select: resolveGqlTxSelect(fieldsWithSubFields),
        minHeight,
        maxHeight,
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

      let { rows: result } = await cassandraClient.execute(
        txQuery.query,
        txQuery.params,
        { prepare: true, executionProfile: 'gql' }
      );

      let hasNextPage = false;

      const resultWithTags = await Promise.all(
        result.map(async (tx) =>
          R.assoc(
            'tags',
            txQuery.tags !== undefined ? await DbMapper.tagsByTxId(tx.id) : [],
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
      const fetchSize = Math.min(
        queryParams.first || DEFAULT_PAGE_SIZE,
        MAX_PAGE_SIZE
      );

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
      const [select, deferedSelect] = resolveGqlBlockSelect(
        fieldsWithSubFields
      );

      // No selection = no search
      if (R.isEmpty(select) && R.isEmpty(deferedSelect)) {
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
      const hasNextPage = false;

      let { rows: result } = await cassandraClient.execute(
        blockQuery.query,
        blockQuery.params,
        { prepare: true, executionProfile: 'gql' }
      );

      if (!R.isEmpty(deferedSelect)) {
        result = await Promise.all(
          result.map(async (row) => {
            const deferedBlockQuery = generateDeferedBlockQuery({
              deferedSelect,
              indep_hash: row.indep_hash,
            });

            const {
              rows: deferedResult,
            } = await cassandraClient.execute(
              deferedBlockQuery.query,
              deferedBlockQuery.params,
              { prepare: true, executionProfile: 'gql' }
            );
            for (const key of R.keys(deferedResult[0])) {
              switch (key) {
                case 'previous_block': {
                  row['previous'] = deferedResult[0][key];
                  break;
                }
                default: {
                }
              }
            }
            return row;
          })
        );
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
    id: (parent: FieldMap) => {
      return parent.indep_hash;
    },
    timestamp: (parent: FieldMap) => {
      return moment(
        (parent?.timestamp as CassandraTypes.TimeUuid).getDate()
      ).unix();
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
