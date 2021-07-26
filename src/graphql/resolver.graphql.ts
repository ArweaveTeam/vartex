import * as R from 'rambda';
import moment from 'moment';
import { config } from 'dotenv';
import {
  QueryTransactionsArgs,
  QueryBlockArgs,
  QueryBlocksArgs,
} from './types';
import {
  ISO8601DateTimeString,
  winstonToAr,
  utf8DecodeTag,
} from '../utility/encoding.utility';
import { TransactionHeader } from '../types/arweave.types';
import {
  QueryParams,
  generateQuery,
  generateBlockQuery,
} from './query.graphql';
import * as DbMapper from '../database/mapper.database';

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

const blockFieldMap = {
  id: 'blocks.id',
  timestamp: 'blocks.mined_at',
  height: 'blocks.height',
  previous: 'blocks.previous_block',
  extended: 'blocks.extended',
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
        const block = await DbMapper.txIdToBlockMapper.get({
          tx_id: queryParams.id,
        });
        return R.reduce((acc: FieldMap, key: string) => {
          const txKeyKey = fieldMap[key].split('.');
          const txScope = txKeyKey[0];
          const txKey = txKeyKey[1];
          const val =
            txScope === 'blocks'
              ? (block[txKey] || '').toString()
              : (tx[txKey] || '').toString();
          return R.assoc(key, val, acc);
        }, {} as FieldMap)(R.keys(fieldMap));
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
      const pageSize = Math.min(
        queryParams.first || DEFAULT_PAGE_SIZE,
        MAX_PAGE_SIZE
      );

      const params: QueryParams = {
        limit: pageSize + 1,
        offset: offset,
        ids: queryParams.ids || undefined,
        to: queryParams.recipients || undefined,
        from: queryParams.owners || undefined,
        tags: queryParams.tags || undefined,
        blocks: true,
        since: timestamp,
        select: fieldMap,
        minHeight: queryParams.block?.min || undefined,
        maxHeight: queryParams.block?.max || undefined,
        sortOrder: queryParams.sort || undefined,
      };

      const results = (await generateQuery(params)) as TransactionHeader[];
      const hasNextPage = results.length > pageSize;

      return {
        pageInfo: {
          hasNextPage,
        },
        edges: async () => {
          return results.slice(0, pageSize).map((result: any, index) => {
            return {
              cursor: encodeCursor({ timestamp, offset: offset + index + 1 }),
              node: result,
            };
          });
        },
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
      { req, connection }: any
    ) => {
      const { timestamp, offset } = parseCursor(
        queryParams.after || newCursor()
      );
      const pageSize = Math.min(
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

      const query = generateBlockQuery({
        ids,
        select: blockFieldMap,
        minHeight,
        maxHeight,
        sortOrder: queryParams.sort || 'HEIGHT_ASC',
        limit: pageSize + 1,
        offset: offset,
        before: timestamp,
      });

      const results = await query;
      const hasNextPage = results.length > pageSize;

      return {
        pageInfo: {
          hasNextPage,
        },
        edges: async () => {
          return results
            .slice(0, pageSize)
            .map((result: any, index: number) => {
              return {
                cursor: encodeCursor({ timestamp, offset: offset + index + 1 }),
                node: result,
              };
            });
        },
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
      return moment(parent?.timestamp).unix();
    },
  },
};

export interface Cursor {
  timestamp: ISO8601DateTimeString;
  offset: number;
}

export function newCursor(): string {
  return encodeCursor({ timestamp: moment().toISOString(), offset: 0 });
}

export function encodeCursor({ timestamp, offset }: Cursor): string {
  const string = JSON.stringify([timestamp, offset]);
  return Buffer.from(string).toString('base64');
}

export function parseCursor(cursor: string): Cursor {
  try {
    const [timestamp, offset] = JSON.parse(
      Buffer.from(cursor, 'base64').toString()
    ) as [ISO8601DateTimeString, number];
    return { timestamp, offset };
  } catch (error) {
    throw new Error('invalid cursor');
  }
}
