/* eslint-disable unicorn/no-null */
import * as R from "rambda";
import { Request } from "express";
import moment from "moment";
import { types as CassandraTypes } from "cassandra-driver";
import { cassandraClient, toLong } from "../database/cassandra";
import { transactionMapper } from "../database/mapper";
import { gatewayHeight } from "../database/sync";
import { GraphQLResolveInfo } from "graphql";
import graphqlFields from "graphql-fields";
import { config } from "dotenv";
import {
  Amount,
  Block,
  Maybe,
  MetaData,
  Owner,
  Parent,
  Query,
  QueryBlockArgs as QueryBlockArguments,
  QueryBlocksArgs as QueryBlocksArguments,
  QueryTransactionArgs as QueryTransactionArguments,
  QueryTransactionsArgs as QueryTransactionsArguments,
  Transaction,
} from "./types.graphql";
import { Tag } from "../types/arweave";
import { KEYSPACE } from "../constants";
import { findTxIDsFromTagFilters } from "./tag-search";
import { findTxIDsFromTxFilters } from "./tx-search";
import {
  ownerToAddress,
  winstonToAr,
  utf8DecodeTupleTag,
  fromB64Url,
} from "../utility/encoding";
import {
  QueryParameters,
  generateBlockQuery,
  generateTransactionQuery,
  generateDeferedTxQuery,
  generateDeferedTxBlockQuery,
  // generateDeferedBlockQuery,
} from "./query-builder";

process.env.NODE_ENV !== "test" && config();

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

const DEFAULT_PAGE_SIZE = Number.parseInt(
  process.env.DEFAULT_PAGE_SIZE || "10"
);
const MAX_PAGE_SIZE = Number.parseInt(process.env.MAX_PAGE_SIZE || "100");

interface ArFee {
  winston: CassandraTypes.Long;
  ar: string;
}

interface FieldData {
  size: MetaData["size"];
  type: MetaData["type"];
}

interface FieldMap {
  indep_hash: string;
  id: string;
  ids: string[] | string; // legacy support, should only be string[]
  tx_id: string;
  data_root: string;
  anchor: string;
  recipient: string;
  target: string;
  tags: CassandraTypes.Tuple[];
  fee: ArFee;
  height: CassandraTypes.Long;
  quantity: string;
  data: FieldData;
  data_size: MetaData["size"];
  data_type: MetaData["type"];
  parent: Parent;
  owner: string;
  owner_address: string;
  signature: string;
  timestamp: CassandraTypes.Long;
  previous_block: string;
  block: unknown;
  block_id: string;
  block_timestamp: string;
  block_height: string;
  block_previous: string;
}

const edgeFieldMapTx = {
  "edges.node.id": "tx_id",
  "edges.node.last_tx": "anchor",
  "edges.node.recipient": "target",
  "edges.node.tags": "tags",
  // 'edges.node.reward': 'fee',
  // "edges.node.quantity": "quantity",
  "edges.node.data_size": "data_size",
  "edges.node.content_type": "data_type",
  "edges.node.parent": "parent",
  "edges.node.owner": "owner",
  "edges.node.data_root": "data_root",
  // 'edges.node.owner_address': 'owner_address',
};

const edgeFieldMapBlock = {
  "edges.node.id": "indep_hash",
  "edges.node.timestamp": "timestamp",
  "edges.node.height": "height",
  "edges.node.previous": "previous_block",
};

const blockFieldMap = {
  id: "blocks.indep_hash",
  timestamp: "blocks.mined_at",
  height: "blocks.height",
  // extended: 'blocks.extended',
};

const resolveGqlSingleTxSelect = (userFields: unknown): string[] => {
  const select = [];
  for (const keyPath of R.keys(edgeFieldMapTx)) {
    if (
      R.hasPath(
        keyPath.replace("edges.node.", ""),
        userFields as Record<string, unknown>
      )
    ) {
      select.push(edgeFieldMapTx[keyPath]);
    }
  }
  return select;
};

const resolveGqlTxSelect = (userFields: unknown): string[] => {
  const select = [];
  for (const keyPath of R.keys(edgeFieldMapTx)) {
    if (R.hasPath(keyPath, userFields as Record<string, unknown>)) {
      select.push(edgeFieldMapTx[keyPath]);
    }
  }
  return select;
};

const resolveGqlBlockSelect = (userFields: unknown): string[] => {
  const select: string[] = [];

  for (const keyPath of R.keys(edgeFieldMapBlock)) {
    if (R.hasPath(keyPath as string, userFields as Record<string, unknown>)) {
      select.push(edgeFieldMapBlock[keyPath]);
    }
  }

  return select;
};

export const resolvers = {
  Query: {
    transaction: async (
      parent: FieldMap,
      queryParameters: QueryTransactionArguments,
      request: Request, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: GraphQLResolveInfo
    ): Promise<Maybe<Query["transaction"]>> => {
      const fieldsWithSubFields = graphqlFields(info);

      const queryId = queryParameters.id;

      const maybeTx =
        queryId && !R.isEmpty(queryId)
          ? await transactionMapper.get({ tx_id: queryId })
          : undefined;

      const maybeBlock =
        fieldsWithSubFields.block &&
        maybeTx &&
        typeof maybeTx.block_hash === "string"
          ? (
              await cassandraClient.execute(
                `SELECT timestamp,height,previous_block FROM ${KEYSPACE}.block WHERE indep_hash='${maybeTx.block_hash}'`
              )
            ).rows[0] || {}
          : {};

      return R.assoc("block", maybeBlock, maybeTx) as Transaction;
    },
    transactions: async (
      parent: string,
      queryParameters: QueryTransactionsArguments,
      request: Request, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: GraphQLResolveInfo
    ): Promise<Maybe<Query["transactions"]>> => {
      const fieldsWithSubFields = graphqlFields(info);
      const wantsBlock = R.hasPath("edges.node.block", fieldsWithSubFields);

      const tagSearchMode =
        queryParameters.tags && !R.isEmpty(queryParameters.tags);

      const [txSearchResult, hasNextPage] = tagSearchMode
        ? await findTxIDsFromTagFilters(queryParameters)
        : await findTxIDsFromTxFilters(queryParameters);

      if (R.isEmpty(txSearchResult)) {
        return {
          pageInfo: {
            hasNextPage: false,
          },
          edges: [],
        };
      }

      const txs = await Promise.all(
        txSearchResult.map(
          async ({ txId, cursor }: { txId: string; cursor: string }) => ({
            tx: await transactionMapper.get({ tx_id: txId }),
            cursor,
          })
        )
      );

      const txsClean = R.reject(R.isNil)(txs);
      const edges = await Promise.all(
        txsClean.map(async ({ block, tx, cursor }) => ({
          cursor,
          node: R.assoc(
            "block",
            wantsBlock && tx.block_hash
              ? (
                  await cassandraClient.execute(
                    `SELECT timestamp,height,previous_block FROM ${KEYSPACE}.block WHERE indep_hash='${tx.block_hash}'`
                  )
                ).rows[0] || {}
              : {},
            tx
          ),
        }))
      );

      return {
        pageInfo: {
          hasNextPage,
        },
        edges,
      };
    },

    blocks: async (
      parent: FieldMap,
      queryParameters: QueryBlocksArguments,
      request: Request, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: GraphQLResolveInfo
    ): Promise<Maybe<Query["blocks"]>> => {
      return {
        pageInfo: {
          hasNextPage: false,
        },
        edges: [],
      };
    },
  },
  Transaction: {
    id: (parent: FieldMap): string => {
      return parent.tx_id;
    },
    dataRoot: (parent: FieldMap): string => {
      return parent.data_root;
    },
    anchor: (parent: FieldMap): string => {
      return parent.anchor || "";
    },
    signature: (parent: FieldMap): string => {
      return parent.signature || "";
    },
    tags: (parent: FieldMap): Tag[] => {
      return (parent.tags || []).map(utf8DecodeTupleTag);
    },
    recipient: (parent: FieldMap): string => {
      return parent.target || "";
    },
    data: (parent: FieldMap): MetaData => {
      // Q29udGVudC1UeXBl = "Content-Type"
      // Y29udGVudC10eXBl = "content-type"
      const maybeContentType =
        Array.isArray(parent.tags) &&
        parent.tags.find((tag) =>
          ["Q29udGVudC1UeXBl", "Y29udGVudC10eXBl"].includes(tag.get(0))
        );

      return {
        // eslint-ignore-next-line unicorn/explicit-length-check
        size: `${parent.data_size || 0}`,
        type:
          parent.data_type ||
          (maybeContentType
            ? fromB64Url(maybeContentType.get(1)).toString("utf8")
            : ""),
      };
    },
    quantity: (parent: FieldMap): Amount => {
      return {
        ar: winstonToAr((parent.quantity && parent.quantity.toString()) || "0"),
        winston: parent.quantity || "0",
      };
    },
    fee: (parent: FieldMap): Amount => {
      const maybeFee =
        parent.fee && parent.fee.winston && parent.fee.winston.toString();
      return {
        ar: winstonToAr(maybeFee || "0"),
        winston: maybeFee || "0",
      };
    },
    block: (parent: FieldMap): Block => {
      return parent.block as Block;
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
    owner: (parent: FieldMap): Owner => {
      return {
        address: ownerToAddress(parent.owner),
        key: parent.owner,
      };
    },
    parent: (parent: FieldMap): Maybe<Parent> => {
      if (parent.parent) {
        return parent.parent;
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
    height: (parent: FieldMap): number => {
      return parent.height.toInt();
    },
    id: (parent: FieldMap): string => {
      return parent.indep_hash;
    },
    previous: (parent: FieldMap): string => {
      return parent.previous_block;
    },
    timestamp: (parent: FieldMap): string => {
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
  return Buffer.from(string).toString("base64");
}

export function parseCursor(cursor: string): Cursor {
  try {
    const [timestamp, offset] = JSON.parse(
      Buffer.from(cursor, "base64").toString()
    ) as [string, number];
    return { timestamp, offset };
  } catch {
    throw new Error("invalid cursor");
  }
}
