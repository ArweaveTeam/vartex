/* eslint-disable unicorn/no-null */
import * as R from "rambda";
import { Request } from "express";
import { types as CassandraTypes } from "cassandra-driver";
import {
  cassandraClient,
  blockMapper,
  transactionMapper,
} from "../database/mapper";
import { getMaxHeightBlock, toLong } from "../database/utils";
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
// import { findTxIDsFromTagFilters } from "./tag-search";
import { findTxIDsFromTxFilters } from "./tx-search";
import {
  ownerToAddress,
  winstonToAr,
  utf8DecodeTupleTag,
  fromB64Url,
} from "../utility/encoding";

process.env.NODE_ENV !== "test" && config();

let maxHeightBlock: [string, CassandraTypes.Long];

getMaxHeightBlock(cassandraClient).then((max) => {
  maxHeightBlock = max;
});
// update once per minute the maxHeight val
setInterval(async () => {
  try {
    maxHeightBlock = await getMaxHeightBlock(cassandraClient);
  } catch {}
}, 1000 * 60);

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
  height: CassandraTypes.Long;
  quantity: string;
  data_size: MetaData["size"];
  data_type: MetaData["type"];
  parent: Parent;
  owner: string;
  owner_address: string;
  reward: CassandraTypes.Long;
  signature: string;
  timestamp: CassandraTypes.Long;
  previous_block: string;
  block: unknown;
  block_id: string;
  block_timestamp: string;
  block_height: string;
  block_previous: string;
}

interface BlockCursor {
  block_hash: string;
  block_height: string;
  cursorType?: string;
  nthMillion: number;
}

function encodeBlockCursor({
  block_hash,
  block_height,
  nthMillion,
}: BlockCursor): string {
  const string = JSON.stringify([
    "block_search",
    block_hash,
    block_height,
    nthMillion,
  ]);
  return Buffer.from(string).toString("base64url");
}

function parseBlockCursor(cursor: string): BlockCursor {
  try {
    const [cursorType, block_hash, block_height, nthMillion] = JSON.parse(
      Buffer.from(cursor, "base64url").toString()
    ) as [string, string, string, number];
    return { cursorType, block_hash, block_height, nthMillion };
  } catch {
    throw new Error("invalid cursor");
  }
}

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
      if (!maxHeightBlock) {
        throw new Error(`graphql isn't ready!`);
      }
      const fieldsWithSubFields = graphqlFields(info);
      const wantsBlock = R.hasPath("edges.node.block", fieldsWithSubFields);

      // const tagSearchMode =
      //   queryParameters.tags && !R.isEmpty(queryParameters.tags);

      const [txSearchResult, hasNextPage] = await findTxIDsFromTxFilters(
        maxHeightBlock[1],
        queryParameters
      );
      // const [txSearchResult, hasNextPage] = tagSearchMode
      //   ? await findTxIDsFromTagFilters(maxHeightBlock[1], queryParameters)
      //   : await findTxIDsFromTxFilters(maxHeightBlock[1], queryParameters);

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
        txsClean.map(async ({ tx, cursor }) => ({
          cursor,
          node: R.assoc(
            "block",
            wantsBlock && tx.block_hash
              ? (
                  await cassandraClient.execute(
                    `SELECT indep_hash,timestamp,height,previous_block FROM ${KEYSPACE}.block WHERE indep_hash='${tx.block_hash}'`
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

    block: async (
      parent: FieldMap,
      queryParameters: QueryBlockArguments,
      request: Request, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: GraphQLResolveInfo // eslint-disable-line @typescript-eslint/no-unused-vars
    ): Promise<Maybe<Block>> => {
      return await blockMapper.get({ indep_hash: queryParameters.id });
    },
    blocks: async (
      parent: FieldMap,
      queryParameters: QueryBlocksArguments,
      request: Request, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: GraphQLResolveInfo
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ): Promise<any> => {
      if (!maxHeightBlock) {
        throw new Error(`graphql isn't ready!`);
      }

      const fieldsWithSubFields = graphqlFields(info);

      const wantsCursor = R.hasPath("edges.cursor", fieldsWithSubFields);

      // should never "or" to the fallback
      const maxHeight = maxHeightBlock[1] || toLong(10e6);

      const sortOrder =
        queryParameters.sort === "HEIGHT_ASC" ? "HEIGHT_ASC" : "HEIGHT_DESC";

      const cursorQuery =
        queryParameters.after &&
        typeof queryParameters.after === "string" &&
        !R.isEmpty(queryParameters.after);

      const maybeCursor = cursorQuery
        ? parseBlockCursor(queryParameters.after)
        : // eslint-disable-next-line @typescript-eslint/no-explicit-any
          ({} as any);

      const tableName =
        sortOrder === "HEIGHT_ASC"
          ? "block_height_sorted_asc"
          : "block_height_sorted_desc";

      const limit = Math.min(100, queryParameters.first || 10);

      const blockMinHeight_ =
        typeof queryParameters.height === "object" &&
        typeof queryParameters.height.min === "number"
          ? `${queryParameters.height.min}`
          : "0";

      const blockMinHeight =
        typeof maybeCursor.block_height !== "undefined" &&
        sortOrder === "HEIGHT_ASC" &&
        toLong(maybeCursor.block_height).gt(toLong(blockMinHeight_))
          ? maybeCursor.block_height
          : blockMinHeight_;

      const blockMaxHeight_ =
        typeof queryParameters.height === "object" &&
        typeof queryParameters.height.max === "number"
          ? `${queryParameters.height.max}`
          : maxHeight.toString();

      const blockMaxHeight =
        typeof maybeCursor.block_height !== "undefined" &&
        sortOrder === "HEIGHT_DESC" &&
        toLong(maybeCursor.block_height).lt(toLong(blockMaxHeight_))
          ? maybeCursor.block_height
          : blockMaxHeight_;

      const xMillions = toLong(blockMaxHeight).div(1e6);

      const rangePostFunction =
        sortOrder === "HEIGHT_ASC" ? R.identity : R.reverse;

      const bucketStart =
        typeof maybeCursor.nthMillion !== "undefined" &&
        sortOrder === "HEIGHT_ASC"
          ? maybeCursor.nthMillion
          : 0;

      const bucketEnd =
        typeof maybeCursor.nthMillion !== "undefined" &&
        sortOrder === "HEIGHT_DESC"
          ? maybeCursor.nthMillion + 1
          : (xMillions.add(1).toInt() as number);

      const buckets: number[] = rangePostFunction(
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (R.range as any)(bucketStart, bucketEnd)
      );

      let hasNextPage = false;
      let resultCount = 0;
      let nthBucket = 0;

      const searchResult: {
        nthMillion: number;
        block_hash: string;
        block_height: string;
      }[] = [];

      while (nthBucket < buckets.length && resultCount < limit) {
        const nextResult = await cassandraClient.execute(
          `SELECT * FROM ${KEYSPACE}.${tableName} WHERE nth_million=${
            buckets[nthBucket]
          } AND block_height >= ${blockMinHeight} AND block_height <= ${blockMaxHeight} LIMIT ${
            limit - resultCount + 1
          }`
        );
        for (const row of nextResult.rows) {
          searchResult.push(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (R.assoc as any)("nthMillion", buckets[nthBucket], row)
          );
          if (resultCount !== limit) {
            resultCount += 1;
          } else {
            hasNextPage = true;
          }
        }

        nthBucket += 1;
      }

      const blocks = await Promise.all(
        searchResult
          .slice(0, limit)
          .map(
            async ({ block_hash }) =>
              await blockMapper.get({ indep_hash: block_hash })
          )
      );

      const cursors = wantsCursor
        ? // eslint-disable-next-line @typescript-eslint/no-explicit-any
          searchResult.slice(1, limit + 1).map((block: any) =>
            encodeBlockCursor({
              block_hash: block.block_hash,
              block_height: block.block_height.toString(),
              nthMillion: block.nthMillion,
            })
          )
        : [];

      return {
        pageInfo: {
          hasNextPage,
        },
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        edges: blocks.map((block: any, index: number) => ({
          node: block,
          cursor:
            wantsCursor && index < cursors.length ? cursors[index] : undefined,
        })),
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
      const maybeFee = parent.reward ? parent.reward.toString() : "0";
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
      return parent.previous_block || "";
    },
    timestamp: (parent: FieldMap): string => {
      return parent.timestamp.toString();
    },
  },
};
