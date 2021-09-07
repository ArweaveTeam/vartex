/* eslint-disable unicorn/no-null */
import * as R from "rambda";
import { Request } from "express";
import moment from "moment";
import { types as CassandraTypes } from "cassandra-driver";
import { cassandraClient, toLong } from "../database/cassandra";
import { topHeight } from "../database/sync";
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
} from "./types";
import { Tag } from "../types/arweave";
import {
  ownerToAddress,
  winstonToAr,
  utf8DecodeTupleTag,
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

interface FieldMap {
  indep_hash: string;
  id: string;
  tx_id: string;
  anchor: string;
  recipient: string;
  target: string;
  tags: CassandraTypes.Tuple[];
  fee: string;
  height: CassandraTypes.Long;
  quantity: string;
  data_size: MetaData["size"];
  data_type: MetaData["type"];
  parent: Parent;
  owner: string;
  owner_address: string;
  signature: string;
  timestamp: CassandraTypes.Long;
  previous: string;
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

const resolveGqlTxSelect = (
  userFields: unknown,
  singleTx = false
): string[] => {
  const select = [];
  for (const keyPath of R.keys(edgeFieldMapTx)) {
    if (
      R.hasPath(
        singleTx ? keyPath.replace("edges.node.", "") : keyPath,
        userFields as Record<string, unknown>
      )
    ) {
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

      const parameters: { id: string | undefined; select: string[] } = {
        id: queryParameters.id || undefined,
        select: resolveGqlTxSelect(fieldsWithSubFields, true),
      };
      // No selection = no search
      if (R.isEmpty(parameters.select)) {
        return null;
      }
      // todo, elide selectors not selected from user
      if (!parameters.select.includes("tx_id")) {
        parameters.select = R.append("tx_id", parameters.select);
      }

      parameters.select = R.append("tx_index", parameters.select);
      const txQuery = generateTransactionQuery(parameters);

      const {
        rows: resultArray,
      }: { rows: unknown[] } = await cassandraClient.execute(
        txQuery.query,
        txQuery.params,
        { prepare: true, executionProfile: "gql" }
      );

      if (R.isEmpty(resultArray)) {
        return null;
      }

      const result = resultArray[0] as Partial<
        Transaction & {
          tx_id: string;
          tx_index?: CassandraTypes.Long;
          block: Block | null;
        }
      >;

      if (fieldsWithSubFields.block !== undefined) {
        let selectParameters = [];
        const userSelectKeys = R.keys(fieldsWithSubFields.block);
        for (const selectKey of ["id", "timestamp", "height", "previous"]) {
          if (userSelectKeys.includes(selectKey)) {
            switch (selectKey) {
              case "id": {
                selectParameters = R.append("indep_hash", selectParameters);
                break;
              }
              case "previous": {
                selectParameters = R.append("previous_block", selectParameters);
                break;
              }
              default: {
                selectParameters = R.append(selectKey, selectParameters);
              }
            }
          }
        }

        const blockQuery = generateDeferedTxBlockQuery(
          result.tx_index.divide(1000),
          selectParameters
        );

        const {
          rows: blockResult,
        }: { rows: unknown[] } = await cassandraClient.execute(
          blockQuery.query,
          blockQuery.params,
          {
            prepare: true,
            executionProfile: "gql",
          }
        );
        result.block = R.isEmpty(blockResult)
          ? null
          : (blockResult[0] as Block);
      }

      const selectedDeferedKeysUser = [];
      for (const k of R.keys(fieldsWithSubFields) as string[]) {
        ["anchor", "fee", "signature", "quantity"].includes(k) &&
          selectedDeferedKeysUser.push(
            R.find(R.equals(k))(["anchor", "fee", "signature", "quantity"])
          );
      }

      if (!R.isEmpty(selectedDeferedKeysUser)) {
        const selectedDeferedKeysDatabase = [];
        for (const k of selectedDeferedKeysUser) {
          switch (k) {
            case "anchor": {
              selectedDeferedKeysDatabase.push("last_tx");
              break;
            }
            case "fee": {
              selectedDeferedKeysDatabase.push("reward");
              break;
            }
            case "quantity": {
              selectedDeferedKeysDatabase.push("quantity");
              break;
            }

            default: {
              selectedDeferedKeysDatabase.push(k);
            }
          }
        }

        const deferedTxQ = generateDeferedTxQuery({
          deferedSelect: selectedDeferedKeysDatabase,
          tx_id: result.tx_id,
        });

        const { rows: deferedTxResult } = await cassandraClient.execute(
          deferedTxQ.query,
          deferedTxQ.params,
          {
            prepare: true,
            executionProfile: "gql",
          }
        );
        if (deferedTxResult[0].last_tx) {
          result.anchor = deferedTxResult[0].last_tx || "";
        }
        if (deferedTxResult[0].reward) {
          result.fee = {
            winston: deferedTxResult[0].reward || "",
            ar: winstonToAr(parent.quantity || "0"),
          };
        }
        if (deferedTxResult[0].signature) {
          result.signature = deferedTxResult[0].signature || "";
        }
      }

      return result as Transaction;
    },
    transactions: async (
      parent: string,
      queryParameters: QueryTransactionsArguments,
      request: Request, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: GraphQLResolveInfo
    ): Promise<Maybe<Query["transactions"]>> => {
      const { timestamp, offset } = parseCursor(
        queryParameters.after || newCursor()
      );
      const fieldsWithSubFields = graphqlFields(info);

      const fetchSize =
        Math.min(queryParameters.first || DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE) + 1;

      let minHeight = toLong(0);
      let maxHeight = toLong(750_000); // FIXME poll for topTxIndex

      if (queryParameters.block && queryParameters.block.min) {
        minHeight = toLong(queryParameters.block.min).mul(1000);
      }

      if (queryParameters.block && queryParameters.block.max) {
        maxHeight = toLong(queryParameters.block.max).mul(1000);
      }

      const selectsBlock = R.hasPath("edges.node.block", fieldsWithSubFields);
      const parameters: Partial<
        Omit<QueryParameters, "after"> & { before: string }
      > = {
        limit: fetchSize,
        offset: offset,
        // ids: queryParameters.ids || undefined,
        recipients: queryParameters.recipients || undefined,
        from: queryParameters.owners || undefined,
        tags: queryParameters.tags || undefined,
        blocks: true,
        before: timestamp,
        select: resolveGqlTxSelect(fieldsWithSubFields, false),
        minHeight,
        maxHeight,
        sortOrder: queryParameters.sort || undefined,
      };

      // No selection = no search
      if (R.isEmpty(parameters.select) && !selectsBlock) {
        return {
          pageInfo: {
            hasNextPage: false,
          },
          edges: [],
        };
      }

      // todo, elide selectors not selected from user
      if (!parameters.select.includes("tx_id")) {
        parameters.select = R.append("tx_id", parameters.select);
      }

      parameters.select = R.append("tx_index", parameters.select);

      const txQuery = generateTransactionQuery(parameters);

      const {
        rows: result_,
      }: { rows: unknown[] } = await cassandraClient.execute(
        txQuery.query,
        txQuery.params,
        { prepare: true, executionProfile: "gql" }
      );

      let result = result_ as Partial<
        Transaction & { tx_index: CassandraTypes.Long; tx_id?: string }
      >[];
      let hasNextPage = false;

      if (result.length === fetchSize) {
        hasNextPage = true;
        result = R.dropLast(1, result);
      }

      if (selectsBlock) {
        let selectParameters = [];
        // let resultWithBlock = [];
        const userSelectKeys = Object.keys(
          R.path("edges.node.block", fieldsWithSubFields)
        );
        for (const selectKey of ["id", "timestamp", "height", "previous"]) {
          if (userSelectKeys.includes(selectKey)) {
            switch (selectKey) {
              case "id": {
                selectParameters = R.append("indep_hash", selectParameters);
                break;
              }
              case "previous": {
                selectParameters = R.append("previous_block", selectParameters);
                break;
              }
              default: {
                selectParameters = R.append(selectKey, selectParameters);
              }
            }
          }
        }
        for (const item of result as Partial<{
          tx_index?: CassandraTypes.Long;
          block: Block | undefined;
        }>[]) {
          // const userSelectKeys = R.keys(fieldsWithSubFields.edges.node.block);

          const blockQuery = generateDeferedTxBlockQuery(
            item.tx_index.divide(1000),
            selectParameters
          );

          const {
            rows: blockResult,
          }: { rows: unknown[] } = await cassandraClient.execute(
            blockQuery.query,
            blockQuery.params,
            {
              prepare: true,
              executionProfile: "gql",
            }
          );

          item.block = R.isEmpty(blockResult)
            ? null
            : (blockResult[0] as Block);
        }
      }

      const selectedDeferedKeysUser = [];
      for (const k of R.keys(fieldsWithSubFields.edges.node) as string[]) {
        ["anchor", "fee", "signature", "quantity"].includes(k) &&
          selectedDeferedKeysUser.push(
            R.find(R.equals(k))(["anchor", "fee", "signature", "quantity"])
          );
      }
      if (!R.isEmpty(selectedDeferedKeysUser)) {
        const selectedDeferedKeysDatabase = [];
        for (const k of selectedDeferedKeysUser) {
          switch (k) {
            case "anchor": {
              selectedDeferedKeysDatabase.push("last_tx");
              break;
            }
            case "fee": {
              selectedDeferedKeysDatabase.push("reward");
              break;
            }
            case "quantity": {
              selectedDeferedKeysDatabase.push("quantity");
              break;
            }
            default: {
              selectedDeferedKeysDatabase.push(k);
            }
          }
        }

        for (const tx of result) {
          const deferedTxQ = generateDeferedTxQuery({
            deferedSelect: selectedDeferedKeysDatabase,
            tx_id: tx.tx_id,
          });

          const {
            rows: deferedTxResult_,
          }: { rows: unknown[] } = await cassandraClient.execute(
            deferedTxQ.query,
            deferedTxQ.params,
            {
              prepare: true,
              executionProfile: "gql",
            }
          );

          const deferedTxResult = deferedTxResult_[0] as {
            last_tx?: string;
            reward?: string;
            signature?: string;
          };

          if (deferedTxResult?.last_tx) {
            tx.anchor = deferedTxResult.last_tx || "";
          }
          if (deferedTxResult?.reward) {
            tx.fee = {
              winston: deferedTxResult.reward || "",
              ar: winstonToAr(deferedTxResult.reward || "0"),
            };
          }
          if (deferedTxResult?.signature) {
            tx.signature = deferedTxResult.signature || "";
          }
        }
      }

      return {
        pageInfo: {
          hasNextPage,
        },
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        edges: (R.sort as any)(
          parameters.select.sort === "HEIGHT_ASC"
            ? sortByTxIndexAsc
            : sortByTxIndexDesc,
          result
        ).map((tx, index) => ({
          cursor: encodeCursor({ timestamp, offset: offset + index + 1 }),
          node: tx as Transaction,
        })),
      };
    },
    block: async (
      parent: string,
      queryParameters: QueryBlockArguments,
      request: Request // eslint-disable-line @typescript-eslint/no-unused-vars
    ): Promise<Maybe<Query["block"]>> => {
      return queryParameters.id
        ? (
            await generateBlockQuery({
              select: blockFieldMap,
              id: queryParameters.id,
              offset: 0,
              fetchSize: 1,
            })
          )[0]
        : null;
    },
    blocks: async (
      parent: FieldMap,
      queryParameters: QueryBlocksArguments,
      request: Request, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: GraphQLResolveInfo
    ): Promise<Maybe<Query["blocks"]>> => {
      const fieldsWithSubFields = graphqlFields(info);

      const { timestamp, offset } = parseCursor(
        queryParameters.after || newCursor()
      );
      const fetchSize =
        Math.min(queryParameters.first || DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE) + 1;

      let ids: Array<string> = [];
      let minHeight = toLong(0);
      let maxHeight = toLong(topHeight);

      if (queryParameters.ids) {
        ids = queryParameters.ids;
      }

      if (queryParameters.height && queryParameters.height.min) {
        minHeight = toLong(queryParameters.height.min);
      }

      if (queryParameters.height && queryParameters.height.max) {
        maxHeight = toLong(queryParameters.height.max);
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
        sortOrder: queryParameters.sort || undefined,
      });

      let hasNextPage = false;

      let {
        rows: result,
      }: { rows: unknown[] } = await cassandraClient.execute(
        blockQuery.query,
        blockQuery.params,
        { prepare: true, executionProfile: "gql" }
      );

      if (result.length === fetchSize) {
        hasNextPage = true;
        result = R.dropLast(1, result);
      }

      return {
        pageInfo: {
          hasNextPage,
        },
        edges: (result as Block[]).map((block, index) => ({
          cursor: encodeCursor({ timestamp, offset: offset + index + 1 }),
          node: block,
        })),
      };
    },
  },
  Transaction: {
    id: (parent: FieldMap): string => {
      return parent.tx_id;
    },
    anchor: (parent: FieldMap): string => {
      return parent.anchor || "";
    },
    signature: (parent: FieldMap): string => {
      return parent.signature || "";
    },
    tags: (parent: FieldMap): Tag[] => {
      return parent.tags.map(utf8DecodeTupleTag);
    },
    recipient: (parent: FieldMap): string => {
      return parent.target || "";
    },
    data: (parent: FieldMap): MetaData => {
      return {
        size: `${parent.data_size || 0}`,
        type: parent.data_type,
      };
    },
    quantity: (parent: FieldMap): Amount => {
      return {
        ar: winstonToAr(parent.quantity || "0"),
        winston: parent.quantity || "0",
      };
    },
    fee: (parent: FieldMap): Amount => {
      return {
        ar: winstonToAr(parent.fee || "0"),
        winston: parent.fee || "0",
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
      return parent.previous;
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
