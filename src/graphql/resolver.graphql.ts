/* eslint-disable unicorn/no-null */
import * as R from "rambda";
import moment from "moment";
import { types as CassandraTypes } from "cassandra-driver";
import { cassandraClient, toLong } from "../database/cassandra.database";
import { topHeight } from "../database/sync.database";
import graphqlFields from "graphql-fields";
import { config } from "dotenv";
import {
  QueryTransactionsArgs as QueryTransactionsArguments,
  QueryBlockArgs as QueryBlockArguments,
  QueryBlocksArgs as QueryBlocksArguments,
} from "./types";
import {
  ownerToAddress,
  winstonToAr,
  utf8DecodeTag,
} from "../utility/encoding.utility";
import {
  QueryParameters,
  generateBlockQuery,
  generateTransactionQuery,
  generateDeferedTxQuery,
  generateDeferedTxBlockQuery,
  // generateDeferedBlockQuery,
} from "./query.graphql";

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

const edgeFieldMapTx = {
  "edges.node.id": "tx_id",
  "edges.node.last_tx": "anchor",
  "edges.node.target": "recipient",
  "edges.node.tags": "tags",
  // 'edges.node.reward': 'fee',
  "edges.node.quantity": "quantity",
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

const resolveGqlTxSelect = (userFields: any, singleTx = false): string[] => {
  const select = [];
  for (const keyPath of R.keys(edgeFieldMapTx)) {
    if (
      R.hasPath(
        singleTx ? keyPath.replace("edges.node.", "") : keyPath,
        userFields
      )
    ) {
      select.push(edgeFieldMapTx[keyPath]);
    }
  }
  return select;
};

const resolveGqlBlockSelect = (userFields: any): string[] => {
  const select: string[] = [];

  for (const keyPath of R.keys(edgeFieldMapBlock)) {
    if (R.hasPath(keyPath as string, userFields)) {
      select.push(edgeFieldMapBlock[keyPath]);
    }
  }

  return select;
};

export const resolvers = {
  Query: {
    transaction: async (
      parent: FieldMap,
      queryParameters: any,
      { req, connection }: any, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: any
    ) => {
      const fieldsWithSubFields = graphqlFields(info);

      const parameters: any = {
        id: queryParameters.id || undefined,
        select: resolveGqlTxSelect(fieldsWithSubFields, true),
      };
      // No selection = no search
      if (R.isEmpty(parameters.select)) {
        return { data: { transaction: null } };
      }
      // todo, elide selectors not selected from user
      if (!parameters.select.includes("tx_id")) {
        parameters.select = R.append("tx_id", parameters.select);
      }

      parameters.select = R.append("tx_index", parameters.select);
      const txQuery = generateTransactionQuery(parameters);

      const { rows: resultArray } = await cassandraClient.execute(
        txQuery.query,
        txQuery.params,
        { prepare: true, executionProfile: "gql" }
      );

      if (R.isEmpty(resultArray)) {
        return null;
      }

      const result = resultArray[0];

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

        const { rows: blockResult } = await cassandraClient.execute(
          blockQuery.query,
          blockQuery.params,
          {
            prepare: true,
            executionProfile: "gql",
          }
        );
        result.block = R.isEmpty(blockResult) ? null : blockResult[0];
      }

      const selectedDeferedKeysUser = [];
      for (const k of R.keys(fieldsWithSubFields) as string[]) {
        ["anchor", "fee", "signature"].includes(k) &&
          selectedDeferedKeysUser.push(
            R.find(R.equals(k))(["anchor", "fee", "signature"])
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
          result.fee = deferedTxResult[0].reward || "";
        }
        if (deferedTxResult[0].signature) {
          result.signature = deferedTxResult[0].signature || "";
        }
      }

      return result as any;
    },
    transactions: async (
      parent: string,
      queryParameters: QueryTransactionsArguments,
      request: any, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: any
    ) => {
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
        to: queryParameters.recipients || undefined,
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

      let { rows: result } = await cassandraClient.execute(
        txQuery.query,
        txQuery.params,
        { prepare: true, executionProfile: "gql" }
      );

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
        for (const item of result) {
          // const userSelectKeys = R.keys(fieldsWithSubFields.edges.node.block);

          const blockQuery = generateDeferedTxBlockQuery(
            item.tx_index.divide(1000),
            selectParameters
          );

          const { rows: blockResult } = await cassandraClient.execute(
            blockQuery.query,
            blockQuery.params,
            {
              prepare: true,
              executionProfile: "gql",
            }
          );

          item.block = R.isEmpty(blockResult) ? null : blockResult[0];
        }
      }

      const selectedDeferedKeysUser = [];
      for (const k of R.keys(fieldsWithSubFields.edges.node) as string[]) {
        ["anchor", "fee", "signature"].includes(k) &&
          selectedDeferedKeysUser.push(
            R.find(R.equals(k))(["anchor", "fee", "signature"])
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

          const { rows: deferedTxResult } = await cassandraClient.execute(
            deferedTxQ.query,
            deferedTxQ.params,
            {
              prepare: true,
              executionProfile: "gql",
            }
          );

          if (deferedTxResult[0].last_tx) {
            tx.anchor = deferedTxResult[0].last_tx || "";
          }
          if (deferedTxResult[0].reward) {
            tx.fee = deferedTxResult[0].reward || "";
          }
          if (deferedTxResult[0].signature) {
            tx.signature = deferedTxResult[0].signature || "";
          }
        }
      }

      return {
        pageInfo: {
          hasNextPage,
        },
        edges: R.sort(
          parameters.select.sort === "HEIGHT_ASC"
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
      queryParameters: QueryBlockArguments,
      request: any // eslint-disable-line @typescript-eslint/no-unused-vars
    ) => {
      return queryParameters.id
        ? (
            await generateBlockQuery({
              select: blockFieldMap,
              id: queryParameters.id,
              offset: 0,
              fetchSize: 100,
            })
          ).first()
        : null;
    },
    blocks: async (
      parent: FieldMap,
      queryParameters: QueryBlocksArguments,
      request: any, // eslint-disable-line @typescript-eslint/no-unused-vars
      info: any
    ) => {
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

      let { rows: result } = await cassandraClient.execute(
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
      return parent.anchor || "";
    },
    signature: (parent: any) => {
      return parent.signature || "";
    },
    tags: (parent: FieldMap) => {
      return parent.tags.map(utf8DecodeTag);
    },
    recipient: (parent: FieldMap) => {
      return parent.recipient.trim() || "";
    },
    data: (parent: FieldMap) => {
      return {
        size: parent.data_size || 0,
        type: parent.data_type,
      };
    },
    quantity: (parent: FieldMap) => {
      return {
        ar: winstonToAr(parent.quantity || ("0" as const)),
        winston: parent.quantity || "0",
      };
    },
    fee: (parent: FieldMap) => {
      return {
        ar: winstonToAr(parent.fee || "0"),
        winston: parent.fee || "0",
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
