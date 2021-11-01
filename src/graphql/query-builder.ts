import * as R from "rambda";
import { types as CassandraTypes } from "cassandra-driver";
import { config } from "dotenv";
import { KEYSPACE, tableId } from "../constants";
import { TagFilter } from "./types.graphql";
import { toB64url } from "../query/transaction";
import { default as cqlBuilder } from "@ridi/cql-builder";

const { Select } = cqlBuilder;

export type TxSortOrder = "HEIGHT_ASC" | "HEIGHT_DESC";

process.env.NODE_ENV !== "test" && config();

export interface QueryParameters {
  recipients?: string[];
  owners?: string[];
  id?: string;
  ids?: string[];
  limit?: number;
  offset?: number;
  select?: any; // eslint-disable-line @typescript-eslint/no-explicit-any
  blocks?: boolean;
  since?: string;
  before?: string;
  sortOrder?: TxSortOrder;
  status?: "any" | "confirmed" | "pending";
  tags?: TagFilter[];
  pendingMinutes?: number;
  minHeight?: CassandraTypes.Long;
  maxHeight?: CassandraTypes.Long;
  tagSearchMode?: boolean;
}

interface CqlQuery {
  query: string;
  params: unknown[];
}

export function generateTransactionQuery(
  parameters: QueryParameters
): CqlQuery {
  const cql = Select();

  if (
    parameters.ids &&
    Array.isArray(parameters.ids) &&
    parameters.ids.length > 0
  ) {
    if (parameters.ids.length === 1) {
      return cql
        .table("transaction", KEYSPACE)
        .field(parameters.select)
        .where("tx_id = ?", parameters.ids[0])
        .build();
    } else {
      // cql.where.apply(
      //   cql,
      //   [...R,
      //       `tx_id IN ( ${R.range(0, parameters.ids.length)
      //         .map(() => "?")
      //         .join(", ")} )`,
      //     ].concat(
      //     parameters.ids
      //   )
      // );
    }
  }
  const table =
    parameters.sortOrder === "HEIGHT_ASC"
      ? tableId.TABLE_GQL_TX_ASC
      : tableId.TABLE_GQL_TX_DESC;

  cql
    .table(table, KEYSPACE)
    .field(parameters.select)
    .where("partition_id = %1")
    .where("bucket_id = %2")
    .where("bucket_number = %3");

  if (parameters.id) {
    cql.where(`tx_id = ?`, parameters.id);
    cql.build();
  }

  // else if (parameters.ids && Array.isArray(parameters.ids)) {
  //   cql.limit(parameters.limit);
  //   cql.where.apply(
  //     cql,
  //     R.concat(
  //       [
  //         `tx_id IN ( ${R.range(0, parameters.ids.length)
  //           .map(() => "?")
  //           .join(", ")} )`,
  //       ],
  //       parameters.ids
  //     )
  //   );
  // }

  if (Array.isArray(parameters.tags) && !R.isEmpty(parameters.tags)) {
    for (const { name, values = "" } of parameters.tags) {
      cql.where("tag_name = ?", toB64url(name || ""));
      for (const value of values) {
        cql.where("tag_value = ?", toB64url(value || ""));
      }
    }
  }

  if (parameters.since) {
    cql.where(
      "block_timestamp < ?",
      CassandraTypes.Long.fromNumber(
        Math.floor(
          CassandraTypes.TimeUuid.fromString(parameters.since)
            .getDate()
            .valueOf() / 1000
        )
      )
    );
  }

  // if (params.status === 'confirmed') {
  //   cql.where('block_height >= ?', CassandraTypes.Long.fromNumber(0));
  // }

  if (parameters.owners && !R.isEmpty(parameters.owners)) {
    if (parameters.owners.length === 1) {
      cql.where(`owner = ?`, parameters.owners[0]);
    } else {
      cql.where.apply(
        cql,
        `owner IN ( ${parameters.owners.map(() => "?").join(", ")} )`,
        parameters.owners
      );
    }
  }

  if (parameters.recipients && !R.isEmpty(parameters.recipients)) {
    if (parameters.recipients.length === 1) {
      cql.where(`target = ?`, parameters.recipients[0]);
    } else {
      cql.where.apply(
        cql,
        `target IN ( ${parameters.recipients.map(() => "?").join(", ")} )`,
        parameters.recipients
      );
    }
  }

  // if (params.before) {
  //   cql.where('timestamp < ?', params.before);
  // }

  if (
    parameters.minHeight &&
    parameters.maxHeight &&
    parameters.maxHeight.eq(parameters.minHeight)
  ) {
    cql.where("tx_index >= ?", parameters.minHeight.toString());
    cql.where("tx_index < ?", parameters.minHeight.add(1000).toString());
  } else {
    if (parameters.minHeight) {
      const txsMinHeight =
        parameters.sortOrder === "HEIGHT_ASC"
          ? parameters.minHeight.add(parameters.offset).toString()
          : parameters.minHeight.toString();

      cql.where("tx_index >= ?", txsMinHeight);
    }

    if (parameters.maxHeight) {
      const txsMaxHeight =
        parameters.sortOrder === "HEIGHT_DESC"
          ? parameters.maxHeight.sub(parameters.offset).toString()
          : parameters.maxHeight.toString();
      cql.where("tx_index <= ?", txsMaxHeight);
    }
  }

  return cql.limit(parameters.limit).build();
}

export interface BlockQueryParameters {
  id?: string;
  ids?: string[];
  select?: any; // eslint-disable-line @typescript-eslint/no-explicit-any
  before?: string;
  offset: number;
  fetchSize: number;
  minHeight?: CassandraTypes.Long;
  maxHeight?: CassandraTypes.Long;
  sortOrder?: TxSortOrder;
}

export function generateBlockQuery(parameters: BlockQueryParameters): CqlQuery {
  const {
    id,
    ids,
    select,
    before,
    offset = 0,
    fetchSize,
    minHeight,
    maxHeight,
    sortOrder,
  } = parameters;

  const cql = Select()
    // .table(
    //   sortOrder === "HEIGHT_ASC"
    //     ? tableId.TABLE_GQL_BLOCK_ASC
    //     : tableId.TABLE_GQL_BLOCK_DESC,
    //   KEYSPACE
    // )
    .field(
      select.includes("indep_hash") ? select : R.append("indep_hash", select)
    )
    .where("partition_id = %1")
    .where("bucket_id = %2")
    .where("bucket_number = %3")
    .filtering();

  // const query = connection.queryBuilder().select(select).from('blocks');
  if (id) {
    cql.where("indep_hash = ?", id);
  } else if (ids && Array.isArray(ids) && !R.isEmpty(ids)) {
    // cql.where.apply(
    //   cql,
    //   [...R,
    //       `indep_hash IN ( ${R.range(0, ids.length)
    //         .map(() => "?")
    //         .join(", ")} )`,
    //     ].concat(
    //     ids
    //   )
    // );
  }

  if (before) {
    cql.where("timestamp < ?", before);
  }

  cql.where(
    "height >= ?",
    sortOrder === "HEIGHT_ASC"
      ? minHeight.add(offset).toString()
      : minHeight.toString()
  );

  cql.where(
    "height <= ?",
    sortOrder === "HEIGHT_DESC"
      ? (maxHeight as CassandraTypes.Long).sub(offset).toString()
      : maxHeight.toString()
  );

  cql.limit(fetchSize);

  return cql.build();
}

export interface DeferedBlockQueryParameters {
  indep_hash: string;
  deferedSelect: string[];
}

export function generateDeferedBlockQuery(
  parameters: DeferedBlockQueryParameters
): CqlQuery {
  return Select()
    .table(tableId.TABLE_BLOCK, KEYSPACE)
    .where("indep_hash = ?", parameters.indep_hash)
    .field(parameters.deferedSelect)
    .build();
}

export function generateDeferedTxQuery(parameters: {
  tx_id: string;
  deferedSelect: unknown;
}): CqlQuery {
  return Select()
    .table(tableId.TABLE_TX, KEYSPACE)
    .where("tx_id = ?", parameters.tx_id)
    .field(parameters.deferedSelect)
    .build();
}

export function generateDeferedTxBlockQuery(
  height: CassandraTypes.Long,
  fieldSelect: unknown
): CqlQuery {
  return (
    Select()
      // .table(tableId.TABLE_GQL_BLOCK_ASC, KEYSPACE)
      .field(fieldSelect)
      .where("height = ?", height)
      // .where("bucket_number = ?", CONST.getGqlBlockHeightAscBucketNumber(height))
      // .where("partition_id = ?", CONST.getGqlBlockHeightAscPartitionName(height))
      // .where("bucket_id = ?", CONST.getGqlBlockHeightAscBucketName(height))
      .build()
  );
}
