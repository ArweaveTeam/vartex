import * as R from "rambda";
import { types as CassandraTypes } from "cassandra-driver";
import * as Constants from "../database/constants";
import { config } from "dotenv";
import { KEYSPACE } from "../constants";
import { TagFilter } from "./types";
import { toB64url } from "../query/transaction";
import { default as cqlBuilder } from "@ridi/cql-builder";

const { Select } = cqlBuilder;

export type TxSortOrder = "HEIGHT_ASC" | "HEIGHT_DESC";

process.env.NODE_ENV !== "test" && config();

export interface QueryParameters {
  recipients?: string[];
  from?: string[];
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
}

interface CqlQuery {
  query: string;
  params: unknown[];
}

export function generateTransactionQuery(
  parameters: QueryParameters
): CqlQuery {
  let table = "tx_id_gql_desc";

  table =
    parameters.sortOrder === "HEIGHT_ASC" ? "tx_id_gql_asc" : "tx_id_gql_desc";

  const cql = Select()
    .table(table, KEYSPACE)
    .field(parameters.select)
    .filtering();

  if (parameters.id !== undefined) {
    cql.where("tx_id = ?", parameters.id);
    return cql.build();
  } else if (parameters.ids && Array.isArray(parameters.ids)) {
    cql.where.apply(
      cql,
      R.concat(
        [
          `tx_id IN ( ${R.range(0, parameters.ids.length)
            .map(() => "?")
            .join(", ")} )`,
        ],
        parameters.ids
      )
    );
  }

  if (Array.isArray(parameters.tags) && !R.isEmpty(parameters.tags)) {
    for (const { name, values = "" } of parameters.tags) {
      if (Array.isArray(values)) {
        for (const value of values) {
          cql.where(
            "tags CONTAINS (?, ?)",
            toB64url(name || ""),
            toB64url(value || "")
          );
        }
      } else {
        cql.where(
          "tags CONTAINS (?, ?)",
          toB64url(name || ""),
          toB64url(values || "")
        );
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

  if (parameters.recipients && !R.isEmpty(parameters.recipients)) {
    cql.where(
      `target IN ( ${parameters.recipients.map(() => "?").join(", ")} )`
    );
  }

  // if (params.before) {
  //   cql.where('timestamp < ?', params.before);
  // }

  cql.where(
    "tx_index >= ?",
    parameters.sortOrder === "HEIGHT_ASC"
      ? parameters.minHeight.add(parameters.offset).toString()
      : parameters.minHeight.toString()
  );

  cql.where(
    "tx_index <= ?",
    parameters.sortOrder === "HEIGHT_DESC"
      ? parameters.maxHeight.sub(parameters.offset).toString()
      : parameters.maxHeight.toString()
  );

  cql.limit(parameters.limit);

  return cql.build();
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
    .table(
      sortOrder === "HEIGHT_ASC"
        ? "block_gql_asc_migration_1"
        : "block_gql_desc_migration_1",
      KEYSPACE
    )
    .field(
      select.includes("indep_hash") ? select : R.append("indep_hash", select)
    )
    .filtering();

  // const query = connection.queryBuilder().select(select).from('blocks');
  if (id) {
    cql.where("indep_hash = ?", id);
  } else if (ids && Array.isArray(ids) && !R.isEmpty(ids)) {
    cql.where.apply(
      cql,
      R.concat(
        [
          `indep_hash IN ( ${R.range(0, ids.length)
            .map(() => "?")
            .join(", ")} )`,
        ],
        ids
      )
    );
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
    .table("block", KEYSPACE)
    .where("indep_hash = ?", parameters.indep_hash)
    .field(parameters.deferedSelect)
    .build();
}

export function generateDeferedTxQuery(parameters: {
  tx_id: string;
  deferedSelect: unknown;
}): CqlQuery {
  return Select()
    .table("transaction", KEYSPACE)
    .where("tx_id = ?", parameters.tx_id)
    .field(parameters.deferedSelect)
    .build();
}

export function generateDeferedTxBlockQuery(
  height: CassandraTypes.Long,
  fieldSelect: unknown
): CqlQuery {
  return Select()
    .table("block_gql_asc", KEYSPACE)
    .field(fieldSelect)
    .where("height = ?", height)
    .where(
      "partition_id = ?",
      Constants.getGqlBlockHeightAscPartitionName(height)
    )
    .where("bucket_id = ?", Constants.getGqlBlockHeightAscBucketName(height))
    .build();
}

export function generateTagQuery(tags: TagFilter[]): CqlQuery {
  const cql = Select().table("tx_tag", KEYSPACE).field("tx_id").filtering();
  for (const tag of tags) {
    cql.where("name = ?", tag.name.toString());
    if (Array.isArray(tag.values)) {
      cql.where.apply(
        cql,
        R.concat(
          [
            `value IN ( ${R.range(0, tag.values.length)
              .map(() => "?")
              .join(", ")} )`,
          ],
          tag.values
        )
      );
    }
  }
  return cql.build();
}
