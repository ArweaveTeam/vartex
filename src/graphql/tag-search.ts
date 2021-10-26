import { propOr } from "rambda";
import { cassandraClient } from "../database/cassandra";
import { types as CassandraTypes } from "cassandra-driver";
import { toB64url } from "../query/transaction";
import { KEYSPACE } from "../constants";

const filtersToTable: { [direction: string]: Record<string, string> } = {
  HEIGHT_ASC: {
    bundledId_dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_asc",
    bundledId_dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_asc",
    bundledId_dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_asc",
    bundledId_dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_asc",
    dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_asc",
    bundledId_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_asc",
    bundledId_dataRoots_recipients_tags:
      "tx_tag_gql_by_target_and_bundled_in_and_data_root_asc",
    bundledId_dataRoots_owners_tags:
      "tx_tag_gql_by_owner_and_bundled_in_and_data_root_asc",
    dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_data_root_asc",
    bundledId_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_asc",
    bundledId_dataRoots_ids_tags:
      "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_asc",
    dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_data_root_asc",
    bundledId_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_asc",
    dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_data_root_asc",
    bundledId_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc",
    ids_owners_recipients_tags: "tx_tag_gql_by_tx_id_and_owner_and_target_asc",
    bundledId_dataRoots_tags: "tx_tag_gql_by_bundled_in_and_data_root_asc",
    dataRoots_recipients_tags: "tx_tag_gql_by_target_and_data_root_asc",
    bundledId_recipients_tags: "tx_tag_gql_by_target_and_bundled_in_asc",
    dataRoots_owners_tags: "tx_tag_gql_by_owner_and_data_root_asc",
    bundledId_owners_tags: "tx_tag_gql_by_owner_and_bundled_in_asc",
    owners_recipients_tags: "tx_tag_gql_by_owner_and_target_asc",
    dataRoots_ids_tags: "tx_tag_gql_by_tx_id_and_data_root_asc",
    bundledId_ids_tags: "tx_tag_gql_by_tx_id_and_bundled_in_asc",
    ids_recipients_tags: "tx_tag_gql_by_tx_id_and_target_asc",
    ids_owners_tags: "tx_tag_gql_by_tx_id_and_owner_asc",
    dataRoots_tags: "tx_tag_gql_by_data_root_asc",
    bundledId_tags: "tx_tag_gql_by_bundled_in_asc",
    recipients_tags: "tx_tag_gql_by_target_asc",
    owners_tags: "tx_tag_gql_by_owner_asc",
    ids_tags: "tx_tag_gql_by_tx_id_asc",
    tags: "tx_tag_gql_asc",
  },
  HEIGHT_DESC: {
    bundledId_dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_desc",
    bundledId_dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_desc",
    bundledId_dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_desc",
    bundledId_dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_desc",
    dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_desc",
    bundledId_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_desc",
    bundledId_dataRoots_recipients_tags:
      "tx_tag_gql_by_target_and_bundled_in_and_data_root_desc",
    bundledId_dataRoots_owners_tags:
      "tx_tag_gql_by_owner_and_bundled_in_and_data_root_desc",
    dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_data_root_desc",
    bundledId_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_desc",
    bundledId_dataRoots_ids_tags:
      "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_desc",
    dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_data_root_desc",
    bundledId_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_desc",
    dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_data_root_desc",
    bundledId_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_desc",
    ids_owners_recipients_tags: "tx_tag_gql_by_tx_id_and_owner_and_target_desc",
    bundledId_dataRoots_tags: "tx_tag_gql_by_bundled_in_and_data_root_desc",
    dataRoots_recipients_tags: "tx_tag_gql_by_target_and_data_root_desc",
    bundledId_recipients_tags: "tx_tag_gql_by_target_and_bundled_in_desc",
    dataRoots_owners_tags: "tx_tag_gql_by_owner_and_data_root_desc",
    bundledId_owners_tags: "tx_tag_gql_by_owner_and_bundled_in_desc",
    owners_recipients_tags: "tx_tag_gql_by_owner_and_target_desc",
    dataRoots_ids_tags: "tx_tag_gql_by_tx_id_and_data_root_desc",
    bundledId_ids_tags: "tx_tag_gql_by_tx_id_and_bundled_in_desc",
    ids_recipients_tags: "tx_tag_gql_by_tx_id_and_target_desc",
    ids_owners_tags: "tx_tag_gql_by_tx_id_and_owner_desc",
    dataRoots_tags: "tx_tag_gql_by_data_root_desc",
    bundledId_tags: "tx_tag_gql_by_bundled_in_desc",
    recipients_tags: "tx_tag_gql_by_target_desc",
    owners_tags: "tx_tag_gql_by_owner_desc",
    ids_tags: "tx_tag_gql_by_tx_id_desc",
    tags: "tx_tag_gql_desc",
  },
};

interface TagQueryFilter {
  name: string;
  values: string[];
}

export const findTxIDsFromTagFilters = async ({
  tagFilterKeys,
  tagFilterVals,
  minHeight,
  maxHeight,
  limit,
  offset,
  sortOrder,
}: {
  tagFilterKeys: string[];
  tagFilterVals: { [any_: string]: any; tags: TagQueryFilter[] };
  minHeight?: CassandraTypes.Long;
  maxHeight?: CassandraTypes.Long;
  limit?: number;
  offset?: number;
  sortOrder?: string;
}) => {
  console.log({
    tagFilterKeys,
    tagFilterVals,
    minHeight,
    maxHeight,
    limit,
    sortOrder,
  });
  const txsMinHeight =
    sortOrder === "HEIGHT_ASC"
      ? minHeight.add(offset).toString()
      : minHeight.toString();
  const txsMaxHeight =
    sortOrder === "HEIGHT_DESC"
      ? maxHeight.sub(offset).toString()
      : maxHeight.toString();
  const tableKey = tagFilterKeys.sort().join("_");
  const table = filtersToTable[sortOrder][tableKey];
  const tagPairsIn = tagFilterVals.tags.reduce((accumulator, tagPairs) => {
    // acc[0].push(`'${}'`);
    const tagName = toB64url(tagPairs.name || "");
    for (const tagValue of tagPairs.values) {
      accumulator.push(`'${tagName}-${toB64url(tagValue)}'`);
    }
    return accumulator;
  }, []);
  const whereClause = tagFilterKeys.reduce((accumulator, key) => {
    if (key === "tags") {
      return accumulator;
    } else {
      const whereVals = tagFilterVals[key];
      const cqlKey = propOr(
        key,
        key
      )({
        ids: "tx_id",
        recipients: "target",
        owners: "owner",
        dataRoots: "data_root",
        bundledIn: "bundled_in",
      });
      // if (cqlKey === "owner") {
      //   console.log({ whereVals });
      //   console.log("OWNER", whereVals.map(ownerToAddress));
      // }
      const whereValsString =
        whereVals.length === 1
          ? ` = '${whereVals[0]}'`
          : `IN (${whereVals.map((wv: string) => `'${wv}'`).join(",")})`;
      return `${accumulator} AND ${cqlKey} ${whereValsString}`;
    }
  }, "");
  console.log(
    `SELECT tx_id FROM ${KEYSPACE}.${table} WHERE tag_pair IN (${tagPairsIn.join(
      ","
    )}) ${whereClause}`
  );
  const tagQ = await cassandraClient.execute(
    `SELECT tx_id FROM ${KEYSPACE}.${table} WHERE tx_index <= ${txsMaxHeight} AND tx_index >= ${txsMinHeight} AND tag_pair IN (${tagPairsIn.join(
      ","
    )}) ${whereClause} LIMIT ${limit || 100}`
  );

  return (tagQ as any).rows;
};
