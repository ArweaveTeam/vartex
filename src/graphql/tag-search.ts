import { propOr } from "rambda";
import { cassandraClient } from "../database/cassandra";
import { toB64url } from "../query/transaction";
import { KEYSPACE } from "../constants";

const filtersToTable = {
  HEIGHT_ASC: {
    bundledId_dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_asc_migration_0",
    bundledId_dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_asc_migration_0",
    bundledId_dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_asc_migration_0",
    bundledId_dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_asc_migration_0",
    dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_asc_migration_0",
    bundledId_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_asc_migration_0",
    bundledId_dataRoots_recipients_tags:
      "tx_tag_gql_by_target_and_bundled_in_and_data_root_asc_migration_0",
    bundledId_dataRoots_owners_tags:
      "tx_tag_gql_by_owner_and_bundled_in_and_data_root_asc_migration_0",
    dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_data_root_asc_migration_0",
    bundledId_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_asc_migration_0",
    bundledId_dataRoots_ids_tags:
      "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_asc_migration_0",
    dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_data_root_asc_migration_0",
    bundledId_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_asc_migration_0",
    dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_data_root_asc_migration_0",
    bundledId_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc_migration_0",
    ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_asc_migration_0",
    bundledId_dataRoots_tags:
      "tx_tag_gql_by_bundled_in_and_data_root_asc_migration_0",
    dataRoots_recipients_tags:
      "tx_tag_gql_by_target_and_data_root_asc_migration_0",
    bundledId_recipients_tags:
      "tx_tag_gql_by_target_and_bundled_in_asc_migration_0",
    dataRoots_owners_tags: "tx_tag_gql_by_owner_and_data_root_asc_migration_0",
    bundledId_owners_tags: "tx_tag_gql_by_owner_and_bundled_in_asc_migration_0",
    owners_recipients_tags: "tx_tag_gql_by_owner_and_target_asc_migration_0",
    dataRoots_ids_tags: "tx_tag_gql_by_tx_id_and_data_root_asc_migration_0",
    bundledId_ids_tags: "tx_tag_gql_by_tx_id_and_bundled_in_asc_migration_0",
    ids_recipients_tags: "tx_tag_gql_by_tx_id_and_target_asc_migration_0",
    ids_owners_tags: "tx_tag_gql_by_tx_id_and_owner_asc_migration_0",
    dataRoots_tags: "tx_tag_gql_by_data_root_asc_migration_0",
    bundledId_tags: "tx_tag_gql_by_bundled_in_asc_migration_0",
    recipients_tags: "tx_tag_gql_by_target_asc_migration_0",
    owners_tags: "tx_tag_gql_by_owner_asc_migration_0",
    ids_tags: "tx_tag_gql_by_tx_id_asc_migration_0",
    tags: "tx_tag_gql_asc_migration_0",
  },
  HEIGHT_DESC: {
    bundledId_dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_desc_migration_0",
    bundledId_dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_desc_migration_0",
    bundledId_dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_desc_migration_0",
    bundledId_dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_desc_migration_0",
    dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_desc_migration_0",
    bundledId_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_desc_migration_0",
    bundledId_dataRoots_recipients_tags:
      "tx_tag_gql_by_target_and_bundled_in_and_data_root_desc_migration_0",
    bundledId_dataRoots_owners_tags:
      "tx_tag_gql_by_owner_and_bundled_in_and_data_root_desc_migration_0",
    dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_data_root_desc_migration_0",
    bundledId_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_desc_migration_0",
    bundledId_dataRoots_ids_tags:
      "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_desc_migration_0",
    dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_data_root_desc_migration_0",
    bundledId_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_desc_migration_0",
    dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_data_root_desc_migration_0",
    bundledId_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_desc_migration_0",
    ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_desc_migration_0",
    bundledId_dataRoots_tags:
      "tx_tag_gql_by_bundled_in_and_data_root_desc_migration_0",
    dataRoots_recipients_tags:
      "tx_tag_gql_by_target_and_data_root_desc_migration_0",
    bundledId_recipients_tags:
      "tx_tag_gql_by_target_and_bundled_in_desc_migration_0",
    dataRoots_owners_tags: "tx_tag_gql_by_owner_and_data_root_desc_migration_0",
    bundledId_owners_tags:
      "tx_tag_gql_by_owner_and_bundled_in_desc_migration_0",
    owners_recipients_tags: "tx_tag_gql_by_owner_and_target_desc_migration_0",
    dataRoots_ids_tags: "tx_tag_gql_by_tx_id_and_data_root_desc_migration_0",
    bundledId_ids_tags: "tx_tag_gql_by_tx_id_and_bundled_in_desc_migration_0",
    ids_recipients_tags: "tx_tag_gql_by_tx_id_and_target_desc_migration_0",
    ids_owners_tags: "tx_tag_gql_by_tx_id_and_owner_desc_migration_0",
    dataRoots_tags: "tx_tag_gql_by_data_root_desc_migration_0",
    bundledId_tags: "tx_tag_gql_by_bundled_in_desc_migration_0",
    recipients_tags: "tx_tag_gql_by_target_desc_migration_0",
    owners_tags: "tx_tag_gql_by_owner_desc_migration_0",
    ids_tags: "tx_tag_gql_by_tx_id_desc_migration_0",
    tags: "tx_tag_gql_desc_migration_0",
  },
};

export const findTxIDsFromTagFilters = async ({
  tagFilterKeys,
  tagFilterVals,
  minHeight,
  maxHeight,
  limit,
  sortOrder,
}) => {
  console.log({
    tagFilterKeys,
    tagFilterVals,
    minHeight,
    maxHeight,
    limit,
    sortOrder,
  });
  const tableKey = tagFilterKeys.sort().join("_");
  const table = filtersToTable[sortOrder][tableKey];
  const [tagNameIn, tagsValIn] = tagFilterVals.tags.reduce(
    (acc, tagPair) => {
      acc[0].push(`'${toB64url(tagPair.name)}'`);
      tagPair.values.forEach((tagVal) => {
        acc[1].push(`'${toB64url(tagVal)}'`);
      });
      return acc;
    },
    [[], []]
  );
  const whereClause = tagFilterKeys.reduce((acc, key) => {
    if (key === "tags") {
      return acc;
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
      const whereValsStr =
        whereVals.length === 1
          ? ` = '${whereVals[0]}'`
          : `IN (${whereVals.map((wv) => `'${wv}'`).join(",")})`;
      return `${acc} AND ${cqlKey} ${whereValsStr}`;
    }
  }, "");

  // console.log(
  //   { tagsIn, whereClause },
  //   `SELECT tx_id FROM ${KEYSPACE}.${table} WHERE (tag_name, tag_value) IN (${tagsIn.join(
  //     ","
  //   )}) ${whereClause}`
  // );

  console.log(
    `SELECT tx_id FROM ${KEYSPACE}.${table} WHERE tag_name IN (${tagNameIn.join(
      ","
    )}) AND tag_value IN (${tagsValIn.join(",")}) ${whereClause}`
  );
  const tagQ = await cassandraClient.execute(
    `SELECT tx_id FROM ${KEYSPACE}.${table} WHERE tag_name IN (${tagNameIn.join(
      ","
    )}) AND tag_value IN (${tagsValIn.join(",")}) ${whereClause}`
  );

  return (tagQ as any).rows;
};
