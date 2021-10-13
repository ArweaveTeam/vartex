const R = require("rambda");
const concatAll = R.converge(R.reduce(R.concat), [R.head, R.tail]);

// // of 6
// tags ids owners recipients bundledId dataRoot

// // of 5
// tags ids owners recipients bundledId
// tags ids owners recipients dataRoot
// tags ids owners bundledId dataRoot
// tags ids recipients bundledId dataRoot
// tags owners recipients bundledId dataRoot

// // of 4
// tags ids owners recipients
// tags ids owners bundledId
// tags ids owners dataRoot
// tags ids recipients bundledId
// tags ids recipients dataRoot
// tags ids bundledId dataRoot
// tags owners recipients bundledId
// tags owners recipients dataRoot
// tags owners bundledId dataRoot
// tags recipients bundledId dataRoot

// // of 3
// tags ids owners
// tags ids recipients
// tags ids bundledId
// tags ids dataRoot
// tags owners recipients
// tags owners bundledId
// tags owners dataRoot
// tags recipients bundledId
// tags recipients dataRoot
// tags bundledId dataRoot

// // of 2
// tags ids
// tags owners
// tags recipients
// tags bundledId
// tags dataRoot

// tags of 1
// tags

function generateTagFilterTables(tableName, filters) {
  const newCols = R.reject(R.propEq("name", "tx_id"), filters);
  const columns = newCols
    .map(({ name, type }) => `${name} ${type},`)
    .join("\n");

  const extraPrimaryKeys =
    filters.length === 0 ? "" : `, ${filters.map((nt) => nt.name).join(", ")}`;

  return [
    `
    CREATE TABLE IF NOT EXISTS tx_tag_gql${
      filters.length !== 0 ? "_by_" : ""
    }${tableName}_asc_migration_1 (
           tag_pair text,
           tx_id text,
           tx_index bigint,
           data_item_index bigint,
           tag_index int,
           ${columns}
           PRIMARY KEY ((tag_pair ${extraPrimaryKeys}), tx_index, data_item_index, tag_index)
        )
    WITH CLUSTERING ORDER BY (tx_index ASC, data_item_index ASC, tag_index ASC)`,

    `CREATE TABLE IF NOT EXISTS tx_tag_gql${
      filters.length !== 0 ? "_by_" : ""
    }${tableName}_desc_migration_1 (
           tag_pair text,
           tx_id text,
           tx_index bigint,
           data_item_index bigint,
           tag_index int,
           ${columns}
           PRIMARY KEY ((tag_pair ${extraPrimaryKeys}), tx_index, data_item_index, tag_index)
        )
    WITH CLUSTERING ORDER BY (tx_index DESC, data_item_index DESC, tag_index DESC);
`,
  ];
}

// tags of 1 (no tx filter other than tag)
const tagFilters1 = R.flatten([generateTagFilterTables("", [])]);

// tags of 2
const tagFilters2 = R.flatten([
  // tags ids
  generateTagFilterTables("tx_id", [{ name: "tx_id", type: "text" }]),
  // tags owners
  generateTagFilterTables("owner", [{ name: "owner", type: "text" }]),
  // tags recipient (target)
  generateTagFilterTables("target", [{ name: "target", type: "text" }]),
  // tags bundleid
  generateTagFilterTables("bundled_in", [{ name: "bundled_in", type: "text" }]),
  // tags dataRoot
  generateTagFilterTables("data_root", [{ name: "data_root", type: "text" }]),
]);

// tags of 3
const tagFilters3 = R.flatten([
  // tags+ids+owners
  generateTagFilterTables("tx_id_and_owner", [
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
  ]),
  // tags+ids+recipients(target)
  generateTagFilterTables("tx_id_and_target", [
    { name: "tx_id", type: "text" },
    { name: "target", type: "text" },
  ]),
  // tags+ids+bundleIds
  generateTagFilterTables("tx_id_and_bundled_in", [
    { name: "tx_id", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tags+ids+dataRoot
  generateTagFilterTables("tx_id_and_data_root", [
    { name: "tx_id", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+owners+recipients(target)
  generateTagFilterTables("owner_and_target", [
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
  ]),
  // tags+owners+bundleIds
  generateTagFilterTables("owner_and_bundled_in", [
    { name: "owner", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tags+owners+dataRoot
  generateTagFilterTables("owner_and_data_root", [
    { name: "owner", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+target+bundleId
  generateTagFilterTables("target_and_bundled_in", [
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tags+target+dataRoot
  generateTagFilterTables("target_and_data_root", [
    { name: "target", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+bundleId+dataRoot
  generateTagFilterTables("bundled_in_and_data_root", [
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
]);

// tags of 4
const tagFilters4 = R.flatten([
  // tags+ids+owners+target
  generateTagFilterTables("tx_id_and_owner_and_target", [
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
  ]),
  // tags+ids+owners+bundled_in
  generateTagFilterTables("tx_id_and_owner_and_bundled_in", [
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tags+ids+owners+data_root
  generateTagFilterTables("tx_id_and_owner_and_data_root", [
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+ids+target+bundled_in
  generateTagFilterTables("tx_id_and_target_and_bundled_in", [
    { name: "tx_id", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tags+ids+target+data_root
  generateTagFilterTables("tx_id_and_target_and_data_root", [
    { name: "tx_id", type: "text" },
    { name: "target", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+ids+bundled_in+data_root
  generateTagFilterTables("tx_id_and_bundled_in_and_data_root", [
    { name: "tx_id", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+owner+target+bundled_in
  generateTagFilterTables("owner_and_target_and_bundled_in", [
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tags+owner+target+data_root
  generateTagFilterTables("owner_and_target_and_data_root", [
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+owner+bundled_in+data_root
  generateTagFilterTables("owner_and_bundled_in_and_data_root", [
    { name: "owner", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+target+bundled_in+data_root
  generateTagFilterTables("target_and_bundled_in_and_data_root", [
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
]);

// tage of 5
const tagFilters5 = R.flatten([
  // tags+ids+owners+target+bundled_in
  generateTagFilterTables("tx_id_and_owner_and_target_and_bundled_in", [
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tags+ids+owners+target+data_root
  generateTagFilterTables("tx_id_and_owner_and_target_and_data_root", [
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+ids+owners+bundled_in+data_root
  generateTagFilterTables("tx_id_and_owner_and_bundled_in_and_data_root", [
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+ids+target+bundled_in+data_root
  generateTagFilterTables("tx_id_and_target_and_bundled_in_and_data_root", [
    { name: "tx_id", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tags+owner+target+bundled_in+data_root
  generateTagFilterTables("owner_and_target_and_bundled_in_and_data_root", [
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
]);

// tags of 6
const tagFilters6 = R.flatten([
  // tags+ids+owners+target+bundled_in+data_root
  generateTagFilterTables(
    "tx_id_and_owner_and_target_and_bundled_in_and_data_root",
    [
      { name: "tx_id", type: "text" },
      { name: "owner", type: "text" },
      { name: "target", type: "text" },
      { name: "bundled_in", type: "text" },
      { name: "data_root", type: "text" },
    ]
  ),
]);

const tables = [
  "tx_tag_gql_asc_migration_0",
  "tx_tag_gql_desc_migration_0",
  "tx_tag_gql_by_tx_id_asc_migration_0",
  "tx_tag_gql_by_tx_id_desc_migration_0",
  "tx_tag_gql_by_owner_asc_migration_0",
  "tx_tag_gql_by_owner_desc_migration_0",
  "tx_tag_gql_by_target_asc_migration_0",
  "tx_tag_gql_by_target_desc_migration_0",
  "tx_tag_gql_by_bundled_in_asc_migration_0",
  "tx_tag_gql_by_bundled_in_desc_migration_0",
  "tx_tag_gql_by_data_root_asc_migration_0",
  "tx_tag_gql_by_data_root_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_target_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_target_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_bundled_in_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_bundled_in_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_data_root_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_data_root_desc_migration_0",
  "tx_tag_gql_by_owner_and_target_asc_migration_0",
  "tx_tag_gql_by_owner_and_target_desc_migration_0",
  "tx_tag_gql_by_owner_and_bundled_in_asc_migration_0",
  "tx_tag_gql_by_owner_and_bundled_in_desc_migration_0",
  "tx_tag_gql_by_owner_and_data_root_asc_migration_0",
  "tx_tag_gql_by_owner_and_data_root_desc_migration_0",
  "tx_tag_gql_by_target_and_bundled_in_asc_migration_0",
  "tx_tag_gql_by_target_and_bundled_in_desc_migration_0",
  "tx_tag_gql_by_target_and_data_root_asc_migration_0",
  "tx_tag_gql_by_target_and_data_root_desc_migration_0",
  "tx_tag_gql_by_bundled_in_and_data_root_asc_migration_0",
  "tx_tag_gql_by_bundled_in_and_data_root_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_target_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_target_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_data_root_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_data_root_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_target_and_bundled_in_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_target_and_bundled_in_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_target_and_data_root_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_target_and_data_root_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_desc_migration_0",
  "tx_tag_gql_by_owner_and_target_and_bundled_in_asc_migration_0",
  "tx_tag_gql_by_owner_and_target_and_bundled_in_desc_migration_0",
  "tx_tag_gql_by_owner_and_target_and_data_root_asc_migration_0",
  "tx_tag_gql_by_owner_and_target_and_data_root_desc_migration_0",
  "tx_tag_gql_by_owner_and_bundled_in_and_data_root_asc_migration_0",
  "tx_tag_gql_by_owner_and_bundled_in_and_data_root_desc_migration_0",
  "tx_tag_gql_by_target_and_bundled_in_and_data_root_asc_migration_0",
  "tx_tag_gql_by_target_and_bundled_in_and_data_root_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_desc_migration_0",
  "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_asc_migration_0",
  "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_desc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_asc_migration_0",
  "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_desc_migration_0",
];

module.exports = {
  tables,
  createTableQueries: concatAll([
    tagFilters1,
    tagFilters2,
    tagFilters3,
    tagFilters4,
    tagFilters5,
    tagFilters6,
  ]),
};
