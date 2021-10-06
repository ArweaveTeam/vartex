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
  const columns = filters
    .map(({ name, type }) => `${name} ${type},`)
    .join("\n");

  const primaryKeys =
    filters.length === 0
      ? "tag_sum"
      : `(tag_sum, ${filters.map((nt) => nt.name).join(", ")})`;
  return `
    CREATE TABLE IF NOT EXISTS tx_tag_gql_by_${tableName}_asc_migration_0 (
           tag_sum text,
           ${columns}
           tx_index bigint,
           data_item_index bigint,
           PRIMARY KEY (${primaryKeys}, tx_index, data_item_index)
        )
    WITH CLUSTERING ORDER BY (tx_index ASC, data_item_index ASC)

    CREATE TABLE IF NOT EXISTS tx_tag_gql_by_${tableName}_desc_migration_0 (
           tag_sum text,
           ${columns}
           tx_index bigint,
           data_item_index bigint,
           PRIMARY KEY (${primaryKeys}, tx_index, data_item_index)
        )
    WITH CLUSTERING ORDER BY (tx_index DESC, data_item_index DESC)`;
}

// tags of 1 (no tx filter other than tag)
const tagFilters1 = [generateTagFilterTables("tagsum", [])];

// tags of 2
const tagFilters2 = [
  // tags ids
  generateTagFilterTables("tagsum_and_tx_id", [
    { name: "tx_id", type: "string" },
  ]),
  // tags owners
  generateTagFilterTables("tagsum_and_owner", [
    { name: "owner", type: "string" },
  ]),
  // tags recipient (target)
  generateTagFilterTables("tagsum_and_target", [
    { name: "target", type: "string" },
  ]),
  // tags bundleid
  generateTagFilterTables("tagsum_and_bundle_id", [
    { name: "bundle_id", type: "string" },
  ]),
  // tags dataRoot
  generateTagFilterTables("tagsum_and_data_root", [
    { name: "data_root", type: "string" },
  ]),
];

// tags of 3
const tagFilters3 = [
  // tags+ids+owners
  generateTagFilterTables("tagsum_and_tx_id_and_owner", [
    { name: "tx_id", type: "string" },
    { name: "owner", type: "string" },
  ]),
  // tags+ids+recipients(target)
  generateTagFilterTables("tagsum_and_tx_id_and_target", [
    { name: "tx_id", type: "string" },
    { name: "target", type: "string" },
  ]),
  // tags+ids+bundleIds
  generateTagFilterTables("tagsum_and_tx_id_and_bundle_id", [
    { name: "tx_id", type: "string" },
    { name: "bundle_id", type: "string" },
  ]),
  // tags+ids+dataRoot
  generateTagFilterTables("tagsum_and_tx_id_and_data_root", [
    { name: "tx_id", type: "string" },
    { name: "data_root", type: "string" },
  ]),
  // tags+owners+recipients(target)
  generateTagFilterTables("tagsum_and_owner_and_target", [
    { name: "owner", type: "string" },
    { name: "target", type: "string" },
  ]),
  // tags+owners+bundleIds
  generateTagFilterTables("tagsum_and_owner_and_bundle_id", [
    { name: "owner", type: "string" },
    { name: "bundle_id", type: "string" },
  ]),
  // tags+owners+dataRoot
  generateTagFilterTables("tagsum_and_owner_and_data_root", [
    { name: "owner", type: "string" },
    { name: "data_root", type: "string" },
  ]),
  // tags+target+bundleId
  generateTagFilterTables("tagsum_and_target_and_bundle_id", [
    { name: "target", type: "string" },
    { name: "bundle_id", type: "string" },
  ]),
  // tags+target+dataRoot
  generateTagFilterTables("tagsum_and_target_and_data_root", [
    { name: "target", type: "string" },
    { name: "data_root", type: "string" },
  ]),
  // tags+bundleId+dataRoot
  generateTagFilterTables("tagsum_and_bundle_id_and_data_root", [
    { name: "bundle_id", type: "string" },
    { name: "data_root", type: "string" },
  ]),
];

// tags of 4
const tagFilters4 = [
  // tags+ids+owners+target
  generateTagFilterTables("tagsum_and_tx_id_and_owner_and_target", [
    { name: "tx_id", type: "string" },
    { name: "owner", type: "string" },
    { name: "target", type: "string" },
  ]),
  // tags+ids+owners+bundle_id
  generateTagFilterTables("tagsum_and_tx_id_and_owner_and_bundle_id", [
    { name: "tx_id", type: "string" },
    { name: "owner", type: "string" },
    { name: "bundle_id", type: "string" },
  ]),
  // tags+ids+owners+data_root
  generateTagFilterTables("tagsum_and_tx_id_and_owner_and_data_root", [
    { name: "tx_id", type: "string" },
    { name: "owner", type: "string" },
    { name: "data_root", type: "string" },
  ]),
  // tags+ids+target+bundle_id
  generateTagFilterTables("tagsum_and_tx_id_and_target_and_bundle_id", [
    { name: "tx_id", type: "string" },
    { name: "target", type: "string" },
    { name: "bundle_id", type: "string" },
  ]),
  // tags+ids+target+data_root
  generateTagFilterTables("tagsum_and_tx_id_and_target_and_data_root", [
    { name: "tx_id", type: "string" },
    { name: "target", type: "string" },
    { name: "data_root", type: "string" },
  ]),
  // tags+ids+bundle_id+data_root
  generateTagFilterTables("tagsum_and_tx_id_and_bundle_id_and_data_root", [
    { name: "tx_id", type: "string" },
    { name: "bundle_id", type: "string" },
    { name: "data_root", type: "string" },
  ]),
  // tags+owner+target+bundle_id
  generateTagFilterTables("tagsum_and_owner_and_target_and_bundle_id", [
    { name: "owner", type: "string" },
    { name: "target", type: "string" },
    { name: "bundle_id", type: "string" },
  ]),
  // tags+owner+target+data_root
  generateTagFilterTables("tagsum_and_owner_and_target_and_data_root", [
    { name: "owner", type: "string" },
    { name: "target", type: "string" },
    { name: "data_root", type: "string" },
  ]),
  // tags+owner+bundle_id+data_root
  generateTagFilterTables("tagsum_and_owner_and_bundle_id_and_data_root", [
    { name: "owner", type: "string" },
    { name: "bundle_id", type: "string" },
    { name: "data_root", type: "string" },
  ]),
  // tags+target+bundle_id+data_root
  generateTagFilterTables("tagsum_and_target_and_bundle_id_and_data_root", [
    { name: "target", type: "string" },
    { name: "bundle_id", type: "string" },
    { name: "data_root", type: "string" },
  ]),
];

// tage of 5
const tagFilters5 = [
  // tags+ids+owners+target+bundle_id
  generateTagFilterTables(
    "tagsum_and_tx_id_and_owner_and_target_and_bundle_id",
    [
      { name: "tx_id", type: "string" },
      { name: "owner", type: "string" },
      { name: "target", type: "string" },
      { name: "bundle_id", type: "string" },
    ]
  ),
  // tags+ids+owners+target+data_root
  generateTagFilterTables(
    "tagsum_and_tx_id_and_owner_and_target_and_data_root",
    [
      { name: "tx_id", type: "string" },
      { name: "owner", type: "string" },
      { name: "target", type: "string" },
      { name: "data_root", type: "string" },
    ]
  ),
  // tags+ids+owners+bundle_id+data_root
  generateTagFilterTables(
    "tagsum_and_tx_id_and_owner_and_bundle_id_and_data_root",
    [
      { name: "tx_id", type: "string" },
      { name: "owner", type: "string" },
      { name: "bundle_id", type: "string" },
      { name: "data_root", type: "string" },
    ]
  ),
  // tags+ids+target+bundle_id+data_root
  generateTagFilterTables(
    "tagsum_and_tx_id_and_target_and_bundle_id_and_data_root",
    [
      { name: "tx_id", type: "string" },
      { name: "target", type: "string" },
      { name: "bundle_id", type: "string" },
      { name: "data_root", type: "string" },
    ]
  ),
  // tags+owner+target+bundle_id+data_root
  generateTagFilterTables(
    "tagsum_and_owner_and_target_and_bundle_id_and_data_root",
    [
      { name: "owner", type: "string" },
      { name: "target", type: "string" },
      { name: "bundle_id", type: "string" },
      { name: "data_root", type: "string" },
    ]
  ),
];

// tags of 6
const tagFilters6 = [
  // tags+ids+owners+target+bundle_id+data_root
  generateTagFilterTables(
    "tagsum_and_tx_id_and_owner_and_target_and_bundle_id_and_data_root",
    [
      { name: "tx_id", type: "string" },
      { name: "owner", type: "string" },
      { name: "target", type: "string" },
      { name: "bundle_id", type: "string" },
      { name: "data_root", type: "string" },
    ]
  ),
];

module.exports = concatAll([
  tagFilters1,
  tagFilters2,
  tagFilters3,
  tagFilters4,
  tagFilters5,
  tagFilters6,
]).join(`\n`);
