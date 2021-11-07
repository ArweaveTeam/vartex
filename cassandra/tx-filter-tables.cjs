const R = require("rambda");
const concatAll = R.converge(R.reduce(R.concat), [R.head, R.tail]);

// of 5
// tx_id owner recipient bundled_in data_root

// of 4
// tx_id owner recipient bundled_in
// tx_id owner recipient data_root
// tx_id owner bundled_in data_root
// tx_id recipient bundled_in data_root
// owner recipient bundled_in data_root

// of 3
// tx_id owner recipient
// tx_id owner bundled_in
// tx_id owner data_root
// tx_id recipient bundled_in
// tx_id recipient data_root
// tx_id bundled_in data_root
// owner recipient bundled_in
// owner recipient data_root
// owner bundled_in data_root
// recipient bundled_in data_root

// of 2
// tx_id owner
// tx_id recipient
// tx_id bundled_in
// tx_id data_root
// owner recipient
// owner bundled_in
// owner data_root
// recipient bundled_in
// recipient data_root
// bundled_in data_root

// of 1
// owner
// recipient
// bundled_in
// data_root

function generateFilterTables(filters) {
  const newCols = R.reject(R.propEq("name", "tx_id"), filters);
  const tableName = R.pluck("name", filters).join("_");
  const columns = newCols
    .map(({ name, type }) => `${name} ${type},`)
    .join("\n");

  const extraPrimaryKeys = filters.map((nt) => nt.name).join(", ");

  return [
    `
      CREATE TABLE IF NOT EXISTS tx_gql_by_${tableName}_asc (
           tx_id text,
           tx_index bigint,
           data_item_index bigint,
           tag_pairs frozen<list<text>>,
           ${columns}
           PRIMARY KEY ((${extraPrimaryKeys}), tx_index, data_item_index)
        )
      WITH CLUSTERING ORDER BY (tx_index ASC, data_item_index ASC);
    `,
    `CREATE INDEX IF NOT EXISTS tx_gql_by_${tableName}_asc_tag_pairs_index ON tx_gql_by_${tableName}_asc(full(tag_pairs));`,
    `CREATE TABLE IF NOT EXISTS tx_gql_by_${tableName}_desc (
           tx_id text,
           tx_index bigint,
           data_item_index bigint,
           tag_pairs frozen<list<text>>,
           ${columns}
           PRIMARY KEY ((${extraPrimaryKeys}), tx_index, data_item_index)
        )
    WITH CLUSTERING ORDER BY (tx_index DESC, data_item_index DESC);`,
    `CREATE INDEX IF NOT EXISTS tx_gql_by_${tableName}_desc_tag_pairs_index ON tx_gql_by_${tableName}_desc(full(tag_pairs));`,
  ];
}

const filterTables1 = R.flatten([
  // owner
  generateFilterTables([{ name: "owner", type: "text" }]),
  // recipient (target)
  generateFilterTables([{ name: "target", type: "text" }]),
  // bundlenId
  generateFilterTables([{ name: "bundled_in", type: "text" }]),
  // dataRoot
  generateFilterTables([{ name: "data_root", type: "text" }]),
]);

const filterTables2 = R.flatten([
  // tx_id owner
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
  ]),
  // tx_id recipient (target)
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "target", type: "text" },
  ]),
  // tx_id bundledId
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tx_id dataRoot
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // owner recipient
  generateFilterTables([
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
  ]),
  // owner bundled_in
  generateFilterTables([
    { name: "owner", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // owner data_root
  generateFilterTables([
    { name: "owner", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // recipient bundled_in
  generateFilterTables([
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // recipient data_root
  generateFilterTables([
    { name: "target", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // bundled_in data_root
  generateFilterTables([
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
]);

const filterTables3 = R.flatten([
  // tx_id owner recipient
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
  ]),
  // tx_id owner bundled_in
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tx_id owner data_root
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tx_id recipient bundled_in
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tx_id recipient data_root
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "target", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tx_id bundled_in data_root
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // owner recipient bundled_in
  generateFilterTables([
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // owner recipient data_root
  generateFilterTables([
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // owner bundled_in data_root
  generateFilterTables([
    { name: "owner", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // recipient bundled_in data_root
  generateFilterTables([
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
]);

const filterTables4 = R.flatten([
  // tx_id owner recipient bundled_in
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
  ]),
  // tx_id owner recipient data_root
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tx_id owner bundled_in data_root
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // tx_id recipient bundled_in data_root
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
  // owner recipient bundled_in data_root
  generateFilterTables([
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
]);

const filterTables5 = R.flatten([
  // tx_id owner recipient bundled_in data_root
  generateFilterTables([
    { name: "tx_id", type: "text" },
    { name: "owner", type: "text" },
    { name: "target", type: "text" },
    { name: "bundled_in", type: "text" },
    { name: "data_root", type: "text" },
  ]),
]);

module.exports = concatAll([
  filterTables1,
  filterTables2,
  filterTables3,
  filterTables4,
  filterTables5,
]);
