// 14/09/2021 - fixing incorrect sorting between different buckets

const tableRenames = {
  block_gql_asc: "block_gql_asc_migration_1",
  block_gql_desc: "block_gql_desc_migration_1",
  tx_id_gql_asc: "tx_id_gql_asc_migration_1",
  tx_id_gql_desc: "tx_id_gql_desc_migration_1",
  tx_tag_gql_by_name_asc: "tx_tag_gql_by_name_asc_migration_1",
  tx_tag_gql_by_name_desc: "tx_tag_gql_by_name_desc_migration_1",
  tx_tag: "tx_tag_migration_1",
};

const badTables = Object.keys(tableRenames);

const blockGqlInsertAscQuery = (keyspace, kvs) => {
  const query = `INSERT INTO ${keyspace}.block_gql_asc_migration_1
    (partition_id, bucket_id, bucket_number, height, indep_hash, timestamp, previous)
    VALUES (?, ?, ?, ?, ?, ?, ?)`;
  return [
    query,
    [
      kvs.partition_id,
      kvs.bucket_id,
      kvs.bucket_number,
      kvs.height,
      kvs.indep_hash,
      kvs.timestamp,
      kvs.previous,
    ],
  ];
};

const blockGqlInsertDescQuery = (keyspace, kvs) => {
  const query = `INSERT INTO ${keyspace}.block_gql_desc_migration_1
    (partition_id, bucket_id, bucket_number, height, indep_hash, timestamp, previous)
    VALUES (?, ?, ?, ?, ?, ?, ?)`;
  return [
    query,
    [
      kvs.partition_id,
      kvs.bucket_id,
      kvs.bucket_number,
      kvs.height,
      kvs.indep_hash,
      kvs.timestamp,
      kvs.previous,
    ],
  ];
};

const txIdGqlInsertAscQuery = (keyspace, kvs) => {
  const query = `INSERT INTO ${keyspace}.tx_id_gql_asc_migration_1
   (partition_id, bucket_id, bucket_number, tx_index, tags, tx_id, owner, target, bundle_id)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  return [
    query,
    [
      kvs.partition_id,
      kvs.bucket_id,
      kvs.bucket_number,
      kvs.tx_index,
      kvs.tags,
      kvs.tx_id,
      kvs.owner,
      kvs.target,
      kvs.bundle_id,
    ],
  ];
};

const txIdGqlInsertDescQuery = (keyspace, kvs) => {
  const query = `INSERT INTO ${keyspace}.tx_id_gql_desc_migration_1
   (partition_id, bucket_id, bucket_number, tx_index, tags, tx_id, owner, target, bundle_id)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;

  return [
    query,
    [
      kvs.partition_id,
      kvs.bucket_id,
      kvs.bucket_number,
      kvs.tx_index,
      kvs.tags,
      kvs.tx_id,
      kvs.owner,
      kvs.target,
      kvs.bundle_id,
    ],
  ];
};

const txTagInsertQuery = (keyspace, kvs) => {
  const txTagKeys = [
    "partition_id",
    "bucket_id",
    "bucket_number",
    "tx_id",
    "tx_index",
    "tag_index",
    "next_tag_index",
    "name",
    "value",
  ];
  const query = `INSERT INTO ${keyspace}.tx_tag_migration_1 (${txTagKeys.join(
    ", "
  )}) VALUES (${txTagKeys.map(() => "?").join(", ")})`;

  return [
    query,
    [
      kvs.partition_id,
      kvs.bucket_id,
      kvs.bucket_number,
      kvs.tx_id,
      kvs.tx_index,
      kvs.tag_index,
      kvs.next_tag_index,
      kvs.name,
      kvs.value,
    ],
  ];
};

const txTagGqlByNameInsertAscQuery = (keyspace, kvs) => {
  const query = `INSERT INTO ${keyspace}.tx_tag_gql_by_name_asc_migration_1
       (partition_id, bucket_id, bucket_number, tx_index, tags, tx_id, owner, target, bundle_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
  return [
    query,
    [
      kvs.partition_id,
      kvs.bucket_id,
      kvs.bucket_number,
      kvs.tx_index,
      kvs.tags,
      kvs.tx_id,
      kvs.owner,
      kvs.target,
      kvs.bundle_id,
    ],
  ];
};

const txTagGqlByNameInsertDescQuery = (keyspace, kvs) => {
  const query = `INSERT INTO ${keyspace}.tx_tag_gql_by_name_desc_migration_1
       (partition_id, bucket_id, bucket_number, tx_index, tags, tx_id, owner, target, bundle_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
  return [
    query,
    [
      kvs.partition_id,
      kvs.bucket_id,
      kvs.bucket_number,
      kvs.tx_index,
      kvs.tags,
      kvs.tx_id,
      kvs.owner,
      kvs.target,
      kvs.bundle_id,
    ],
  ];
};

const insertQueries = {
  block_gql_asc: blockGqlInsertAscQuery,
  block_gql_desc: blockGqlInsertDescQuery,
  tx_id_gql_asc: txIdGqlInsertAscQuery,
  tx_id_gql_desc: txIdGqlInsertDescQuery,
  tx_tag_gql_by_name_asc: txTagGqlByNameInsertAscQuery,
  tx_tag_gql_by_name_desc: txTagGqlByNameInsertDescQuery,
  tx_tag: txTagInsertQuery,
};

module.exports = async (client) => {
  const allTables = await client.execute("describe tables");
  const KEYSPACE = process.env["KEYSPACE"]
    ? process.env["KEYSPACE"]
    : "gateway";
  let warned = false;

  for (const row of allTables.rows) {
    if (
      row.keyspace_name === KEYSPACE &&
      row.type === "table" &&
      badTables.includes(row.name)
    ) {
      if (!warned) {
        console.error(
          `[migrate1] Unmigrated table ${row.name} detected, this may take a while...\n`,
          `Start vartex again with SKIP_MIGRATION=1 to skip, but be aware that in doing so will result in incorrect ordering of transactions!`
        );
        warned = true;
      }
      console.error(`migrating ${row.name} to ${row.name}_migration_1`);
      const result = await client.execute(
        `SELECT * FROM ${KEYSPACE}.${row.name}`,
        [],
        { prepare: true }
      );

      for await (const rowRes of result) {
        const [bucketNumericString] = rowRes.bucket_id.match(/\d+$/);
        const bucket_number = Number.parseInt(bucketNumericString);
        const [query, params] = insertQueries[row.name](KEYSPACE, {
          bucket_number,
          ...rowRes,
        });
        await client.execute(query, params, { prepare: true });
      }
      await client.execute(`DROP TABLE ${KEYSPACE}.${row.name}`, [], {
        prepare: true,
      });
    }
  }
};
