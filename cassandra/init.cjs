'use strict';
const cassandra = require('cassandra-driver');
require('dotenv').config();

const KEYSPACE = process.env['KEYSPACE'] ? process.env['KEYSPACE'] : 'gateway';

const contactPoints = process.env.CASSANDRA_CONTACT_POINTS
  ? JSON.parse(process.env.CASSANDRA_CONTACT_POINTS)
  : ['localhost:9042'];

const client = new cassandra.Client({
  contactPoints,
  localDataCenter: 'datacenter1',
  credentials: { username: 'cassandra', password: 'cassandra' },
});

client
  .connect()
  .then(function () {
    const queries = [
      `CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE}
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }`,
      `USE ${KEYSPACE}`,
      `CREATE TABLE IF NOT EXISTS poa (
         option text,
         tx_path text,
         data_path text,
         chunk text,
         block_hash text,
         block_height bigint,
         PRIMARY KEY (block_hash, block_height)
      )
      WITH CLUSTERING ORDER BY (block_height DESC)`,
      `CREATE TABLE IF NOT EXISTS block_height_by_block_hash (
         block_height bigint,
         block_hash text,
         PRIMARY KEY (block_height)
       )`,
      `CREATE TABLE IF NOT EXISTS block (
         block_size bigint,
         cumulative_diff text,
         diff bigint,
         hash text,
         hash_list_merkle text,
         height bigint,
         indep_hash text,
         last_retarget bigint,
         nonce text,
         previous_block text,
         reward_addr text,
         reward_pool bigint,
         tags list<frozen<tuple<text, text>>>,
         timestamp bigint,
         tx_root text,
         tx_tree frozen<list<text>>,
         txs frozen<list<text>>,
         wallet_list text,
         weave_size bigint,
         PRIMARY KEY (indep_hash)
       )`,
      // bucket at 500k
      `CREATE TABLE IF NOT EXISTS block_gql_asc (
          partition_id text,
          bucket_id text,
          height bigint,
          indep_hash text,
          previous text,
          timestamp timeuuid,
          PRIMARY KEY ((partition_id, bucket_id), height, timestamp)
        )
        WITH CLUSTERING ORDER BY (height ASC, timestamp ASC)`,
      // bucket at 500k
      `CREATE TABLE IF NOT EXISTS block_gql_desc (
          partition_id text,
          bucket_id text,
          height bigint,
          indep_hash text,
          previous text,
          timestamp timeuuid,
          PRIMARY KEY ((partition_id, bucket_id), height, timestamp)
        )
      WITH CLUSTERING ORDER BY (height DESC, timestamp DESC)`,

      // bucket at 50000
      `CREATE TABLE IF NOT EXISTS tx_id_gql_asc (
         partition_id text,
         bucket_id text,
         tx_index bigint,
         tags list<frozen<tuple<text,text>>>,
         tx_id text,
         owner text,
         target text,
         bundle_id text,
         PRIMARY KEY ((partition_id, bucket_id), tx_index)
       )
       WITH CLUSTERING ORDER BY (tx_index ASC)`,

      // bucket at 50000
      `CREATE TABLE IF NOT EXISTS tx_id_gql_desc (
         partition_id text,
         bucket_id text,
         tx_index bigint,
         tags list<frozen<tuple<text,text>>>,
         tx_id text,
         owner text,
         target text,
         bundle_id text,
         PRIMARY KEY ((partition_id, bucket_id), tx_index)
       )
       WITH CLUSTERING ORDER BY (tx_index DESC)`,

      // bucket at 50000
      `CREATE TABLE IF NOT EXISTS tx_tag (
         partition_id text,
         bucket_id text,
         tx_index bigint,
         tag_index int,
         tx_id text,
         next_tag_index int,
         name text,
         value text,
         PRIMARY KEY ((partition_id, bucket_id), tx_index, tag_index)
      )
      WITH CLUSTERING ORDER BY (tx_index DESC, tag_index DESC)`,

      // reuse tx_id tables for owners filters, optimize later
      `CREATE INDEX IF NOT EXISTS ON tx_id_gql_asc (owner)`,
      `CREATE INDEX IF NOT EXISTS ON tx_id_gql_desc (owner)`,
      // reuse tx_id tables for recipients filters, optimize later
      `CREATE INDEX IF NOT EXISTS ON tx_id_gql_asc (target)`,
      `CREATE INDEX IF NOT EXISTS ON tx_id_gql_desc (target)`,
      // reuse tx_id tables for bundle filters, optimize later
      `CREATE INDEX IF NOT EXISTS ON tx_id_gql_asc (bundle_id)`,
      `CREATE INDEX IF NOT EXISTS ON tx_id_gql_desc (bundle_id)`,

      // bucket at 50000
      `CREATE TABLE IF NOT EXISTS tx_tag_gql_by_name_asc (
         partition_id text,
         bucket_id text,
         tx_index bigint,
         tag_index int,
         tag_name text,
         tag_value text,
         tx_id text,
         owner text,
         target text,
         bundle_id text,
         PRIMARY KEY ((partition_id, bucket_id), tx_index, tag_index)
      )
      WITH CLUSTERING ORDER BY (tx_index ASC, tag_index ASC)`,
      // bucket at 50000
      `CREATE TABLE IF NOT EXISTS tx_tag_gql_by_name_desc (
         partition_id text,
         bucket_id text,
         tx_index bigint,
         tag_index int,
         tag_name text,
         tag_value text,
         tx_id text,
         owner text,
         target text,
         bundle_id text,
         PRIMARY KEY ((partition_id, bucket_id), tx_index, tag_index)
      )
      WITH CLUSTERING ORDER BY (tx_index DESC, tag_index DESC)`,
      `CREATE INDEX IF NOT EXISTS ON tx_tag_gql_by_name_asc (tag_name)`,
      `CREATE INDEX IF NOT EXISTS ON tx_tag_gql_by_name_desc (tag_name)`,

      `CREATE TABLE IF NOT EXISTS transaction (
        tx_index bigint,
        block_height bigint,
        block_hash text,
        data_root text,
        data_size bigint,
        data_tree frozen<list<text>>,
        format int,
        tx_id text,
        last_tx text,
        owner text,
        quantity bigint,
        reward bigint,
        signature text,
        tags list<frozen<tuple<text,text>>>,
        target text,
        PRIMARY KEY (tx_id)
      )`,

      `CREATE TABLE IF NOT EXISTS tx_offset (
         tx_id text,
         size bigint,
         offset bigint,
         PRIMARY KEY(tx_id)
       )`,
      // `CREATE TABLE IF NOT EXISTS manifest (
      //    manifest_url text,
      //    manifest_id text,
      //    tx_id text,
      //    path text,
      //    PRIMARY KEY(manifest_id, tx_id)
      //  )
      //  WITH CLUSTERING ORDER BY (tx_id DESC)`,
    ];
    let p = Promise.resolve();
    // Create the schema executing the queries serially
    queries.forEach((query) => (p = p.then(() => client.execute(query))));
    return p;
  })
  .then(() => {
    console.log('[cassandra] init done');
    process.exit(0);
  })
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
