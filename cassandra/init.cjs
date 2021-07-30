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
         txs_synced boolean,
         wallet_list text,
         weave_size bigint,
         PRIMARY KEY (indep_hash)
       )`,
      // make sorting possible with gql
      `CREATE TABLE IF NOT EXISTS block_gql (
          height bigint,
          indep_hash text,
          timestamp bigint,
          PRIMARY KEY (indep_hash, height)
        )
        WITH CLUSTERING ORDER BY (height DESC)`,
      // optimize for search
      // tag id is tx_id + tag_index
      `CREATE TABLE IF NOT EXISTS tx_tag (
         tag_index int,
         next_tag_index int,
         tx_id text,
         name text,
         value text,
         PRIMARY KEY (tag_index, tx_id)
      )
      WITH CLUSTERING ORDER BY (tx_id DESC)`,
      `CREATE INDEX IF NOT EXISTS ON tx_tag (name)`,
      `CREATE INDEX IF NOT EXISTS ON tx_tag (value)`,
      `CREATE TABLE IF NOT EXISTS block_by_tx_id (
         tx_id text,
         block_height bigint,
         block_hash text,
         PRIMARY KEY (tx_id)
       )`,
      `CREATE TABLE IF NOT EXISTS transaction (
        block_height bigint,
        block_hash text,
        block_timestamp bigint,
        data text,
        data_root text,
        data_size bigint,
        data_tree frozen<list<text>>,
        format int,
        id text,
        last_tx text,
        owner text,
        quantity bigint,
        reward bigint,
        signature text,
        tag_count int,
        target text,
        tx_uuid timeuuid,
        PRIMARY KEY ((id), block_height)
      )
      WITH CLUSTERING ORDER BY (block_height DESC)`,
      `CREATE TABLE IF NOT EXISTS tx_offset (
         tx_id text,
         size bigint,
         offset bigint,
         PRIMARY KEY(tx_id)
       )`,
      `CREATE TABLE IF NOT EXISTS manifest (
         manifest_url text,
         manifest_id text,
         tx_id text,
         path text,
         PRIMARY KEY(manifest_id, tx_id)
       )
       WITH CLUSTERING ORDER BY (tx_id DESC)`,
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
