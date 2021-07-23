'use strict';
const cassandra = require('cassandra-driver');
require('dotenv').config();

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
      `CREATE KEYSPACE IF NOT EXISTS gateway
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }`,
      'USE gateway',
      `CREATE TABLE IF NOT EXISTS poa (
         option text,
         tx_path text,
         data_path text,
         chunk text,
         block_hash text,
         block_height bigint,
         PRIMARY KEY (block_hash)
      )`,
      // because iterating all the rows is expensive
      `CREATE TABLE IF NOT EXISTS sync_status (
         last_block_hash text,
         last_block_height bigint,
         session_uuid timeuuid,
         PRIMARY KEY (session_uuid)
       )`,
      // because iterating all the rows is expensive
      `CREATE TABLE IF NOT EXISTS block_status (
         block_hash text,
         block_height bigint,
         synced boolean,
         PRIMARY KEY (block_hash)
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
         PRIMARY KEY (hash)
       )`,
      // optimize for search
      // tag id is tx_id + tag_index
      `CREATE TABLE IF NOT EXISTS tx_tag (
         tag_id text,
         tag_index int,
         tx_id text,
         name text,
         value text,
         PRIMARY KEY (tag_id)
      )`,
      `CREATE TABLE IF NOT EXISTS transaction (
        data text,
        data_root text,
        data_tree frozen<list<text>>,
        format bigint,
        id text,
        last_tx text,
        owner text,
        quantity text,
        reward text,
        signature text,
        tag_count int,
        PRIMARY KEY (id)
      )`,
      `CREATE TABLE IF NOT EXISTS manifest (
         manifest_url text,
         manifest_id text,
         tx_id text,
         path text,
         PRIMARY KEY(manifest_id, tx_id)
       )`,
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
