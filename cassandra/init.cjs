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
      `CREATE TYPE IF NOT EXISTS poa (
         option text,
         tx_path text,
         data_path text,
         chunk text
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
         poa frozen<tuple<text,text,text,text>>,
         previous_block text,
         reward_addr text,
         reward_pool bigint,
         tags set<frozen<tuple<text, text>>>,
         timestamp bigint,
         tx_root text,
         tx_tree list<text>,
         txs list<text>,
         wallet_list text,
         weave_size bigint,
         PRIMARY KEY (height)
       )`,
      `CREATE TABLE IF NOT EXISTS transaction (
        data text,
        data_root text,
        data_tree list<text>,
        format bigint,
        id text,
        last_tx text,
        owner text,
        quantity text,
        reward text,
        signature text,
        tags set<frozen<tuple<text, text>>>,
        PRIMARY KEY (id)
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
