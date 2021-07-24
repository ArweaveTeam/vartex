// DELETES EVERYTHING
// IN CASSANDRA, BE CAREFUL!
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
      `DROP TABLE IF EXISTS poa`,
      `DROP TABLE IF EXISTS sync_status`,
      `DROP TABLE IF EXISTS poll_status`,
      `DROP TABLE IF EXISTS block_by_tx_id`,
      `DROP TABLE IF EXISTS block_status`,
      `DROP TABLE IF EXISTS block_height_by_block_hash`,
      `DROP TABLE IF EXISTS block`,
      `DROP TABLE IF EXISTS tx_offset`,
      `DROP TABLE IF EXISTS tx_tag`,
      `DROP TABLE IF EXISTS transaction`,
    ];
    let p = Promise.resolve();
    // Create the schema executing the queries serially
    queries.forEach((query) => (p = p.then(() => client.execute(query))));
    return p;
  })
  .then(() => {
    console.log('[cassandra] nuke done');
    process.exit(0);
  })
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
