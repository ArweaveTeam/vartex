// DELETES EVERYTHING
// IN CASSANDRA, BE CAREFUL!
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
      `DROP TABLE IF EXISTS poa`,
      `DROP TABLE IF EXISTS block_by_tx_id`,
      `DROP TABLE IF EXISTS block_height_by_block_hash`,
      `DROP TABLE IF EXISTS block`,
      `DROP TABLE IF EXISTS block_gql`,
      `DROP TABLE IF EXISTS tx_offset`,
      `DROP TABLE IF EXISTS tx_tag`,
      `DROP TABLE IF EXISTS transaction`,
      // not used anymore
      `DROP TABLE IF EXISTS block_gql_asc`,
      `DROP TABLE IF EXISTS block_gql_desc`,
      `DROP TABLE IF EXISTS block_status`,
      `DROP TABLE IF EXISTS sync_status`,
      `DROP TABLE IF EXISTS poll_status`,
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
