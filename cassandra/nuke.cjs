// DELETES EVERYTHING
// IN CASSANDRA, BE CAREFUL!
'use strict';
const cassandra = require('cassandra-driver');
process.env.NODE_ENV !== 'test' && require('dotenv').config();

const KEYSPACE = process.env['KEYSPACE'] ? process.env['KEYSPACE'] : 'gateway';

let contactPoints = ['localhost:9042'];
try {
  contactPoints = process.env.CASSANDRA_CONTACT_POINTS
  ? JSON.parse(process.env.CASSANDRA_CONTACT_POINTS)
  : ['localhost:9042'];
} catch (e) {
  console.error('[nuke] Invalid array of contact points.');
}

const client = new cassandra.Client({
  contactPoints,
  localDataCenter: 'datacenter1',
  credentials: { username: process.env.CASSANDRA_USERNAME, password: process.env.CASSANDRA_PASSWORD },
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
      `DROP TABLE IF EXISTS block_gql_asc`,
      `DROP TABLE IF EXISTS block_gql_desc`,

      `DROP INDEX IF EXISTS tx_id_gql_asc_owner_idx`,
      `DROP INDEX IF EXISTS tx_id_gql_desc_owner_idx`,
      `DROP INDEX IF EXISTS tx_id_gql_asc_target_idx`,
      `DROP INDEX IF EXISTS tx_id_gql_desc_target_idx`,
      `DROP INDEX IF EXISTS tx_id_gql_asc_bundle_id_idx`,
      `DROP INDEX IF EXISTS tx_id_gql_desc_bundle_id_idx`,
      `DROP INDEX IF EXISTS tx_tag_gql_by_name_asc_tag_name`,
      `DROP INDEX IF EXISTS tx_tag_gql_by_name_desc_tag_name`,

      `DROP TABLE IF EXISTS tx_id_gql_asc`,
      `DROP TABLE IF EXISTS tx_id_gql_desc`,
      `DROP TABLE IF EXISTS tx_tag_gql_by_name_asc`,
      `DROP TABLE IF EXISTS tx_tag_gql_by_name_desc`,

      `DROP TABLE IF EXISTS tx_tag`,
      `DROP TABLE IF EXISTS tx_offset`,
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
