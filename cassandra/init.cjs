"use strict";
const fs = require("fs");
const path = require("path");
const net = require("net");
const cassandra = require("cassandra-driver");
const txFilterTables = require("./tx-filter-tables.cjs");
const tagTables = require("./tag-tables.cjs");
const dotenvPath = path.resolve(__dirname, "../.env");
const dotenvPathFallback = path.resolve(__dirname, "../.env.example");

if (fs.existsSync(dotenvPath)) {
  require("dotenv").config({ path: dotenvPath });
} else {
  require("dotenv").config({ path: dotenvPathFallback });
}

/**
 * CASSANDRA INIT
 */
const retries = 5;
let retryCount = 0;

const KEYSPACE = process.env["KEYSPACE"] ? process.env["KEYSPACE"] : "gateway";

let contactPoints = ["localhost:9042"];
try {
  contactPoints = process.env.CASSANDRA_CONTACT_POINTS
    ? JSON.parse(process.env.CASSANDRA_CONTACT_POINTS)
    : ["localhost:9042"];
} catch (e) {
  console.error("[init] Invalid array of contact points.");
}

async function connect() {
  const client = new cassandra.Client({
    contactPoints,
    localDataCenter: "datacenter1",
    socketOptions: {
      connectTimeout: 600000,
      readTimeout: 6000000,
    },
    credentials: {
      username: process.env.CASSANDRA_USERNAME,
      password: process.env.CASSANDRA_PASSWORD,
    },
  });

  client
    .connect()
    .then(function () {
      const priviligedQuery = [
        `CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE}
         WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }`,
      ];
      const queries = [
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
           txs_count int,
           wallet_list text,
           weave_size bigint,
           PRIMARY KEY (indep_hash)
         )`,

        `CREATE TABLE IF NOT EXISTS block_gql_asc (
          partition_id text,
          bucket_id text,
          bucket_number int,
          height bigint,
          indep_hash text,
          previous text,
          timestamp bigint,
          PRIMARY KEY ((partition_id, bucket_id), bucket_number, height)
        )
        WITH CLUSTERING ORDER BY (bucket_number ASC, height ASC)`,

        `CREATE TABLE IF NOT EXISTS block_gql_desc (
          partition_id text,
          bucket_id text,
          bucket_number int,
          height bigint,
          indep_hash text,
          previous text,
          timestamp bigint,
          PRIMARY KEY ((partition_id, bucket_id), bucket_number, height)
        )
        WITH CLUSTERING ORDER BY (bucket_number DESC, height DESC)`,

        `CREATE TABLE IF NOT EXISTS transaction (
          tx_index bigint,
          data_item_index bigint,
          block_height bigint,
          block_hash text,
          bundled_in text,
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
          tag_count int,
          target text,
          PRIMARY KEY (tx_id)
        )`,

        `CREATE TABLE IF NOT EXISTS tx_offset (
           tx_id text,
           size bigint,
           offset bigint,
           PRIMARY KEY(tx_id)
         )`,

        `CREATE TABLE IF NOT EXISTS manifest (
          tx_id text,
          manifest_type text,
          manifest_version text,
          manifest_index text,
          manifest_paths text,
          PRIMARY KEY(tx_id)
        )`,

        `CREATE TABLE IF NOT EXISTS permaweb_path (
          domain_id text,
          uri_path text,
          target_id text,
          content_length text,
          content_type text,
          blacklisted boolean,
          custom_headers list<frozen<tuple<text,text>>>,
          PRIMARY KEY(domain_id, uri_path)
        )`,

        `CREATE TABLE IF NOT EXISTS status (
          session timeuuid,
          status text,
          arweave_height text,
          gateway_height text,
          vartex_git_revision text,
          current_imports list<text>,
          current_migrations map<int, text>,
          PRIMARY KEY(session)
        )`,

        `CREATE TABLE IF NOT EXISTS heartbeat (
          session timeuuid,
          status text,
          role text,
          PRIMARY KEY(session)
        )`,

        `CREATE TABLE IF NOT EXISTS block_queue (
          block_hash text,
          block_height bigint,
          last_import_attempt timestamp,
          import_attempt_cnt int,
          PRIMARY KEY(block_hash, block_height)
         )
         WITH CLUSTERING ORDER BY (block_height ASC)`,
        // manifests rely on data which may not be available
        // at the same time as the tx headers attached to it are.
        `CREATE TABLE IF NOT EXISTS manifest_queue (
          tx_id text,
          first_seen timestamp,
          last_import_attempt timestamp,
          import_attempt_cnt int,
         PRIMARY KEY(tx_id)
         )`,
      ]
        .concat(tagTables.createTableQueries)
        .concat(txFilterTables);
      console.log(queries);
      let p = Promise.resolve();
      let aresolve;
      let a = new Promise((resolve) => {
        aresolve = resolve;
      });
      try {
        for (const q of priviligedQuery) {
          client
            .execute(q)
            .then(() => aresolve())
            .catch(
              () =>
                console.error(
                  "ROLE with limited privilige detected (which is a good practice for prod!)"
                ) || aresolve()
            );
        }
      } catch (e) {
        console.error(
          "ROLE with limited privilige detected (which is a good practice for prod!)"
        );
        aresolve && aresolve();
      }
      // Create the schema executing the queries serially
      return a.then(async () => {
        for (const qq of queries) {
          await client.execute(qq);
        }
        if (!process.env.SKIP_MIGRATION) {
          // skipped when fully migrated
          // await migration1(client);
          // await migration2(client);
          // await migration3(client);
        }
      });
    })
    .then(async () => {
      console.log("[cassandra] init done");
      process.exit(0);
    })
    .catch((error) => {
      console.error(error);

      console.error("ERRCODE: ", error.code);

      if (error.code == "ECONNREFUSED" && ++retryCount < retries) {
        console.log("[cassandra] Retrying connection...");
        setTimeout(connect, 10000);
        return;
      }

      process.exit(1);
    });
}
connect();
