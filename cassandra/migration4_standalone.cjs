// for a good while tx_offset was being passed incorrectly
// this will have affected vartex databases in beta
const R = require("rambda");
const path = require("path");
const fs = require("fs");
const got = require("got");
const cassandra = require("cassandra-driver");

let nodes = [];

try {
  nodes = process.env.ARWEAVE_NODES
    ? JSON.parse(process.env.ARWEAVE_NODES)
    : ["http://gateway-7.arweave.net:1984"];
} catch {
  console.error("[node] invalid list of nodes.");
}

let concurrent = 0;

async function getTxOffset({ txId, retry = 0 }) {
  const tryNode = nodes[retry % nodes.length];

  let jsonPayload;
  try {
    jsonPayload = await got.get(`${tryNode}/tx/${txId}/offset`, {
      responseType: "json",
      resolveBodyOnly: true,
    });
  } catch (error) {
    if (retry > 100) {
      throw new Error(
        "getTransaction: Failed to establish connection to any specified node after 100 retries\n"
      );
    }

    await new Promise(function (resolve) {
      setTimeout(resolve, 10);
    });

    return await getTxOffset({ txId, retry: retry + 1 });
  }

  return jsonPayload;
}

(async () => {
  const pWaitFor = (await import("p-wait-for")).default;
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

  const KEYSPACE = process.env["KEYSPACE"]
    ? process.env["KEYSPACE"]
    : "gateway";

  let contactPoints = ["localhost:9042"];
  try {
    contactPoints = process.env.CASSANDRA_CONTACT_POINTS
      ? JSON.parse(process.env.CASSANDRA_CONTACT_POINTS)
      : ["localhost:9042"];
  } catch (e) {
    console.error("[init] Invalid array of contact points.");
  }

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

  await client.connect();

  const mapper = new cassandra.mapping.Mapper(client, {
    models: {
      TxOffset: {
        keyspace: KEYSPACE,
        tables: ["tx_offset"],
      },
    },
  });

  const mappr = mapper.forModel("TxOffset");

  const result = await client.execute(
    `SELECT * FROM ${KEYSPACE}.transaction`,
    [],
    { prepare: true }
  );

  let migrationProgress = 0;
  for await (const tx of result) {
    console.log("progress:", migrationProgress);
    if (tx.data_size && tx.data_size.gt(0)) {
      await pWaitFor(() => concurrent < 200);
      migrationProgress += 1;
      let offset;
      let size;
      try {
         const res_  = await getTxOffset({ txId: tx.tx_id });
        offset = cassandra.types.Long.fromString(res_.offset);
        size = cassandra.types.Long.fromString(res_.size);
      } catch (error) {
        console.error(error);
        process.exit(1);
      }
process.exit(1);
      if (offset) {
        mappr
          .insert({ tx_id: tx.tx_id, size, offset })
          .then(() => {
            concurrent -= 1;
          });
      } else {
        concurrent -= 1;
      }
    }
  }
})();
