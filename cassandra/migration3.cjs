// migration3
// converts wallet key to wallet address
// in columns used for filtering

// ownerToAddress;
const { createHash } = require("crypto");
const B64js = require("base64-js");
const { mapping, types } = require("cassandra-driver");
const { Mapper } = mapping;
const KEYSPACE = process.env["KEYSPACE"] ? process.env["KEYSPACE"] : "gateway";

function b64UrlDecode(b64UrlString) {
  b64UrlString = b64UrlString.replace(/-/g, "+").replace(/_/g, "/");
  let padding;
  b64UrlString.length % 4 == 0
    ? (padding = 0)
    : (padding = 4 - (b64UrlString.length % 4));
  return [...b64UrlString, ..."=".repeat(padding)].join("");
}

function b64UrlToStringBuffer(b64UrlString) {
  return Buffer.from(B64js.toByteArray(b64UrlDecode(b64UrlString)));
}

function toB64url(buffer) {
  return buffer
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "");
}

function sha256(buffer) {
  return createHash("sha256").update(buffer).digest();
}

function ownerToAddress(owner_) {
  return toB64url(sha256(b64UrlToStringBuffer(owner_)));
}

const tagModels = {
  Tag: [],
  TagAndTxId: ["tx_id"],
  TagAndOwner: ["owner"],
  TagAndTarget: ["target"],
  TagAndBundledIn: ["bundled_in"],
  TagAndDataRoot: ["data_root"],
  TagAndTxIdAndOwner: ["tx_id", "owner"],
  TagAndTxIdAndTarget: ["tx_id", "target"],
  TagAndTxIdAndBundledIn: ["tx_id", "bundled_in"],
  TagAndTxIdAndDataRoot: ["tx_id", "data_root"],
  TagAndOwnerAndTarget: ["owner", "target"],
  TagAndOwnerAndBundledIn: ["owner", "bundled_in"],
  TagAndOwnerAndDataRoot: ["owner", "data_root"],
  TagAndTargetAndBundledIn: ["target", "bundled_in"],
  TagAndTargetAndDataRoot: ["target", "data_root"],
  TagAndBundledInAndDataRoot: ["bundled_in", "data_root"],
  TagAndTxIdAndOwnerAndTarget: ["tx_id", "owner", "target"],
  TagAndTxIdAndOwnerAndBundledIn: ["tx_id", "owner", "bundled_in"],
  TagAndTxIdAndOwnerAndDataRoot: ["tx_id", "owner", "data_root"],
  TagAndTxIdAndTargetAndBundledIn: ["tx_id", "target", "bundled_in"],
  TagAndTxIdAndTargetAndDataRoot: ["tx_id", "target", "data_root"],
  TagAndTxIdAndBundledInAndDataRoot: ["tx_id", "bundled_in", "data_root"],
  TagAndOwnerAndTargetAndBundledIn: ["owner", "target", "bundled_in"],
  TagAndOwnerAndTargetAndDataRoot: ["owner", "target", "data_root"],
  TagAndOwnerAndBundledInAndDataRoot: ["owner", "bundled_in", "data_root"],
  TagAndTargetAndBundledInAndDataRoot: ["target", "bundled_in", "data_root"],
  TagAndTxIdAndOwnerAndTargetAndBundledIn: [
    "tx_id",
    "owner",
    "target",
    "bundled_in",
  ],
  TagAndTxIdAndOwnerAndTargetAndDataRoot: [
    "tx_id",
    "owner",
    "target",
    "data_root",
  ],
  TagAndTxIdAndOwnerAndBundledInAndDataRoot: [
    "tx_id",
    "owner",
    "bundled_in",
    "data_root",
  ],
  TagAndTxIdAndTargetAndBundledInAndDataRoot: [
    "tx_id",
    "target",
    "bundled_in",
    "data_root",
  ],
  TagAndOwnerAndTargetAndBundledInAndDataRoot: [
    "owner",
    "target",
    "bundled_in",
    "data_root",
  ],
  TagAndTxIdAndOwnerAndTargetAndBundledInAndDataRoot: [
    "tx_id",
    "owner",
    "target",
    "bundled_in",
    "data_root",
  ],
};

const badTables = {
  tx_tag_gql_by_owner_asc_migration_1: "TagAndOwner",
  tx_tag_gql_by_tx_id_and_owner_asc_migration_1: "TagAndTxIdAndOwner",
  tx_tag_gql_by_owner_and_target_asc_migration_1: "TagAndOwnerAndTarget",
  tx_tag_gql_by_owner_and_bundled_in_asc_migration_1: "TagAndOwnerAndBundledIn",
  tx_tag_gql_by_owner_and_data_root_asc_migration_1: "TagAndOwnerAndDataRoot",
  tx_tag_gql_by_tx_id_and_owner_and_target_asc_migration_1:
    "TagAndTxIdAndOwnerAndTarget",
  tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc_migration_1:
    "TagAndTxIdAndOwnerAndBundledIn",
  tx_tag_gql_by_tx_id_and_owner_and_data_root_asc_migration_1:
    "TagAndTxIdAndOwnerAndDataRoot",
  tx_tag_gql_by_owner_and_target_and_bundled_in_asc_migration_1:
    "TagAndOwnerAndTargetAndBundledIn",
  tx_tag_gql_by_owner_and_target_and_data_root_asc_migration_1:
    "TagAndOwnerAndTargetAndDataRoot",
  tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_asc_migration_1:
    "TagAndTxIdAndOwnerAndTargetAndBundledIn",
  tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_asc_migration_1:
    "TagAndTxIdAndOwnerAndTargetAndDataRoot",
  tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_asc_migration_1:
    "TagAndTxIdAndOwnerAndBundledInAndDataRoot",
  tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_asc_migration_1:
    "TagAndOwnerAndTargetAndBundledInAndDataRoot",
  tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_asc_migration_1:
    "TagAndTxIdAndOwnerAndTargetAndBundledInAndDataRoot",
  tx_id_gql_asc_migration_1: "TxGql",
};

const makeTagsMapper = (cassandraClient) =>
  new Mapper(cassandraClient, {
    models: {
      TagAndOwner: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_asc_migration_1",
          "tx_tag_gql_by_owner_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwner: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_desc_migration_1",
        ],
      },
      TagAndOwnerAndTarget: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_asc_migration_1",
          "tx_tag_gql_by_owner_and_target_desc_migration_1",
        ],
      },
      TagAndOwnerAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_bundled_in_asc_migration_1",
          "tx_tag_gql_by_owner_and_bundled_in_desc_migration_1",
        ],
      },
      TagAndOwnerAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_data_root_asc_migration_1",
          "tx_tag_gql_by_owner_and_data_root_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndTarget: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_and_target_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_data_root_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_and_data_root_desc_migration_1",
        ],
      },
      TagAndOwnerAndTargetAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_and_bundled_in_asc_migration_1",
          "tx_tag_gql_by_owner_and_target_and_bundled_in_desc_migration_1",
        ],
      },
      TagAndOwnerAndTargetAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_and_data_root_asc_migration_1",
          "tx_tag_gql_by_owner_and_target_and_data_root_desc_migration_1",
        ],
      },
      TagAndOwnerAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_bundled_in_and_data_root_asc_migration_1",
          "tx_tag_gql_by_owner_and_bundled_in_and_data_root_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndTargetAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndTargetAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_desc_migration_1",
        ],
      },
      TagAndOwnerAndTargetAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_asc_migration_1",
          "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndTargetAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_desc_migration_1",
        ],
      },
      TxGql: {
        keyspace: KEYSPACE,
        tables: ["tx_id_gql_desc_migration_1", "tx_id_gql_asc_migration_1"],
      },
    },
  });

let warned = false;
let tableCnt = 0;
let concurrent = 0;

const commonFields = ["tx_index", "data_item_index", "tx_id"];

module.exports = async (client) => {
  const migrationNeededQuery = await client.execute(
    "SELECT owner FROM gateway.tx_id_gql_desc_migration_1 WHERE owner > '' LIMIT 1 ALLOW FILTERING",
    {}
  );
  if (
    migrationNeededQuery.rows === 0 ||
    migrationNeededQuery.rows[0].owner.length === 43
  ) {
    return;
  }
  const pWaitFor = (await import("p-wait-for")).default;
  const allTables = await client.execute("describe tables");
  const KEYSPACE = process.env["KEYSPACE"]
    ? process.env["KEYSPACE"]
    : "gateway";
  let warned = false;
  const tagsMapper = makeTagsMapper(client);

  for (const row of allTables.rows) {
    if (
      row.keyspace_name === KEYSPACE &&
      row.type === "table" &&
      Object.keys(badTables).includes(row.name)
    ) {
      if (!warned) {
        console.error(
          `[migrate3] Unmigrated tables detected, this may take a while...\n`
        );
        warned = true;
      }

      const result = await client.execute(
        `SELECT * FROM ${KEYSPACE}.transaction`,
        [],
        { prepare: true }
      );

      for await (const tx of result) {
        if (tx.tags && !R.isEmpty(tx.tags)) {
          const ownerAddress = ownerToAddress(tx.owner);

          for (const tuple of tx.tags) {
            const [tag_name, tag_value] = tuple.values();
            for (const tableName of Object.keys(badTables)) {
              const tagModelName = badTables[tableName];
              const allFields = R.concat(commonFields, tagModels[tagModelName]);
              const obj = R.pickAll(allFields, tx);
              obj["owner"] = ownerAddress;

              // until ans104 comes
              if (!obj["data_item_index"]) {
                obj["data_item_index"] = types.Long.fromNumber(0);
              }
              await pWaitFor(() => concurrent < 200);
              concurrent += 1;
              const mappr = tagsMapper.forModel(tagModelName);
              mappr.update(obj).then(() => {
                concurrent -= 1;
              });
            }
          }
        }
      }
    }
  }
};
