// migration3
// converts wallet key to wallet address
// in columns used for filtering

// ownerToAddress;
const R = require("rambda");
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
  TxGql: ["tx_index", "tags", "tx_id", "owner", "target", "bundle_id"],
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
      TagAndOwnerDel1: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_owner_asc_migration_1"],
      },
      TagAndOwnerDel2: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_owner_desc_migration_1"],
      },
      TagAndTxIdAndOwner: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerDel1: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_tx_id_and_owner_asc_migration_1"],
      },
      TagAndTxIdAndOwnerDel2: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_tx_id_and_owner_desc_migration_1"],
      },
      TagAndOwnerAndTarget: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_asc_migration_1",
          "tx_tag_gql_by_owner_and_target_desc_migration_1",
        ],
      },
      TagAndOwnerAndTargetDel1: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_owner_and_target_asc_migration_1"],
      },
      TagAndOwnerAndTargetDel2: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_owner_and_target_desc_migration_1"],
      },
      TagAndOwnerAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_bundled_in_asc_migration_1",
          "tx_tag_gql_by_owner_and_bundled_in_desc_migration_1",
        ],
      },
      TagAndOwnerAndBundledInDel1: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_owner_and_bundled_in_asc_migration_1"],
      },
      TagAndOwnerAndBundledInDel2: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_owner_and_bundled_in_desc_migration_1"],
      },
      TagAndOwnerAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_data_root_asc_migration_1",
          "tx_tag_gql_by_owner_and_data_root_desc_migration_1",
        ],
      },
      TagAndOwnerAndDataRootDel1: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_owner_and_data_root_asc_migration_1"],
      },
      TagAndOwnerAndDataRootDel2: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_owner_and_data_root_desc_migration_1"],
      },
      TagAndTxIdAndOwnerAndTarget: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_and_target_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndTargetDel1: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_tx_id_and_owner_and_target_asc_migration_1"],
      },
      TagAndTxIdAndOwnerAndTargetDel2: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_tx_id_and_owner_and_target_desc_migration_1"],
      },
      TagAndTxIdAndOwnerAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc_migration_1",
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_desc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndBundledInDel1: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndBundledInDel2: {
        keyspace: KEYSPACE,
        tables: [
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
      TagAndTxIdAndOwnerAndDataRootDel1: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_tx_id_and_owner_and_data_root_asc_migration_1"],
      },
      TagAndTxIdAndOwnerAndDataRootDel2: {
        keyspace: KEYSPACE,
        tables: [
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
      TagAndOwnerAndTargetAndBundledInDel1: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_and_bundled_in_asc_migration_1",
        ],
      },
      TagAndOwnerAndTargetAndBundledInDel2: {
        keyspace: KEYSPACE,
        tables: [
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
      TagAndOwnerAndTargetAndDataRootDel1: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_and_data_root_asc_migration_1",
        ],
      },
      TagAndOwnerAndTargetAndDataRootDel2: {
        keyspace: KEYSPACE,
        tables: [
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
      TagAndOwnerAndBundledInAndDataRootDel1: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_bundled_in_and_data_root_asc_migration_1",
        ],
      },
      TagAndOwnerAndBundledInAndDataRootDel2: {
        keyspace: KEYSPACE,
        tables: [
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
      TagAndTxIdAndOwnerAndTargetAndBundledInDel1: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_asc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndTargetAndBundledInDel2: {
        keyspace: KEYSPACE,
        tables: [
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
      TagAndTxIdAndOwnerAndTargetAndDataRootDel1: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_asc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndTargetAndDataRootDel2: {
        keyspace: KEYSPACE,
        tables: [
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
      TagAndTxIdAndOwnerAndBundledInAndDataRootDel1: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_asc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndBundledInAndDataRootDel2: {
        keyspace: KEYSPACE,
        tables: [
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
      TagAndOwnerAndTargetAndBundledInAndDataRootDel1: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_asc_migration_1",
        ],
      },
      TagAndOwnerAndTargetAndBundledInAndDataRootDel2: {
        keyspace: KEYSPACE,
        tables: [
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
      TagAndTxIdAndOwnerAndTargetAndBundledInAndDataRootDel1: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_asc_migration_1",
        ],
      },
      TagAndTxIdAndOwnerAndTargetAndBundledInAndDataRootDel2: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_desc_migration_1",
        ],
      },
      TxGql: {
        keyspace: KEYSPACE,
        tables: [],
      },
      TxGqlDesc: {
        keyspace: KEYSPACE,
        tables: ["tx_id_gql_desc_migration_1"],
      },
      TxGqlAsc: {
        keyspace: KEYSPACE,
        tables: ["tx_id_gql_asc_migration_1"],
      },
    },
  });

let warned = false;
let tableCnt = 0;
let concurrent = 0;

const GQL_TX_ID_PARTITION_SIZE = 100_000; // Low partition for greater distribution
const GQL_TX_ID_BUCKET_SIZE = 50_000; // Very hard to guestimate 100MB

const getGqlTxIdAscPartitionName = (height) =>
  `gql_partition_tx_id_asc_${height
    .divide(GQL_TX_ID_PARTITION_SIZE)
    .toString()}`;

const getGqlTxIdAscBucketName = (height) =>
  `gql_bucket_tx_id_asc_${height.divide(GQL_TX_ID_BUCKET_SIZE).toString()}`;

const getGqlTxIdAscBucketNumber = (height) =>
  height.divide(GQL_TX_ID_BUCKET_SIZE).toInt();

const getGqlTxIdDescPartitionName = (height) =>
  `gql_partition_tx_id_desc_${height
    .divide(GQL_TX_ID_PARTITION_SIZE)
    .toString()}`;

const getGqlTxIdDescBucketName = (height) =>
  `gql_bucket_tx_id_desc_${height.divide(GQL_TX_ID_BUCKET_SIZE).toString()}`;

const getGqlTxIdDescBucketNumber = (height) =>
  height.divide(GQL_TX_ID_BUCKET_SIZE).toInt();

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
  const KEYSPACE = process.env["KEYSPACE"]
    ? process.env["KEYSPACE"]
    : "gateway";
  let warned = false;
  const tagsMapper = makeTagsMapper(client);

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
  let migrationProgress = 0;
  for await (const tx of result) {
    console.log("progress:", migrationProgress);
    migrationProgress += 1;
    if (tx.tags && !R.isEmpty(tx.tags)) {
      const ownerAddress = ownerToAddress(tx.owner);
      if (typeof tx.target !== "string") {
        tx.target = "";
      }
      if (typeof tx.bundled_in !== "string") {
        tx.bundled_in = "";
      }
      // possible artifacts of the 2.0 migration
      // nodes always ignore data_root on v1 txs
      // because for them it must be equal to the one they construct from data
      if (typeof tx.data_root !== "string") {
        tx.data_root = "";
      }

      for (const tableName of Object.keys(badTables)) {
        const tagModelName = badTables[tableName];
        const allFields = R.concat(commonFields, tagModels[tagModelName]);
        const obj = R.pickAll(allFields, tx);
        obj["owner"] = ownerAddress;

        // until ans104 comes
        if (tagModelName != "TxGql" && !obj["data_item_index"]) {
          obj["data_item_index"] = types.Long.fromNumber(0);
        }

        if (tagModelName === "TxGql") {
          await pWaitFor(() => concurrent < 200);
          concurrent += 2;
          const objAsc = Object.assign({}, obj);
          const objDesc = Object.assign({}, obj);
          objAsc["partition_id"] = getGqlTxIdAscPartitionName(tx.block_height);
          objAsc["bucket_id"] = getGqlTxIdAscBucketName(tx.block_height);
          objAsc["bucket_number"] = getGqlTxIdDescBucketNumber(tx.block_height);
          objDesc["partition_id"] = getGqlTxIdDescPartitionName(
            tx.block_height
          );
          objDesc["bucket_id"] = getGqlTxIdDescBucketName(tx.block_height);
          objDesc["bucket_number"] = getGqlTxIdDescBucketNumber(
            tx.block_height
          );
          const mapprAsc = tagsMapper.forModel("TxGqlAsc");
          mapprAsc.update(objAsc).then(() => {
            concurrent -= 1;
          });
          const mapprDesc = tagsMapper.forModel("TxGqlDesc");
          mapprDesc.update(objDesc).then(() => {
            concurrent -= 1;
          });
        } else {
          let index = 0;
          for (const tuple of tx.tags) {
            await pWaitFor(() => concurrent < 200);
            const [tag_name, tag_value] = tuple.values();
            concurrent += 1;
            const insertObject = R.merge(obj, {
              tag_pair: `${tag_name}-${tag_value}`,
              tag_index: index,
            });
            const mappr = tagsMapper.forModel(tagModelName);
            mappr.insert(insertObject).then(() => {
              concurrent -= 1;
            });
            // const mapprDel1 = tagsMapper.forModel(tagModelName + "Del1");
            // const mapprDel2 = tagsMapper.forModel(tagModelName + "Del2");
            // Promise.all([
            //   mapprDel1.remove(R.assoc("owner", tx.owner, insertObject), {
            //     ifExists: true,
            //   }),
            //   mapprDel2.remove(R.assoc("owner", tx.owner, insertObject), {
            //     ifExists: true,
            //   }),
            // ]).then(() => {});

            index += 1;
          }
        }
      }
    }
  }
};
