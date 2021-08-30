/* eslint-disable unicorn/prefer-spread */
import * as cassandra from "cassandra-driver";
import * as R from "rambda";
import { BlockType } from "../query/block.query";
import { types as CassandraTypes } from "cassandra-driver";
import { Poa, TxOffset } from "../types/cassandra.types";
import { KEYSPACE } from "../constants";
import { config } from "dotenv";
import {
  getGqlBlockHeightAscPartitionName,
  getGqlBlockHeightAscBucketName,
  getGqlBlockHeightDescPartitionName,
  getGqlBlockHeightDescBucketName,
  getGqlTxIdAscPartitionName,
  getGqlTxIdAscBucketName,
  getGqlTxIdDescPartitionName,
  getGqlTxIdDescBucketName,
  getGqlTxTagAscPartitionName,
  getGqlTxTagAscBucketName,
  getGqlTxTagDescPartitionName,
  getGqlTxTagDescBucketName,
  getTxTagPartitionName,
  getTxTagBucketName,
} from "./constants.database";

process.env.NODE_ENV !== "test" && config();

const isNumeric = (s: string): boolean => !Number.isNaN(s);

export const toLong = (
  anyValue: CassandraTypes.Long | number | string | undefined
): CassandraTypes.Long =>
  CassandraTypes.Long.isLong(anyValue)
    ? anyValue
    : !anyValue && typeof anyValue !== "string"
    ? CassandraTypes.Long.fromNumber(0)
    : typeof anyValue === "string"
    ? CassandraTypes.Long.fromString(R.isEmpty(anyValue) ? "0" : anyValue)
    : CassandraTypes.Long.fromNumber(anyValue);

let contactPoints = ["localhost:9042"];
try {
  contactPoints = process.env.CASSANDRA_CONTACT_POINTS
    ? JSON.parse(process.env.CASSANDRA_CONTACT_POINTS)
    : ["localhost:9042"];
} catch {
  console.error("[cassandra] Invalid array of contact points.");
}

export const cassandraClient = new cassandra.Client({
  contactPoints,
  localDataCenter: "datacenter1",
  credentials: {
    username: process.env.CASSANDRA_USERNAME,
    password: process.env.CASSANDRA_PASSWORD,
  },
  queryOptions: { isIdempotent: true },
  // encoding: {
  //   map: Map,
  //   set: Set,
  // },
  socketOptions: {
    connectTimeout: 50000,
    defunctReadTimeoutThreshold: 64,
    keepAlive: true,
    keepAliveDelay: 0,
    readTimeout: 30_000,
    tcpNoDelay: true,
    coalescingThreshold: 65_536,
  },
  protocolOptions: {
    maxSchemaAgreementWaitSeconds: process.env["DB_TIMEOUT"]
      ? Number.parseInt(process.env["DB_TIMEOUT"])
      : 30,
  },
  profiles: [
    new cassandra.ExecutionProfile("fast", {
      readTimeout: 5000,
      consistency: cassandra.types.consistencies.any,
      serialConsistency: cassandra.types.consistencies.any,
    }),
    new cassandra.ExecutionProfile("gql", {
      readTimeout: 5000,
      consistency: cassandra.types.consistencies.all,
      serialConsistency: cassandra.types.consistencies.serial,
    }),
    new cassandra.ExecutionProfile("full", {
      readTimeout: 15_000,
      consistency: cassandra.types.consistencies.all,
      serialConsistency: cassandra.types.consistencies.serial,
      graphOptions: {
        writeConsistency: cassandra.types.consistencies.all,
      },
    }),
  ],
});

const poaKeys = [
  "option",
  "tx_path",
  "data_path",
  "chunk",
  "block_hash",
  "block_height",
];

const txTagKeys = [
  "partition_id",
  "bucket_id",
  "tx_id",
  "tx_index",
  "tag_index",
  "next_tag_index",
  "name",
  "value",
];

const txOffsetKeys = ["tx_id", "size", "offset"];

const transactionKeys = [
  "tx_index",
  "block_height",
  "block_hash",
  "data_root",
  "data_size",
  "data_tree",
  "format",
  "tx_id",
  "last_tx",
  "owner",
  "quantity",
  "reward",
  "signature",
  "target",
  "tags",
  "tag_count",
];

const blockKeys = [
  "block_size",
  "cumulative_diff",
  "diff",
  "hash",
  "hash_list_merkle",
  "height",
  "indep_hash",
  "last_retarget",
  "nonce",
  "previous_block",
  "reward_addr",
  "reward_pool",
  "tags",
  "timestamp",
  "tx_root",
  "tx_tree",
  "txs",
  "txs_count",
  "wallet_list",
  "weave_size",
];

const transformPoaKeys = (object: unknown): Poa => {
  const poa = object["poa"] ? object["poa"] : {};
  const poaObject = {} as Poa;
  poaObject["option"] = poa["option"] || "";
  poaObject["tx_path"] = poa["tx_path"] || "";
  poaObject["data_path"] = poa["data_path"] || "";
  poaObject["chunk"] = poa["chunk"] || "";
  poaObject["block_hash"] = object["indep_hash"] || "";
  poaObject["block_height"] = toLong(object["height"]);
  return poaObject;
};

// note for optimization reasons
// we may store the data differently than we serve it (eg. bigint->string)
/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
const transformBlockKey = (key: string, object: any) => {
  switch (key) {
    case "txs_count": {
      return object.txs ? object.txs.length : 0;
    }

    case "txs":
    case "tx_tree": {
      const txs = object[key] && Array.isArray(object[key]) ? object[key] : [];
      return txs;
    }
    case "tags": {
      return (
        !R.isEmpty(object.tags) &&
        (object.tags || []).map(({ name, value }) =>
          CassandraTypes.Tuple.fromArray([name, value])
        )
      );
    }

    case "block_size":
    case "diff":
    case "height":
    case "last_retarget":
    case "reward_pool":
    case "timestamp":
    case "weave_size": {
      return toLong(object[key]);
    }
    case "cumulative_diff":
    case "hash":
    case "hash_list_merkle":
    case "indep_hash":
    case "nonce":
    case "previous_block":
    case "reward_addr":
    case "tx_root":
    case "wallet_list": {
      if (object[key] || isNumeric(object[key])) {
        return typeof object[key] === "string"
          ? object[key]
          : object[key].toString();
      } else {
        return "";
      }
    }

    default: {
      console.error("Unknown key", key);
    }
  }
};

const transformTxKey = (
  key: string,
  txIndex: CassandraTypes.Long,
  txData: any, //  eslint-disable-line @typescript-eslint/no-explicit-any
  blockData: any //  eslint-disable-line @typescript-eslint/no-explicit-any
) => {
  switch (key) {
    case "tx_index": {
      return txIndex;
    }
    case "block_timestamp": {
      return toLong(blockData["timestamp"]);
    }
    case "block_height": {
      return toLong(blockData["height"]);
    }

    case "block_hash": {
      return blockData["indep_hash"];
    }

    case "data_tree": {
      const txs = txData[key] && Array.isArray(txData[key]) ? txData[key] : [];
      return txs;
    }
    case "tags": {
      return (txData.tags || []).map(({ name, value }) =>
        CassandraTypes.Tuple.fromArray([name, value])
      );
    }
    case "tag_count": {
      return txData.tags ? txData.tags.length : 0;
    }

    case "tx_id": {
      return txData.id;
    }
    case "data_root":
    case "last_tx":
    case "owner":
    case "signature":
    case "target": {
      if (txData[key]) {
        return typeof txData[key] === "string"
          ? txData[key]
          : txData[key].toString();
      } else {
        return "";
      }
    }

    case "data_size":
    case "quantity":
    case "reward": {
      return toLong(txData[key]);
    }

    case "format": {
      return txData[key];
    }

    default: {
      console.error("Unknown key", key);
    }
  }
};

const transformTxOffsetKeys = (txObject: unknown): TxOffset => {
  const txOffset = txObject["tx_offset"] ? txObject["tx_offset"] : {};
  const txOffsetObject = {} as TxOffset;
  txOffsetObject["tx_id"] = txObject["id"] || "";
  txOffsetObject["size"] = toLong(txOffset["size"] || 0);
  txOffsetObject["offset"] = toLong(txOffset["offset"] || -1);
  return txOffsetObject;
};

interface Tag {
  partition_id: string;
  bucket_id: string;
  tag_index: number;
  tx_index: CassandraTypes.Long;
  tx_id: string;
  name: string;
  value: string;
}

type UpstreamTag = { name: string; value: string };

const transformTag = (
  tag: UpstreamTag,
  txObject: any, // eslint-disable-line @typescript-eslint/no-explicit-any
  blockHeight: CassandraTypes.Long,
  txIndex: CassandraTypes.Long,
  index: number,
  nextIndex?: number
): Tag => {
  const tagObject = {} as Tag;
  tagObject["partition_id"] = getTxTagPartitionName(blockHeight);
  tagObject["bucket_id"] = getTxTagBucketName(blockHeight);
  tagObject["tag_index"] = index;
  tagObject["next_tag_index"] = nextIndex || undefined;
  tagObject["tx_index"] = txIndex;
  tagObject["tx_id"] = txObject["id"];
  tagObject["name"] = tag.name || "";
  tagObject["value"] = tag.value || "";
  return tagObject;
};

const poaInsertQuery = `INSERT INTO ${KEYSPACE}.poa (${poaKeys.join(
  ", "
)}) VALUES (${poaKeys.map(() => "?").join(", ")})`;

const blockInsertQuery = (nonNilBlockKeys: string[]) =>
  `INSERT INTO ${KEYSPACE}.block (${nonNilBlockKeys.join(
    ", "
  )}) VALUES (${nonNilBlockKeys.map(() => "?").join(", ")})`;

const transactionInsertQuery = (nonNilTxKeys: string[]) =>
  `INSERT INTO ${KEYSPACE}.transaction (${nonNilTxKeys.join(
    ", "
  )}) VALUES (${nonNilTxKeys.map(() => "?").join(", ")})`;

const txOffsetInsertQuery = `INSERT INTO ${KEYSPACE}.tx_offset (${txOffsetKeys.join(
  ", "
)}) VALUES (${txOffsetKeys.map(() => "?").join(", ")})`;

const txTagsInsertQuery = `INSERT INTO ${KEYSPACE}.tx_tag (${txTagKeys.join(
  ", "
)}) VALUES (${txTagKeys.map(() => "?").join(", ")})`;

const blockHeightByHashInsertQuery = `INSERT INTO ${KEYSPACE}.block_height_by_block_hash (block_height, block_hash) VALUES (?, ?) IF NOT EXISTS`;

// const blockByTxIdInsertQuery = `INSERT INTO ${KEYSPACE}.block_by_tx_id (tx_id, block_height, block_hash) VALUES (?, ?, ?) IF NOT EXISTS`;

const blockGqlInsertAscQuery = `INSERT INTO ${KEYSPACE}.block_gql_asc (partition_id, bucket_id, height, indep_hash, timestamp, previous) VALUES (?, ?, ?, ?, ?, ?)`;

const blockGqlInsertDescQuery = `INSERT INTO ${KEYSPACE}.block_gql_desc (partition_id, bucket_id, height, indep_hash, timestamp, previous) VALUES (?, ?, ?, ?, ?, ?)`;

const txIdGqlInsertAscQuery = `INSERT INTO ${KEYSPACE}.tx_id_gql_asc
                               (partition_id, bucket_id, tx_index, tags, tx_id, owner, target, bundle_id)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;

const txIdGqlInsertDescQuery = `INSERT INTO ${KEYSPACE}.tx_id_gql_desc
                                (partition_id, bucket_id, tx_index, tags, tx_id, owner, target, bundle_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;

const txTagGqlInsertAscQuery = `INSERT INTO ${KEYSPACE}.tx_tag_gql_by_name_asc
                                (partition_id, bucket_id, tx_index, tag_index, tag_value, tag_name, tx_id, owner, target, bundle_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

const txTagGqlInsertDescQuery = `INSERT INTO ${KEYSPACE}.tx_tag_gql_by_name_desc
                                 (partition_id, bucket_id, tx_index, tag_index, tag_value, tag_name, tx_id, owner, target, bundle_id)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

export const makeTxImportQuery = (
  height: CassandraTypes.Long,
  txIndex: CassandraTypes.Long,
  tx: { [k: string]: any }, // eslint-disable-line @typescript-eslint/no-explicit-any
  blockData: { [k: string]: any } // eslint-disable-line @typescript-eslint/no-explicit-any
  /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
) => (): Promise<any> => {
  let dataSize: CassandraTypes.Long | undefined;
  const nonNilTxKeys: string[] = [];
  /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
  const txInsertParameters: { [k: string]: any } = transactionKeys.reduce(
    (paramz: Array<any>, key: string) => {
      const nextValue = transformTxKey(key, txIndex, tx, blockData);

      if (key === "data_size") {
        dataSize = nextValue;
      }
      if (nextValue && !R.isEmpty(nextValue)) {
        paramz.push(nextValue);
        nonNilTxKeys.push(key);
      }

      return paramz;
    },
    []
  );

  return Promise.all(
    [
      cassandraClient.execute(
        transactionInsertQuery(nonNilTxKeys),
        txInsertParameters,
        { prepare: true, executionProfile: "full" }
      ),

      cassandraClient.execute(
        txIdGqlInsertAscQuery,
        [
          getGqlTxIdAscPartitionName(height),
          getGqlTxIdAscBucketName(height),
          txIndex,
          (tx.tags || []).map(({ name, value }) =>
            CassandraTypes.Tuple.fromArray([name, value])
          ),
          tx.id,
          tx.owner,
          tx.target,
          "", // FIXME ANS-102/ANS-104
        ],
        { prepare: true, executionProfile: "full" }
      ),
      cassandraClient.execute(
        txIdGqlInsertDescQuery,
        [
          getGqlTxIdDescPartitionName(height),
          getGqlTxIdDescBucketName(height),
          txIndex,
          (tx.tags || []).map(({ name, value }) =>
            CassandraTypes.Tuple.fromArray([name, value])
          ),
          tx.id,
          tx.owner,
          tx.target,
          "", // FIXME ANS-102/ANS-104
        ],
        { prepare: true, executionProfile: "full" }
      ),
    ]
      .concat(
        (tx.tags || []).map((tag: UpstreamTag, index: number) =>
          cassandraClient.execute(
            txTagGqlInsertAscQuery,
            [
              getGqlTxTagAscPartitionName(height),
              getGqlTxTagAscBucketName(height),
              txIndex,
              index,
              tag.value || "",
              tag.name || "",
              tx.id,
              tx.owner,
              tx.target,
              "", // FIXME ANS-102/ANS-104
            ],
            { prepare: true, executionProfile: "full" }
          )
        )
      )
      .concat(
        (tx.tags || []).map((tag: UpstreamTag, index: number) =>
          cassandraClient.execute(
            txTagGqlInsertDescQuery,
            [
              getGqlTxTagDescPartitionName(height),
              getGqlTxTagDescBucketName(height),
              txIndex,
              index,
              tag.value || "",
              tag.name || "",
              tx.id,
              tx.owner,
              tx.target,
              "", // FIXME ANS-102/ANS-104
            ],
            { prepare: true, executionProfile: "full" }
          )
        )
      )
      .concat(
        (tx.tags || []).map((tag: UpstreamTag, index: number) =>
          cassandraClient.execute(
            txTagsInsertQuery,
            transformTag(
              tag,
              tx,
              height,
              txIndex,
              index,
              index + 1 < tx.tags.length ? index + 1 : undefined
            ),
            {
              prepare: true,
              executionProfile: "full",
            }
          )
        )
      )
      .concat(
        dataSize && dataSize.gt(0)
          ? [
              cassandraClient.execute(
                txOffsetInsertQuery,
                transformTxOffsetKeys(tx),
                {
                  prepare: true,
                  executionProfile: "full",
                }
              ),
            ]
          : []
      )
  );
};

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
export const makeBlockImportQuery = (input: BlockType) => (): Promise<
  CassandraTypes.ResultSet[]
> => {
  const nonNilBlockKeys: string[] = [];
  const blockInsertParameters = blockKeys.reduce(
    (paramz: Array<any>, key: string) => {
      const nextValue = transformBlockKey(key, input);
      if (nextValue && !R.isEmpty(nextValue)) {
        paramz.push(nextValue);
        nonNilBlockKeys.push(key);
      }

      return paramz;
    },
    []
  );
  const height = toLong(input.height);

  return Promise.all([
    cassandraClient.execute(poaInsertQuery, transformPoaKeys(input), {
      prepare: true,
      executionProfile: "full",
    }),
    cassandraClient.execute(
      blockGqlInsertAscQuery,
      [
        getGqlBlockHeightAscPartitionName(height),
        getGqlBlockHeightAscBucketName(height),
        height,
        input.indep_hash,
        input.timestamp,
        input.previous_block,
      ],
      { prepare: true, executionProfile: "full" }
    ),
    cassandraClient.execute(
      blockGqlInsertDescQuery,
      [
        getGqlBlockHeightDescPartitionName(height),
        getGqlBlockHeightDescBucketName(height),
        height,
        input.indep_hash,
        input.timestamp,
        input.previous_block,
      ],
      { prepare: true, executionProfile: "full" }
    ),
    cassandraClient.execute(
      blockHeightByHashInsertQuery,
      [height, input.indep_hash],
      { prepare: true, executionProfile: "full" }
    ),
    cassandraClient.execute(
      blockInsertQuery(nonNilBlockKeys),
      blockInsertParameters,
      { prepare: true, executionProfile: "full" }
    ),
  ]);
};

export const getMaxHeightBlock = async (): Promise<
  [string, CassandraTypes.Long]
> => {
  // note that the block_hash table is sorted descendingly by block height
  const response = await cassandraClient.execute(
    `SELECT height,indep_hash FROM ${KEYSPACE}.block_gql_desc limit 1;`
  );

  const row = response.rows[0];
  return row ? [row["indep_hash"], row["height"]] : ["", toLong(-1)];
};
