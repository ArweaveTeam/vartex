import * as R from "rambda";
import { mapping } from "cassandra-driver";
import { cassandraClient } from "./cassandra.database.js";

const { Mapper } = mapping;

// prune the null values away
const withDefault = ({ name, fallback }: { name: string; fallback: any }) => ({
  [name]: {
    name,
    toModel: (v: any) => v || fallback,
    fromModel: (v: any) => v || fallback,
  },
});

const mapper = new Mapper(cassandraClient, {
  models: {
    BlockHeightByBlockHash: {
      keyspace: "gateway",
      tables: ["block_height_by_block_hash"],
    },
    Block: {
      keyspace: "gateway",
      tables: ["block"],
      columns: R.mergeAll([
        withDefault({ name: "cumulative_diff", fallback: "" }),
        withDefault({ name: "hash_list_merkle", fallback: "" }),
        withDefault({ name: "previous_block", fallback: "" }), // only block 0
        withDefault({ name: "tags", fallback: [] }),
        withDefault({ name: "tx_root", fallback: "" }),
        withDefault({ name: "tx_tree", fallback: "" }),
        withDefault({ name: "txs", fallback: [] }),
      ]),
    },
    BlockByTxId: {
      keyspace: "gateway",
      tables: ["block_by_tx_id"],
    },
    Poa: {
      keyspace: "gateway",
      tables: ["poa"],
    },
    PollStatus: {
      keyspace: "gateway",
      tables: ["poll_status"],
    },
    Transaction: {
      keyspace: "gateway",
      tables: ["transaction"],
      columns: R.mergeAll([
        withDefault({ name: "target", fallback: "" }),
        withDefault({ name: "data", fallback: "" }),
        withDefault({ name: "data_root", fallback: "" }),
        withDefault({ name: "data_tree", fallback: "" }),
        withDefault({ name: "format", fallback: 0 }),
        withDefault({ name: "tx_uuid", fallback: "" }),
      ]),
    },
    TxTag: {
      keyspace: "gateway",
      tables: ["tx_tag"],
    },
    TxOffset: {
      keyspace: "gateway",
      tables: ["tx_offset"],
    },
  },
});

export const blockHeightToHashMapper = mapper.forModel(
  "BlockHeightByBlockHash"
);

export const blockMapper = mapper.forModel("Block");

export const poaMapper = mapper.forModel("Poa");

export const pollStatusMapper = mapper.forModel("PollStatus");

export const transactionMapper = mapper.forModel("Transaction");

export const txIdToBlockMapper = mapper.forModel("BlockByTxId");

export const txTagMapper = mapper.forModel("TxTag");

export const txOffsetMapper = mapper.forModel("TxOffset");

export const tagsByTxId = async (txId: string) => {
  let lastTagResponse = await txTagMapper.get({ tx_id: txId, tag_index: 0 });
  const tags = [];

  if (!lastTagResponse) {
    return tags;
  } else {
    tags.push({ name: lastTagResponse.name, value: lastTagResponse.value });
    while (lastTagResponse.next_tag_index) {
      lastTagResponse = await txTagMapper.get({
        tx_id: txId,
        tag_index: lastTagResponse.next_tag_index,
      });
      tags.push({ name: lastTagResponse.name, value: lastTagResponse.value });
    }
  }
  return tags;
};
