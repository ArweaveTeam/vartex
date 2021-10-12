import * as R from "rambda";
import { mapping, types as CassandraTypes } from "cassandra-driver";
import { cassandraClient } from "./cassandra";
import { KEYSPACE } from "../constants";

const { Mapper } = mapping;

// prune the null values away
const withDefault = ({
  name,
  fallback,
}: {
  name: string;
  fallback: unknown;
}): Record<string, unknown> => ({
  [name]: {
    name,
    toModel: (v: unknown) => v || fallback,
    fromModel: (v: unknown) => v || fallback,
  },
});

const mapper = new Mapper(cassandraClient, {
  models: {
    BlockHeightByBlockHash: {
      keyspace: KEYSPACE,
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
    Poa: {
      keyspace: KEYSPACE,
      tables: ["poa"],
    },
    Status: {
      keyspace: KEYSPACE,
      tables: ["status"],
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
      keyspace: KEYSPACE,
      tables: ["tx_tag_migration_1"],
    },
    TxOffset: {
      keyspace: KEYSPACE,
      tables: ["tx_offset"],
    },
  },
});

export const blockHeightToHashMapper = mapper.forModel(
  "BlockHeightByBlockHash"
);

export const blockMapper = mapper.forModel("Block");

export const poaMapper = mapper.forModel("Poa");

export const statusMapper = mapper.forModel("Status");

export const transactionMapper = mapper.forModel("Transaction");

export const txTagMapper = mapper.forModel("TxTag");

export const txOffsetMapper = mapper.forModel("TxOffset");

export const tagsByTxId = async (
  txId: string
): Promise<CassandraTypes.Tuple[]> => {
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
