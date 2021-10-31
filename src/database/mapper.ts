import * as R from "rambda";
import { mapping, types as CassandraTypes } from "cassandra-driver";
import { cassandraClient } from "./cassandra";
import { UpstreamTag } from "../types/cassandra";
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
      tables: ["block_height_to_block_hash"],
    },
    Block: {
      keyspace: KEYSPACE,
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
    Manifest: {
      keyspace: KEYSPACE,
      tables: ["manifest"],
    },
    ManifestQueue: {
      keyspace: KEYSPACE,
      tables: ["manifest_queue"],
    },
    PermawebPath: {
      keyspace: KEYSPACE,
      tables: ["permaweb_path"],
    },
    Status: {
      keyspace: KEYSPACE,
      tables: ["status"],
      columns: R.mergeAll([
        withDefault({ name: "current_migrations", fallback: {} }),
        withDefault({ name: "current_imports", fallback: [] }),
      ]),
    },
    Transaction: {
      keyspace: KEYSPACE,
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
    TxGqlAsc: {
      keyspace: KEYSPACE,
      tables: ["tx_id_gql_asc"],
    },
    TxGqlDesc: {
      keyspace: KEYSPACE,
      tables: ["tx_id_gql_desc"],
    },
    // TxTag: {
    //   keyspace: KEYSPACE,
    //   tables: ["tx_tag"],
    // },
    TxOffset: {
      keyspace: KEYSPACE,
      tables: ["tx_offset"],
    },
    TxQueue: {
      keyspace: KEYSPACE,
      tables: ["tx_queue"],
    },
  },
});

export const blockHeightToHashMapper = mapper.forModel(
  "BlockHeightByBlockHash"
);

export const blockMapper = mapper.forModel("Block");

export const manifestMapper = mapper.forModel("Manifest");

export const manifestQueueMapper = mapper.forModel("ManifestQueue");

export const permawebPathMapper = mapper.forModel("PermawebPath");

export const statusMapper = mapper.forModel("Status");

export const transactionMapper = mapper.forModel("Transaction");

export const txGqlAscMapper = mapper.forModel("TxGqlAsc");

export const txGqlDescMapper = mapper.forModel("TxGqlDesc");

export const txOffsetMapper = mapper.forModel("TxOffset");

export const txQueueMapper = mapper.forModel("TxQueue");

export { cassandraClient };
