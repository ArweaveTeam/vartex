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
      tables: ["block_height_by_block_hash"],
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
    BlockGqlAsc: {
      keyspace: KEYSPACE,
      tables: ["block_gql_asc"],
    },
    BlockGqlDesc: {
      keyspace: KEYSPACE,
      tables: ["block_gql_desc"],
    },
    Manifest: {
      keyspace: KEYSPACE,
      tables: ["manifest"],
    },
    ManifestUnimported: {
      keyspace: KEYSPACE,
      tables: ["manifest_queue"],
    },
    Poa: {
      keyspace: KEYSPACE,
      tables: ["poa"],
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
  },
});

export const blockHeightToHashMapper = mapper.forModel(
  "BlockHeightByBlockHash"
);

export const blockMapper = mapper.forModel("Block");

export const blockGqlAscMapper = mapper.forModel("BlockGqlAsc");

export const blockGqlDescMapper = mapper.forModel("BlockGqlDesc");

export const poaMapper = mapper.forModel("Poa");

export const manifestMapper = mapper.forModel("Manifest");

export const manifestUnimportedMapper = mapper.forModel("ManifestUnimported");

export const permawebPathMapper = mapper.forModel("PermawebPath");

export const statusMapper = mapper.forModel("Status");

export const transactionMapper = mapper.forModel("Transaction");

export const txGqlAscMapper = mapper.forModel("TxGqlAsc");

export const txGqlDescMapper = mapper.forModel("TxGqlDesc");

// export const txTagMapper = mapper.forModel("TxTag");

export const txOffsetMapper = mapper.forModel("TxOffset");

// export const tagsByTxId = async (
//   txId: string
// ): Promise<CassandraTypes.Tuple[]> => {};

/*
export const tagsByTxId = async (txId: string): Promise<UpstreamTag[]> => {
  let lastTagResponse = await txTagMapper.get({ tx_id: txId, tag_index: 0 });
  const tags: UpstreamTag[] = [];

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
*/

export { cassandraClient };
