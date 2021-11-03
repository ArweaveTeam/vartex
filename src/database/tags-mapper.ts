import { Client as CassandraClient, mapping } from "cassandra-driver";
import { KEYSPACE } from "../constants";

const { Mapper } = mapping;

export const tagModels: Record<string, string[]> = {
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

export const makeTagsMapper = (cassandraClient: CassandraClient): any =>
  new Mapper(cassandraClient, {
    models: {
      Tag: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_asc", "tx_tag_gql_desc"],
      },
      TagAndTxId: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_tx_id_asc", "tx_tag_gql_by_tx_id_desc"],
      },
      TagAndOwner: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_owner_asc", "tx_tag_gql_by_owner_desc"],
      },
      TagAndTarget: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_target_asc", "tx_tag_gql_by_target_desc"],
      },
      TagAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_bundled_in_asc",
          "tx_tag_gql_by_bundled_in_desc",
        ],
      },
      TagAndDataRoot: {
        keyspace: KEYSPACE,
        tables: ["tx_tag_gql_by_data_root_asc", "tx_tag_gql_by_data_root_desc"],
      },
      TagAndTxIdAndOwner: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_asc",
          "tx_tag_gql_by_tx_id_and_owner_desc",
        ],
      },
      TagAndTxIdAndTarget: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_target_asc",
          "tx_tag_gql_by_tx_id_and_target_desc",
        ],
      },
      TagAndTxIdAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_bundled_in_asc",
          "tx_tag_gql_by_tx_id_and_bundled_in_desc",
        ],
      },
      TagAndTxIdAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_data_root_asc",
          "tx_tag_gql_by_tx_id_and_data_root_desc",
        ],
      },
      TagAndOwnerAndTarget: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_asc",
          "tx_tag_gql_by_owner_and_target_desc",
        ],
      },
      TagAndOwnerAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_bundled_in_asc",
          "tx_tag_gql_by_owner_and_bundled_in_desc",
        ],
      },
      TagAndOwnerAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_data_root_asc",
          "tx_tag_gql_by_owner_and_data_root_desc",
        ],
      },
      TagAndTargetAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_target_and_bundled_in_asc",
          "tx_tag_gql_by_target_and_bundled_in_desc",
        ],
      },
      TagAndTargetAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_target_and_data_root_asc",
          "tx_tag_gql_by_target_and_data_root_desc",
        ],
      },
      TagAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_bundled_in_and_data_root_asc",
          "tx_tag_gql_by_bundled_in_and_data_root_desc",
        ],
      },
      TagAndTxIdAndOwnerAndTarget: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_asc",
          "tx_tag_gql_by_tx_id_and_owner_and_target_desc",
        ],
      },
      TagAndTxIdAndOwnerAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc",
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_desc",
        ],
      },
      TagAndTxIdAndOwnerAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_data_root_asc",
          "tx_tag_gql_by_tx_id_and_owner_and_data_root_desc",
        ],
      },
      TagAndTxIdAndTargetAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_target_and_bundled_in_asc",
          "tx_tag_gql_by_tx_id_and_target_and_bundled_in_desc",
        ],
      },
      TagAndTxIdAndTargetAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_target_and_data_root_asc",
          "tx_tag_gql_by_tx_id_and_target_and_data_root_desc",
        ],
      },
      TagAndTxIdAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_asc",
          "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_desc",
        ],
      },
      TagAndOwnerAndTargetAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_and_bundled_in_asc",
          "tx_tag_gql_by_owner_and_target_and_bundled_in_desc",
        ],
      },
      TagAndOwnerAndTargetAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_and_data_root_asc",
          "tx_tag_gql_by_owner_and_target_and_data_root_desc",
        ],
      },
      TagAndOwnerAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_bundled_in_and_data_root_asc",
          "tx_tag_gql_by_owner_and_bundled_in_and_data_root_desc",
        ],
      },
      TagAndTargetAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_target_and_bundled_in_and_data_root_asc",
          "tx_tag_gql_by_target_and_bundled_in_and_data_root_desc",
        ],
      },
      TagAndTxIdAndOwnerAndTargetAndBundledIn: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_asc",
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_desc",
        ],
      },
      TagAndTxIdAndOwnerAndTargetAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_asc",
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_desc",
        ],
      },
      TagAndTxIdAndOwnerAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_asc",
          "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_desc",
        ],
      },
      TagAndTxIdAndTargetAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_asc",
          "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_desc",
        ],
      },
      TagAndOwnerAndTargetAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_asc",
          "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_desc",
        ],
      },
      TagAndTxIdAndOwnerAndTargetAndBundledInAndDataRoot: {
        keyspace: KEYSPACE,
        tables: [
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_asc",
          "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_desc",
        ],
      },
    },
  });

export interface DropTagQueryParameters {
  bundledIn: string;
  dataItemIndex: string;
  dataRoot: string;
  owner: string;
  tagName: string;
  tagValue: string;
  tagIndex: string;
  target: string;
  txId: string;
  txIndex: string;
}

export function dropTagQuery({
  bundledIn,
  dataItemIndex,
  dataRoot,
  tagIndex,
  tagName,
  tagValue,
  target,
  txId,
  txIndex,
  owner,
}: DropTagQueryParameters) {
  const tagPair = `${tagName}-${tagValue}`;
  const commonWhere = `WHERE tag_pair='${tagPair}' AND tx_index=${txIndex}
                       AND data_item_index=${dataItemIndex} AND tag_index=${tagIndex}`;

  return `
    DELETE FROM ${KEYSPACE}.tx_tag_gql_asc
    ${commonWhere};
    tx_tag_gql_desc
    ${commonWhere};
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_asc
    ${commonWhere} AND tx_id='${txId}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_desc
    ${commonWhere} AND tx_id='${txId}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_asc
    ${commonWhere} AND owner='${owner}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_desc
    ${commonWhere} AND owner='${owner}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_target_asc
    ${commonWhere} AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_target_desc
    ${commonWhere} AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_bundled_in_asc
    ${commonWhere} AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_bundled_in_desc
    ${commonWhere} AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_data_root_asc
    ${commonWhere} AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_data_root_desc
    ${commonWhere} AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_target_asc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_target_desc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_bundled_in_asc
    ${commonWhere} AND tx_id='${txId}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_bundled_in_desc
    ${commonWhere} AND tx_id='${txId}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_target_asc
    ${commonWhere} AND owner='${owner}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_target_desc
    ${commonWhere} AND owner='${owner}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_bundled_in_asc
    ${commonWhere} AND owner='${owner}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_bundled_in_desc
    ${commonWhere} AND owner='${owner}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_data_root_asc
    ${commonWhere} AND owner='${owner}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_data_root_desc
    ${commonWhere} AND owner='${owner}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_target_and_bundled_in_asc
    ${commonWhere} AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_target_and_bundled_in_desc
    ${commonWhere} AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_target_and_data_root_asc
    ${commonWhere} AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_target_and_data_root_desc
    ${commonWhere} AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_bundled_in_and_data_root_asc
    ${commonWhere} AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_bundled_in_and_data_root_desc
    ${commonWhere} AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_target_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_target_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_bundled_in_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_target_and_bundled_in_asc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_target_and_bundled_in_desc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_target_and_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_target_and_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_target_and_bundled_in_asc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_target_and_bundled_in_desc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_target_and_data_root_asc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_target_and_data_root_desc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_bundled_in_and_data_root_asc
    ${commonWhere} AND owner='${owner}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_bundled_in_and_data_root_desc
    ${commonWhere} AND owner='${owner}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_target_and_bundled_in_and_data_root_asc
    ${commonWhere} AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_target_and_bundled_in_and_data_root_desc
    ${commonWhere} AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_asc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_desc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
`;
}
