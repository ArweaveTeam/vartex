import { KEYSPACE } from "../constants";

// tx_gql_by_owner_asc
// tx_gql_by_owner_desc
// tx_gql_by_target_asc
// tx_gql_by_target_desc
// tx_gql_by_bundled_in_asc
// tx_gql_by_bundled_in_desc
// tx_gql_by_data_root_asc
// tx_gql_by_data_root_desc
// tx_gql_by_tx_id_owner_asc
// tx_gql_by_tx_id_owner_desc
// tx_gql_by_tx_id_target_asc
// tx_gql_by_tx_id_target_desc
// tx_gql_by_tx_id_bundled_in_asc
// tx_gql_by_tx_id_bundled_in_desc
// tx_gql_by_tx_id_data_root_asc
// tx_gql_by_tx_id_data_root_desc
// tx_gql_by_owner_target_asc
// tx_gql_by_owner_target_desc
// tx_gql_by_owner_bundled_in_asc
// tx_gql_by_owner_bundled_in_desc
// tx_gql_by_owner_data_root_asc
// tx_gql_by_owner_data_root_desc
// tx_gql_by_target_bundled_in_asc
// tx_gql_by_target_bundled_in_desc
// tx_gql_by_target_data_root_asc
// tx_gql_by_target_data_root_desc
// tx_gql_by_bundled_in_data_root_asc
// tx_gql_by_bundled_in_data_root_desc
// tx_gql_by_tx_id_owner_target_asc
// tx_gql_by_tx_id_owner_target_desc
// tx_gql_by_tx_id_owner_bundled_in_asc
// tx_gql_by_tx_id_owner_bundled_in_desc
// tx_gql_by_tx_id_owner_data_root_asc
// tx_gql_by_tx_id_owner_data_root_desc
// tx_gql_by_tx_id_target_bundled_in_asc
// tx_gql_by_tx_id_target_bundled_in_desc
// tx_gql_by_tx_id_target_data_root_asc
// tx_gql_by_tx_id_target_data_root_desc
// tx_gql_by_tx_id_bundled_in_data_root_asc
// tx_gql_by_tx_id_bundled_in_data_root_desc
// tx_gql_by_owner_target_bundled_in_asc
// tx_gql_by_owner_target_bundled_in_desc
// tx_gql_by_owner_target_data_root_asc
// tx_gql_by_owner_target_data_root_desc
// tx_gql_by_owner_bundled_in_data_root_asc
// tx_gql_by_owner_bundled_in_data_root_desc
// tx_gql_by_target_bundled_in_data_root_asc
// tx_gql_by_target_bundled_in_data_root_desc
// tx_gql_by_tx_id_owner_target_bundled_in_asc
// tx_gql_by_tx_id_owner_target_bundled_in_desc
// tx_gql_by_tx_id_owner_target_data_root_asc
// tx_gql_by_tx_id_owner_target_data_root_desc
// tx_gql_by_tx_id_owner_bundled_in_data_root_asc
// tx_gql_by_tx_id_owner_bundled_in_data_root_desc
// tx_gql_by_tx_id_target_bundled_in_data_root_asc
// tx_gql_by_tx_id_target_bundled_in_data_root_desc
// tx_gql_by_owner_target_bundled_in_data_root_asc
// tx_gql_by_owner_target_bundled_in_data_root_desc
// tx_gql_by_tx_id_owner_target_bundled_in_data_root_asc
// tx_gql_by_tx_id_owner_target_bundled_in_data_root_desc

export interface DropTxsQueryParameters {
  bundledIn: string;
  dataItemIndex: string;
  dataRoot: string;
  owner: string;
  target: string;
  txId: string;
  txIndex: string;
}

export function dropTxsQuery({
  bundledIn,
  dataItemIndex,
  dataRoot,
  target,
  txId,
  txIndex,
  owner,
}: DropTxsQueryParameters) {
  const commonWhere = `
    WHERE tx_index=${txIndex}
    AND data_item_index=${dataItemIndex}
  `;

  return `
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_asc
    ${commonWhere} AND owner='${owner}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_desc
    ${commonWhere} AND owner='${owner}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_target_asc
    ${commonWhere} AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_target_desc
    ${commonWhere} AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_bundled_in_asc
    ${commonWhere} AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_bundled_in_desc
    ${commonWhere} AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_data_root_asc
    ${commonWhere} AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_data_root_desc
    ${commonWhere} AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_target_asc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_target_desc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_bundled_in_asc
    ${commonWhere} AND tx_id='${txId}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_bundled_in_desc
    ${commonWhere} AND tx_id='${txId}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_target_asc
    ${commonWhere} AND owner='${owner}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_target_desc
    ${commonWhere} AND owner='${owner}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_bundled_in_asc
    ${commonWhere} AND owner='${owner}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_bundled_in_desc
    ${commonWhere} AND owner='${owner}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_data_root_asc
    ${commonWhere} AND owner='${owner}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_data_root_desc
    ${commonWhere} AND owner='${owner}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_target_bundled_in_asc
    ${commonWhere} AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_target_bundled_in_desc
    ${commonWhere} AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_target_data_root_asc
    ${commonWhere} AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_target_data_root_desc
    ${commonWhere} AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_bundled_in_data_root_asc
    ${commonWhere} AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_bundled_in_data_root_desc
    ${commonWhere} AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_target_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_target_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_bundled_in_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_bundled_in_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_target_bundled_in_asc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_target_bundled_in_desc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_target_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_target_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_bundled_in_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_bundled_in_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_target_bundled_in_asc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_target_bundled_in_desc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_target_data_root_asc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_target_data_root_desc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_bundled_in_data_root_asc
    ${commonWhere} AND owner='${owner}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_bundled_in_data_root_desc
    ${commonWhere} AND owner='${owner}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_target_bundled_in_data_root_asc
    ${commonWhere} AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_target_bundled_in_data_root_desc
    ${commonWhere} AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_target_bundled_in_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_target_bundled_in_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_target_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_target_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_bundled_in_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_bundled_in_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_target_bundled_in_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_target_bundled_in_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_target_bundled_in_data_root_asc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_owner_target_bundled_in_data_root_desc
    ${commonWhere} AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_target_bundled_in_data_root_asc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
    DELETE FROM ${KEYSPACE}.tx_gql_by_tx_id_owner_target_bundled_in_data_root_desc
    ${commonWhere} AND tx_id='${txId}' AND owner='${owner}' AND target='${target}' AND bundled_in='${bundledIn}' AND data_root='${dataRoot}';
  `;
}
