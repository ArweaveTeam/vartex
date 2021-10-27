import * as R from "rambda";
import { cassandraClient } from "../database/cassandra";
import { types as CassandraTypes } from "cassandra-driver";
import { QueryTransactionsArgs as QueryTransactionsArguments } from "./types.graphql";
import { toB64url } from "../query/transaction";
import { KEYSPACE } from "../constants";

const filtersToTable: { [direction: string]: Record<string, string> } = {
  HEIGHT_ASC: {
    bundledIn_dataRoots_ids_owner_target:
      "tx_gql_by_tx_id_owner_target_bundled_in_data_root_asc",
    bundledIn_dataRoots_owner_target:
      "tx_gql_by_owner_target_bundled_in_data_root_asc",
    bundledIn_dataRoots_ids_target:
      "tx_gql_by_tx_id_target_bundled_in_data_root_asc",
    bundledIn_dataRoots_ids_owner:
      "tx_gql_by_tx_id_owner_bundled_in_data_root_asc",
    dataRoots_ids_owner_target: "tx_gql_by_tx_id_owner_target_data_root_asc",
    bundledIn_ids_owner_target: "tx_gql_by_tx_id_owner_target_bundled_in_asc",
    bundledIn_dataRoots_target: "tx_gql_by_target_bundled_in_data_root_asc",
    bundledIn_dataRoots_owner: "tx_gql_by_owner_bundled_in_data_root_asc",
    dataRoots_owner_target: "tx_gql_by_owner_target_data_root_asc",
    bundledIn_owner_target: "tx_gql_by_owner_target_bundled_in_asc",
    bundledIn_dataRoots_ids: "tx_gql_by_tx_id_bundled_in_data_root_asc",
    dataRoots_ids_target: "tx_gql_by_tx_id_target_data_root_asc",
    bundledIn_ids_target: "tx_gql_by_tx_id_target_bundled_in_asc",
    dataRoots_ids_owner: "tx_gql_by_tx_id_owner_data_root_asc",
    bundledIn_ids_owner: "tx_gql_by_tx_id_owner_bundled_in_asc",
    ids_owner_target: "tx_gql_by_tx_id_owner_target_asc",
    bundledIn_dataRoots: "tx_gql_by_bundled_in_data_root_asc",
    dataRoots_target: "tx_gql_by_target_data_root_asc",
    bundledId_target: "tx_gql_by_target_bundled_in_asc",
    dataRoots_owner: "tx_gql_by_owner_data_root_asc",
    bundledId_owner: "tx_gql_by_owner_bundled_in_asc",
    owner_target: "tx_gql_by_owner_target_asc",
    dataRoots_ids: "tx_gql_by_tx_id_data_root_asc",
    bundledId_ids: "tx_gql_by_tx_id_bundled_in_asc",
    ids_target: "tx_gql_by_tx_id_target_asc",
    ids_owner: "tx_gql_by_tx_id_owner_asc",
    dataRoots: "tx_gql_by_data_root_asc",
    bundledIn: "tx_gql_by_bundled_in_asc",
    target: "tx_gql_by_target_asc",
    owner: "tx_gql_by_owner_asc",
  },
  HEIGHT_DESC: {
    bundledIn_dataRoots_ids_owner_target:
      "tx_gql_by_tx_id_owner_target_bundled_in_data_root_desc",
    bundledIn_dataRoots_owner_target:
      "tx_gql_by_owner_target_bundled_in_data_root_desc",
    bundledIn_dataRoots_ids_target:
      "tx_gql_by_tx_id_target_bundled_in_data_root_desc",
    bundledIn_dataRoots_ids_owner:
      "tx_gql_by_tx_id_owner_bundled_in_data_root_desc",
    dataRoots_ids_owner_target: "tx_gql_by_tx_id_owner_target_data_root_desc",
    bundledIn_ids_owner_target: "tx_gql_by_tx_id_owner_target_bundled_in_desc",
    bundledIn_dataRoots_target: "tx_gql_by_target_bundled_in_data_root_desc",
    bundledIn_dataRoots_owner: "tx_gql_by_owner_bundled_in_data_root_desc",
    dataRoots_owner_target: "tx_gql_by_owner_target_data_root_desc",
    bundledIn_owner_target: "tx_gql_by_owner_target_bundled_in_desc",
    bundledIn_dataRoots_ids: "tx_gql_by_tx_id_bundled_in_data_root_desc",
    dataRoots_ids_target: "tx_gql_by_tx_id_target_data_root_desc",
    bundledIn_ids_target: "tx_gql_by_tx_id_target_bundled_in_desc",
    dataRoots_ids_owner: "tx_gql_by_tx_id_owner_data_root_desc",
    bundledIn_ids_owner: "tx_gql_by_tx_id_owner_bundled_in_desc",
    ids_owner_target: "tx_gql_by_tx_id_owner_target_desc",
    bundledIn_dataRoots: "tx_gql_by_bundled_in_data_root_desc",
    dataRoots_target: "tx_gql_by_target_data_root_desc",
    bundledId_target: "tx_gql_by_target_bundled_in_desc",
    dataRoots_owner: "tx_gql_by_owner_data_root_desc",
    bundledId_owner: "tx_gql_by_owner_bundled_in_desc",
    owner_target: "tx_gql_by_owner_target_desc",
    dataRoots_ids: "tx_gql_by_tx_id_data_root_desc",
    bundledId_ids: "tx_gql_by_tx_id_bundled_in_desc",
    ids_target: "tx_gql_by_tx_id_target_desc",
    ids_owner: "tx_gql_by_tx_id_owner_desc",
    dataRoots: "tx_gql_by_data_root_desc",
    bundledIn: "tx_gql_by_bundled_in_desc",
    target: "tx_gql_by_target_desc",
    owner: "tx_gql_by_owner_desc",
  },
};

//  {
//   tagFilterKeys,
//   tagFilterVals,
//   minHeight,
//   maxHeight,
//   limit,
//   offset,
//   sortOrder,
// }: {
//   txFilterKeys: string[];
//   txFilterVals: { [any_: string]: any };
//   minHeight?: CassandraTypes.Long;
//   maxHeight?: CassandraTypes.Long;
//   limit?: number;
//   offset?: number;
//   sortOrder?: string;
// }

const buildTxFilterKey = (
  queryParameters: QueryTransactionsArguments
): string[] => {
  const filters = [];
  for (const parameter of Object.keys(queryParameters)) {
    if (
      ["ids", "recipients", "owners", "dataRoots", "bundledIn"].includes(
        parameter
      ) &&
      !R.isEmpty<any>(
        queryParameters[parameter as keyof QueryTransactionsArguments]
      )
    ) {
      filters.push(parameter);
    }
  }
  return filters;
};

export const findTxIDsFromTxFilters = async (
  queryParameters: QueryTransactionsArguments
): Promise<[string[], string | undefined]> => {
  const txFilterKeys = buildTxFilterKey(queryParameters);
  const tableKey = txFilterKeys.sort().join("_");
  const sortOrder =
    queryParameters.sort === "HEIGHT_ASC" ? "HEIGHT_ASC" : "HEIGHT_DESC";
  const table = filtersToTable[sortOrder][tableKey];

  const txsMinHeight =
    typeof queryParameters.block === "object" &&
    typeof queryParameters.block.min === "number"
      ? queryParameters.block.min * 1000
      : 0;

  const txsMaxHeight =
    typeof queryParameters.block === "object" &&
    typeof queryParameters.block.max === "number"
      ? queryParameters.block.max * 1000
      : Number.MAX_SAFE_INTEGER;

  if (queryParameters.first === 0) {
    return [[], undefined];
  }

  const limit = Math.max(100, queryParameters.first || 10);

  const whereClause = txFilterKeys.reduce((accumulator, key) => {
    const whereVals: any =
      queryParameters[key as keyof QueryTransactionsArguments];
    const cqlKey = R.propOr(
      key,
      key
    )({
      ids: "tx_id",
      recipients: "target",
      owners: "owner",
      dataRoots: "data_root",
      bundledIn: "bundled_in",
    });

    const whereValsString =
      whereVals.length === 1
        ? ` = '${whereVals[0]}'`
        : `IN (${whereVals.map((wv: string) => `'${wv}'`).join(",")})`;
    return `${accumulator} AND ${cqlKey} ${whereValsString}`;
  }, "");

  const txFilterQ = await cassandraClient.execute(
    `SELECT tx_id, tx_index FROM ${KEYSPACE}.${table} WHERE tx_index <= ${txsMaxHeight} AND tx_index >= ${txsMinHeight} ${whereClause} LIMIT ${
      limit + 1
    }`
  );

  const txIdsResult = txFilterQ.rows.map((row) => row.tx_id);

  const cursor =
    txFilterQ.rows.length > limit
      ? `${
          (sortOrder === "HEIGHT_ASC" ? R.head : R.last)(txFilterQ.rows)
            .tx_index
        }`
      : undefined;

  return [txIdsResult, cursor];
};
