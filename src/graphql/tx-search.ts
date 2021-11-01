import * as R from "rambda";
import { cassandraClient } from "../database/cassandra";
import { toLong } from "../database/utils";
import { types as CassandraTypes } from "cassandra-driver";
import { TxSearchResult } from "./resolver-types";
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
      if (parameter === "recipients") {
        filters.push("target");
      } else {
        filters.push(parameter);
      }
    }
  }
  return filters;
};

interface TxFilterCursor {
  cursorType?: string;
  txIndex: string;
  dataItemIndex: string;
  sortOrder: string;
  nthMillion: number;
}

function encodeCursor({
  sortOrder,
  txIndex,
  dataItemIndex,
  nthMillion,
}: TxFilterCursor): string {
  const string = JSON.stringify([
    "tx_search",
    sortOrder,
    txIndex,
    dataItemIndex,
    nthMillion,
  ]);
  return Buffer.from(string).toString("base64url");
}

function parseTxFilterCursor(cursor: string): TxFilterCursor {
  try {
    const [cursorType, sortOrder, txIndex, dataItemIndex, nthMillion] =
      JSON.parse(Buffer.from(cursor, "base64url").toString()) as [
        string,
        string,
        string,
        string,
        number
      ];
    return { cursorType, sortOrder, txIndex, dataItemIndex, nthMillion };
  } catch {
    throw new Error("invalid cursor");
  }
}

export const findTxIDsFromTxFilters = async (
  maxHeightBlock: CassandraTypes.Long,
  queryParameters: QueryTransactionsArguments
): Promise<[TxSearchResult[], boolean]> => {
  const txFilterKeys = buildTxFilterKey(queryParameters);
  const tableKey = txFilterKeys.sort().join("_");
  const sortOrder =
    queryParameters.sort === "HEIGHT_ASC" ? "HEIGHT_ASC" : "HEIGHT_DESC";
  const table = R.isEmpty(txFilterKeys)
    ? sortOrder === "HEIGHT_ASC"
      ? "txs_sorted_asc"
      : "txs_sorted_desc"
    : filtersToTable[sortOrder][tableKey];

  const cursorQuery =
    queryParameters.after &&
    typeof queryParameters.after === "string" &&
    !R.isEmpty(queryParameters.after);

  const maybeCursor = cursorQuery
    ? parseTxFilterCursor(queryParameters.after)
    : ({} as any);

  if (maybeCursor.cursorType && maybeCursor.cursorType !== "tx_search") {
    throw new Error(
      `invalid cursor: expected cursor of type tx_search but got ${maybeCursor.cursorType}`
    );
  }

  if (maybeCursor.sortOrder && maybeCursor.sortOrder !== sortOrder) {
    throw new Error(
      `invalid cursor: expected sortOrder ${sortOrder} but got cursor of ${maybeCursor.sortOrder}`
    );
  }

  const txsMinHeight_ =
    typeof queryParameters.block === "object" &&
    typeof queryParameters.block.min === "number"
      ? queryParameters.block.min * 1000
      : 0;

  const txsMinHeight =
    typeof maybeCursor.txIndex !== "undefined" &&
    sortOrder === "HEIGHT_ASC" &&
    toLong(maybeCursor.txIndex).gt(toLong(txsMinHeight_))
      ? maybeCursor.txIndex
      : txsMinHeight_;

  const txsMaxHeight_ =
    typeof queryParameters.block === "object" &&
    typeof queryParameters.block.max === "number"
      ? queryParameters.block.max * 1000
      : maxHeightBlock.add(1).mul(1000).toString();

  const txsMaxHeight =
    typeof maybeCursor.txIndex !== "undefined" && sortOrder === "HEIGHT_DESC"
      ? maybeCursor.txIndex
      : txsMaxHeight_;

  if (queryParameters.first === 0) {
    return [[], false];
  }

  const limit = Math.min(100, queryParameters.first || 10);

  const whereClause = R.isEmpty(txFilterKeys)
    ? ""
    : txFilterKeys.reduce((accumulator, key) => {
        const k_ =
          key === "target"
            ? "recipients"
            : (key as keyof QueryTransactionsArguments);
        const whereVals: any = queryParameters[k_];
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

  let hasNextPage = false;
  let txsFilterRows = [];

  if (!R.isEmpty(txFilterKeys)) {
    const txFilterQ = await cassandraClient.execute(
      `SELECT tx_id, tx_index, data_item_index FROM ${KEYSPACE}.${table} WHERE tx_index <= ${txsMaxHeight} AND tx_index >= ${txsMinHeight} ${whereClause} LIMIT ${
        limit + 1
      }`
    );
    txsFilterRows = txFilterQ.rows;
    hasNextPage = txFilterQ.rows.length > limit;
  } else {
    const xMillions = toLong(txsMaxHeight).div(1e6).add(1);

    const rangePostFn = sortOrder === "HEIGHT_ASC" ? R.identity : R.reverse;

    const bucketStart =
      typeof maybeCursor.nthMillion !== "undefined" &&
      maybeCursor.nthMillion !== -1 &&
      sortOrder === "HEIGHT_ASC"
        ? maybeCursor.nthMillion
        : 0;

    const bucketEnd =
      typeof maybeCursor.nthMillion !== "undefined" &&
      maybeCursor.nthMillion !== -1 &&
      sortOrder === "HEIGHT_DESC"
        ? maybeCursor.nthMillion + 1
        : (xMillions.add(1).toInt() as number);

    const buckets: number[] = rangePostFn(
      (R.range as any)(bucketStart, bucketEnd)
    );

    let resultCount = 0;
    let nthBucket = 0;

    while (nthBucket < buckets.length && resultCount < limit) {
      const nextResult = await cassandraClient.execute(
        `SELECT tx_id, tx_index, data_item_index FROM ${KEYSPACE}.${table} WHERE tx_index <= ${txsMaxHeight} AND tx_index >= ${txsMinHeight} AND nth_million=${
          buckets[nthBucket]
        } LIMIT ${limit - resultCount + 1}`
      );
      for (const row of nextResult.rows) {
        !hasNextPage &&
          txsFilterRows.push(
            (R.assoc as any)("nthMillion", buckets[nthBucket], row)
          );
        if (resultCount !== limit) {
          resultCount += 1;
        } else {
          hasNextPage = true;
        }
      }

      nthBucket += 1;
    }
  }

  const cursors = txsFilterRows.slice(1, limit + 1).map((row) =>
    encodeCursor({
      sortOrder,
      txIndex: row.tx_index.toString(),
      dataItemIndex: row.data_item_index.toString(),
      nthMillion: R.isEmpty(txFilterKeys) ? row.nthMillion : -1,
    })
  );

  const txSearchResult = txsFilterRows.slice(0, limit).map((row, index) => ({
    txId: row.tx_id,
    cursor: index < cursors.length ? cursors[index] : undefined,
  }));

  return [txSearchResult, hasNextPage];
};
