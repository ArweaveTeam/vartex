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
    bundledId_dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_asc",
    bundledId_dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_asc",
    bundledId_dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_asc",
    bundledId_dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_asc",
    dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_asc",
    bundledId_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_asc",
    bundledId_dataRoots_recipients_tags:
      "tx_tag_gql_by_target_and_bundled_in_and_data_root_asc",
    bundledId_dataRoots_owners_tags:
      "tx_tag_gql_by_owner_and_bundled_in_and_data_root_asc",
    dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_data_root_asc",
    bundledId_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_asc",
    bundledId_dataRoots_ids_tags:
      "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_asc",
    dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_data_root_asc",
    bundledId_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_asc",
    dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_data_root_asc",
    bundledId_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_asc",
    ids_owners_recipients_tags: "tx_tag_gql_by_tx_id_and_owner_and_target_asc",
    bundledId_dataRoots_tags: "tx_tag_gql_by_bundled_in_and_data_root_asc",
    dataRoots_recipients_tags: "tx_tag_gql_by_target_and_data_root_asc",
    bundledId_recipients_tags: "tx_tag_gql_by_target_and_bundled_in_asc",
    dataRoots_owners_tags: "tx_tag_gql_by_owner_and_data_root_asc",
    bundledId_owners_tags: "tx_tag_gql_by_owner_and_bundled_in_asc",
    owners_recipients_tags: "tx_tag_gql_by_owner_and_target_asc",
    dataRoots_ids_tags: "tx_tag_gql_by_tx_id_and_data_root_asc",
    bundledId_ids_tags: "tx_tag_gql_by_tx_id_and_bundled_in_asc",
    ids_recipients_tags: "tx_tag_gql_by_tx_id_and_target_asc",
    ids_owners_tags: "tx_tag_gql_by_tx_id_and_owner_asc",
    dataRoots_tags: "tx_tag_gql_by_data_root_asc",
    bundledId_tags: "tx_tag_gql_by_bundled_in_asc",
    recipients_tags: "tx_tag_gql_by_target_asc",
    owners_tags: "tx_tag_gql_by_owner_asc",
    ids_tags: "tx_tag_gql_by_tx_id_asc",
    tags: "tx_tag_gql_asc",
  },
  HEIGHT_DESC: {
    bundledId_dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_and_data_root_desc",
    bundledId_dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_and_data_root_desc",
    bundledId_dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_and_data_root_desc",
    bundledId_dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_and_data_root_desc",
    dataRoots_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_data_root_desc",
    bundledId_ids_owners_recipients_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_target_and_bundled_in_desc",
    bundledId_dataRoots_recipients_tags:
      "tx_tag_gql_by_target_and_bundled_in_and_data_root_desc",
    bundledId_dataRoots_owners_tags:
      "tx_tag_gql_by_owner_and_bundled_in_and_data_root_desc",
    dataRoots_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_data_root_desc",
    bundledId_owners_recipients_tags:
      "tx_tag_gql_by_owner_and_target_and_bundled_in_desc",
    bundledId_dataRoots_ids_tags:
      "tx_tag_gql_by_tx_id_and_bundled_in_and_data_root_desc",
    dataRoots_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_data_root_desc",
    bundledId_ids_recipients_tags:
      "tx_tag_gql_by_tx_id_and_target_and_bundled_in_desc",
    dataRoots_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_data_root_desc",
    bundledId_ids_owners_tags:
      "tx_tag_gql_by_tx_id_and_owner_and_bundled_in_desc",
    ids_owners_recipients_tags: "tx_tag_gql_by_tx_id_and_owner_and_target_desc",
    bundledId_dataRoots_tags: "tx_tag_gql_by_bundled_in_and_data_root_desc",
    dataRoots_recipients_tags: "tx_tag_gql_by_target_and_data_root_desc",
    bundledId_recipients_tags: "tx_tag_gql_by_target_and_bundled_in_desc",
    dataRoots_owners_tags: "tx_tag_gql_by_owner_and_data_root_desc",
    bundledId_owners_tags: "tx_tag_gql_by_owner_and_bundled_in_desc",
    owners_recipients_tags: "tx_tag_gql_by_owner_and_target_desc",
    dataRoots_ids_tags: "tx_tag_gql_by_tx_id_and_data_root_desc",
    bundledId_ids_tags: "tx_tag_gql_by_tx_id_and_bundled_in_desc",
    ids_recipients_tags: "tx_tag_gql_by_tx_id_and_target_desc",
    ids_owners_tags: "tx_tag_gql_by_tx_id_and_owner_desc",
    dataRoots_tags: "tx_tag_gql_by_data_root_desc",
    bundledId_tags: "tx_tag_gql_by_bundled_in_desc",
    recipients_tags: "tx_tag_gql_by_target_desc",
    owners_tags: "tx_tag_gql_by_owner_desc",
    ids_tags: "tx_tag_gql_by_tx_id_desc",
    tags: "tx_tag_gql_desc",
  },
};

const buildTagFilterKey = (
  queryParameters: QueryTransactionsArguments
): string[] => {
  const filters = [];
  for (const parameter of Object.keys(queryParameters)) {
    if (
      [
        "tags",
        "ids",
        "recipients",
        "owners",
        "dataRoots",
        "bundledIn",
      ].includes(parameter) &&
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      !R.isEmpty<any>(
        queryParameters[parameter as keyof QueryTransactionsArguments]
      )
    ) {
      filters.push(parameter);
    }
  }
  return filters;
};

interface TagFilterCursor {
  cursorType?: string;
  txIndex: string;
  dataItemIndex: string;
  sortOrder: string;
}

function encodeCursor({
  sortOrder,
  txIndex,
  dataItemIndex,
}: TagFilterCursor): string {
  const string = JSON.stringify([
    "tag_search",
    sortOrder,
    txIndex,
    dataItemIndex,
  ]);
  return Buffer.from(string).toString("base64url");
}

function parseTagFilterCursor(cursor: string): TagFilterCursor {
  try {
    const [cursorType, sortOrder, txIndex, dataItemIndex] = JSON.parse(
      Buffer.from(cursor, "base64url").toString()
    ) as [string, string, string, string, number];
    return { cursorType, sortOrder, txIndex, dataItemIndex };
  } catch {
    throw new Error("invalid cursor");
  }
}

export const findTxIDsFromTagFilters = async (
  maxHeightBlock: CassandraTypes.Long,
  queryParameters: QueryTransactionsArguments
): Promise<[TxSearchResult[], boolean]> => {
  const tagFilterKeys = buildTagFilterKey(queryParameters);
  const tagFilterVals = R.pick(tagFilterKeys, queryParameters);
  const tableKey = tagFilterKeys.sort().join("_");
  const sortOrder =
    queryParameters.sort === "HEIGHT_ASC" ? "HEIGHT_ASC" : "HEIGHT_DESC";
  const table = filtersToTable[sortOrder][tableKey];
  const tagPairsIn = tagFilterVals.tags.reduce((accumulator, tagPairs) => {
    const tagName = toB64url(tagPairs.name || "");
    for (const tagValue of tagPairs.values) {
      accumulator.push(`'${tagName}|${toB64url(tagValue)}'`);
    }
    return accumulator;
  }, []);

  const cursorQuery =
    queryParameters.after &&
    typeof queryParameters.after === "string" &&
    !R.isEmpty(queryParameters.after);

  const maybeCursor = cursorQuery
    ? parseTagFilterCursor(queryParameters.after)
    : // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ({} as any);

  if (maybeCursor.cursorType && maybeCursor.cursorType !== "tag_search") {
    throw new Error(
      `invalid cursor: expected cursor of type tag_search but got ${maybeCursor.cursorType}`
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
    return [[], undefined];
  }

  const limit = Math.min(100, queryParameters.first || 10);

  const whereClause = tagFilterKeys.reduce((accumulator, key) => {
    if (key === "tags") {
      return accumulator;
    } else {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
    }
  }, "");

  const tagFilterQ = await cassandraClient.execute(
    `SELECT tx_id, tx_index, data_item_index FROM ${KEYSPACE}.${table} WHERE tx_index <= ${txsMaxHeight} AND tx_index >= ${txsMinHeight} AND tag_pair IN (${tagPairsIn.join(
      ","
    )})${whereClause} LIMIT ${limit + 1}`
  );

  const hasNextPage = tagFilterQ.rows.length > limit;

  const txSearchResult = tagFilterQ.rows.slice(0, limit).map((row) => ({
    txId: row.tx_id,
    cursor: encodeCursor({
      sortOrder,
      txIndex: row.tx_index.toString(),
      dataItemIndex: row.data_item_index.toString(),
    }),
  }));

  return [txSearchResult, hasNextPage];
};
