import * as R from "rambda";
import {
  types as CassandraTypes,
  Client as CassandraClient,
} from "cassandra-driver";
import { KEYSPACE } from "../constants";

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

export const getMaxHeightBlock = async (
  cassandraClient: CassandraClient
): Promise<[string, CassandraTypes.Long]> => {
  let bucketNumber = 0;
  let lastMaxHeight: [string, CassandraTypes.Long] = ["", toLong(-1)];
  let lastResponse = await cassandraClient.execute(
    `SELECT block_height,block_hash FROM ${KEYSPACE}.block_height_sorted_desc WHERE nth_million = 0 limit 1`
  );
  while (lastResponse && !R.isEmpty(lastResponse.rows)) {
    bucketNumber += 1;
    const row = lastResponse.rows[0];
    if (row) {
      lastMaxHeight = [row["block__hash"], row["block_height"]];
    }
    lastResponse = await cassandraClient.execute(
      `SELECT block_height,block_hash FROM ${KEYSPACE}.block_height_sorted_desc WHERE nth_million = ${bucketNumber} limit 1`
    );
  }
  return lastMaxHeight;
};
