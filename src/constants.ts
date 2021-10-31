import { config } from "dotenv";

process.env.NODE_ENV !== "test" && config();

export const HTTP_TIMEOUT_SECONDS: number = process.env["HTTP_TIMEOUT_SECONDS"]
  ? Number.parseInt(process.env["HTTP_TIMEOUT_SECONDS"] || "15")
  : 15;

if (Number.isNaN(HTTP_TIMEOUT_SECONDS)) {
  console.error("the provided value for HTTP_TIMEOUT_SECONDS was not a number");
  process.exit(1);
}

export const KEYSPACE: string = process.env["KEYSPACE"]
  ? process.env["KEYSPACE"]
  : "gateway";

export const isGatewayNodeModeEnabled = !!process.env["VARTEX_GW_NODE"];

export const IPC_DATA = {};

export const POLLTIME_DELAY_SECONDS: number = process.env[
  "POLLTIME_DELAY_SECONDS"
]
  ? Math.min(1, Number.parseInt(process.env["POLLTIME_DELAY_SECONDS"] || "5"))
  : 5;

// Table names (because migration requires name change)
export const TABLE_BLOCK_HEIGHT_BY_BLOCK_HASH = "block_height_by_block_hash";
export const TABLE_BLOCK = "block";
export const TABLE_GQL_BLOCK_ASC = "block_gql_asc";
export const TABLE_GQL_BLOCK_DESC = "block_gql_desc";
export const TABLE_GQL_TX_ASC = "tx_id_gql_asc";
export const TABLE_GQL_TX_DESC = "tx_id_gql_desc";
export const TABLE_TX = "transaction";
export const TABLE_TX_OFFSET = "tx_offset";

export const tableId = {
  TABLE_BLOCK_HEIGHT_BY_BLOCK_HASH,
  TABLE_BLOCK,
  TABLE_GQL_BLOCK_ASC,
  TABLE_GQL_BLOCK_DESC,
  TABLE_GQL_TX_ASC,
  TABLE_GQL_TX_DESC,
  TABLE_TX,
  TABLE_TX_OFFSET,
};
