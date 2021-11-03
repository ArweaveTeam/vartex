import { cleanEnv, bool, str, json, num, port } from "envalid";
import { config } from "dotenv";

process.env.NODE_ENV !== "test" && config();

export const env = cleanEnv(process.env, {
  ARWEAVE_NODES: json({ default: ["https://arweave.net"] }),
  CASSANDRA_CONTACT_POINTS: json({ default: ["127.0.0.1:9042"] }),
  CASSANDRA_USERNAME: str({ default: "cassandra" }),
  CASSANDRA_PASSWORD: str({ default: "cassandra" }),
  PARALLEL_ANS102_IMPORT: num({ default: 2 }),
  PARALLEL_ANS104_IMPORT: num({ default: 2 }),
  PARALLEL_BLOCK_IMPORT: num({ default: 4 }),
  PARALLEL_MANIFEST_IMPORT: num({ default: 4 }),
  PARALLEL_TX_IMPORT: num({ default: 8 }),
  KEYSPACE: str({ default: "gateway" }),
  PORT: port({ default: 1248 }),
  // internal defs only applicable to master node
  OFFLOAD_MANIFEST_IMPORT: bool({ default: false }),
  OFFLOAD_ANS102_IMPORT: bool({ default: false }),
  OFFLOAD_ANS104_IMPORT: bool({ default: false }),
  OFFLOAD_TX_IMPORT: bool({ default: false }),
});

export const isGatewayNodeModeEnabled = !!process.env["VARTEX_GW_NODE"];

export const KEYSPACE = env.KEYSPACE;
