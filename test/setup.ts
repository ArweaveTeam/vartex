export const testEnvVars = {
  ...process.env,
  PORT: "12482",
  ARWEAVE_NODES: '["http://localhost:12345"]',
  CASSANDRA_CONTACT_POINTS:
    process.env.CASSANDRA_CONTACT_POINTS || '["localhost:9042"]',
  CASSANDRA_USERNAME: "cassandra",
  CASSANDRA_PASSWORD: "cassandra",
  PARALLEL: "32",
  KEYSPACE: "testway",
  NODE_ENV: "test",
  POLLTIME_DELAY_SECONDS: "1",
  CACHE_IMPORT_PATH: "./cache/test-imports",
};
process.env = testEnvVars;

// bypass ts isolatedmodules check
export default true;
