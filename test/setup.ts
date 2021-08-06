export const testEnvVars = {
  ...process.env,
  ARWEAVE_NODES: '["https://mockweave.net"]',
  CASSANDRA_CONTACT_POINTS:
    process.env.CASSANDRA_CONTACT_POINTS || '["localhost:9042"]',
  CASSANDRA_USERNAME: 'cassandra',
  CASSANDRA_PASSWORD: 'cassandra',
  PARALLEL: '32',
  KEYSPACE: 'testway',
  NODE_ENV: 'test',
};
process.env = testEnvVars;

// bypass ts isolatedmodules check
export default true;
