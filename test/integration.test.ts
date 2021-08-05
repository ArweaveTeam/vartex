import got from 'got';
import nock from 'nock';
import net from 'net';
import * as helpers from './helpers';

describe('integration suite', function () {
  jest.setTimeout(60000);
  beforeAll(async function () {
    process.env = {
      ARWEAVE_NODES: 'https://mockweave.net',
      CASSANDRA_CONTACT_POINTS:
        process.env.CASSANDRA_CONTACT_POINTS || 'localhost:9042',
      CASSANDRA_USERNAME: 'cassandra',
      CASSANDRA_PASSWORD: 'cassandra',
      PARALLEL: '32',
      KEYSPACE: 'testway',
      ...process.env,
    };
    await helpers.waitForCassandra();
  });

  beforeEach(async () => {
    jest.resetModules();
    await helpers.nuke();
    await helpers.initDb();
    // process.env = { ...OLD_ENV };
  });

  // afterAll(() => {
  //   process.env = OLD_ENV;
  // });

  test('', () => expect(1).toEqual(2));
});
