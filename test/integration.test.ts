import { jest } from '@jest/globals';
import got from 'got';
import nock from 'nock';
import net from 'net';
import { startSync, signalHook } from '../src/database/sync.database';
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
      NODE_ENV: 'test',
      ...process.env,
    };
    await helpers.waitForCassandra();
  });

  afterAll(signalHook);

  beforeEach(async () => {
    jest.resetModules();
    await helpers.nuke();
    await helpers.initDb();
    // process.env = { ...OLD_ENV };
  });

  // afterAll(() => {
  //   process.env = OLD_ENV;
  // });

  test('it writes blocks into cassandra', async () => {
    const mockBlocks = helpers.generateMockBlocks({ totalBlocks: 100 });
    await startSync({ isTesting: true });

    const txScope = nock('https://mockweave.net')
      .get(/\/tx\/[^\/]*$/)
      .reply(
        200,
        (data) => (console.error(data) as any) || 'Internal server error'
      )
      .persist();

    const blockScope = nock('https://mockweave.net')
      .get(/\/block\/hash\/[^\/]*$/)
      .reply(
        200,
        (data) => (console.error(data) as any) || 'Internal server error'
      )
      .persist();

    const hashListScope = nock('https://mockweave.net')
      .get('/hash_list')
      .reply(
        200,
        (data) => (console.error(data) as any) || 'Internal server error'
      )
      .persist();

    // console.log(txScope, blockScope, hashListScope);
    expect(1).toEqual(2);
  });
});
