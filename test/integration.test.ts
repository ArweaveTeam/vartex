import * as R from 'rambda';
import { exists as existsOrig } from 'fs';
import fs from 'fs/promises';
import { jest } from '@jest/globals';
import util from 'util';
import got from 'got';
import nock from 'nock';
import net from 'net';
import * as helpers from './helpers';

const exists = util.promisify(existsOrig);

const mockBlocks = helpers.generateMockBlocks({ totalBlocks: 100 });

describe('integration suite', function () {
  jest.setTimeout(60000);
  beforeAll(async function () {
    await helpers.waitForCassandra();
  });

  afterAll(() => {
    nock.abortPendingRequests();
    nock.cleanAll();
    // signalHook();
  });

  afterEach(() => {
    // togglePause();
  });
  beforeEach(async () => {
    jest.resetModules();
    nock.disableNetConnect();
    jest.setTimeout(60000);
    await helpers.nuke();
    await helpers.initDb();
    if (await exists('./cache/hash_list_test.json')) {
      await fs.unlink('./cache/hash_list_test.json');
    }

    nock('https://mockweave.net')
      .get('/hash_list')
      .query(() => {
        return true;
      })
      .reply(200, (data) => R.pluck('indep_hash', mockBlocks));

    nock('https://mockweave.net')
      .get(/\/tx\/[^\/]*$/)
      .query(() => {
        return true;
      })
      .reply(200, () => 'Internal server error');

    nock('https://mockweave.net')
      .get(/\/block\/hash\/[^\/]*$/)
      .query(() => {
        return true;
      })
      .reply(200, () => 'Internal server error');
    // togglePause();
  });

  test('it writes blocks into cassandra', async () => {
    // await startSync({ isTesting: true });
    helpers.startGateway();
    await new Promise((resolve) => setTimeout(resolve, 10000));
    // const hashListScope =

    // hashListScope.activate();
    // console.log(txScope, blockScope, hashListScope);
    expect(1).toEqual(2);
  });
});
