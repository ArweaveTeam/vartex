import * as R from 'rambda';
import cassandra, { types as CassandraTypes } from 'cassandra-driver';
import { exists as existsOrig } from 'fs';
import fs from 'fs/promises';
import { jest } from '@jest/globals';
import util from 'util';
import got from 'got';
import express from 'express';
import killPort from 'kill-port';
import * as helpers from './helpers';

const PORT = 12345;

const exists = util.promisify(existsOrig);

let mockBlocks: any[] = helpers.generateMockBlocks({ totalBlocks: 100 });

const lastBlock: any = {
  current: '',
  height: -1,
};

const tmpNextBlock: any = R.last(mockBlocks);

lastBlock['height'] = tmpNextBlock.height;
lastBlock['current'] = tmpNextBlock.indep_hash;

let app: any;
let srv: any;
let proc: any;
let client: any;

describe('integration suite', function () {
  jest.setTimeout(60000);
  beforeAll(async function () {
    await helpers.waitForCassandra();
    client = new cassandra.Client({
      contactPoints: ['localhost:9042'],
      localDataCenter: 'datacenter1',
    });
    app = express();
    app.get('/hash_list', function (req, res) {
      res.status(200).json(R.reverse(R.pluck('indep_hash', mockBlocks)));
    });

    app.get('/info', function (req, res) {
      res.status(200).json(lastBlock);
    });

    app.get('/block/height/:id', function (req, res) {
      const match = R.find(R.propEq('height', parseInt(req.params.id)))(
        mockBlocks
      );
      if (match) {
        res.status(200).json(match);
      } else {
        res.status(404);
      }
    });

    app.get('/block/hash/:id', function (req, res) {
      const match = R.find(R.propEq('indep_hash', req.params.id))(mockBlocks);
      // console.error(req.params.id, req.params.id, match);
      if (match) {
        res.status(200).json(match);
      } else {
        res.status(404);
      }
    });

    app.get('*', function (req, res) {
      console.error(req);
      res.status(404);
      // res.status(200).json(R.pluck('indep_hash', mockBlocks));
    });

    srv = app.listen(PORT);
  });

  afterAll(async () => {
    srv && srv.close();
    if (proc) {
      proc.kill('SIGINT');
      proc = undefined;
    }
    // wait a second for handlers to close
    await new Promise((resolve) => setTimeout(resolve, 1000));
  });

  afterEach(async () => {
    // togglePause();
    if (proc) {
      proc.kill('SIGINT');
      proc = undefined;
    }

    await killPort(3000, 'tcp');
    await new Promise((resolve) => setTimeout(resolve, 1000));
  });
  beforeEach(async () => {
    jest.resetModules();
    jest.setTimeout(60000);
  });

  test('it writes 100 blocks into cassandra', async () => {
    await helpers.nuke();
    await helpers.initDb();
    if (await exists('./cache/hash_list_test.json')) {
      await fs.unlink('./cache/hash_list_test.json');
    }

    let logs = '';
    let fullySyncPromiseResolve: any;
    proc = helpers.startGateway();
    proc.stdout.on('data', (log: string) => {
      if (
        /Database fully in sync/g.test(log.toString()) &&
        fullySyncPromiseResolve
      ) {
        fullySyncPromiseResolve();
        fullySyncPromiseResolve = undefined;
      }
      process.stderr.write(log);
      logs += log.toString();
    });
    await new Promise((resolve, reject) => {
      fullySyncPromiseResolve = resolve;
    });
    // from last http to last written row needs some time to get the queue empty
    await new Promise((resolve) => setTimeout(resolve, 5000));

    const queryResponse = await client.execute(
      'SELECT COUNT(*) FROM testway.block ALLOW FILTERING'
    );

    expect(queryResponse.rows[0].count.toString()).toEqual('100');
  });

  test('it detects correctly fully synced db on startup', async () => {
    const seemsEmptyLog = jest.fn();
    const seemsFullLog = jest.fn();
    const missingBlockLog = jest.fn();

    let logs = '';
    let fullySyncPromiseResolve: any;
    proc = proc || helpers.startGateway();
    proc.stderr.on('data', process.stderr.write); //  (data) => console.error(data.toString())
    proc.stdout.on('data', (log: string) => {
      if (/fully synced db/g.test(log.toString()) && fullySyncPromiseResolve) {
        fullySyncPromiseResolve();
        seemsFullLog();
        fullySyncPromiseResolve = undefined;
      }

      if (/database seems to be empty/.test(log)) {
        seemsEmptyLog();
      }

      if (/Found missing block.*/.test(log)) {
        missingBlockLog();
      }
      process.stderr.write(log);
      logs += log.toString();
    });
    await new Promise((resolve, reject) => {
      fullySyncPromiseResolve = resolve;
    });

    // from last http to last written row needs some time to get the queue empty
    await new Promise((resolve) => setTimeout(resolve, 5000));

    const queryResponse = await client.execute(
      'SELECT COUNT(*) FROM testway.block ALLOW FILTERING'
    );

    expect(missingBlockLog).not.toHaveBeenCalled();
    expect(seemsEmptyLog).not.toHaveBeenCalled();
    expect(seemsFullLog).toHaveBeenCalledTimes(1);
  });

  test('it starts polling and receives new blocks', async () => {
    let logs = '';
    let fullySyncPromiseResolve: any;
    proc = proc || helpers.startGateway();
    proc.stderr.on('data', process.stderr.write); //  (data) => console.error(data.toString())
    proc.stdout.on('data', (log: string) => {
      if (/fully synced db/g.test(log.toString()) && fullySyncPromiseResolve) {
        fullySyncPromiseResolve();
        fullySyncPromiseResolve = undefined;
      }

      process.stderr.write(log);
      logs += log.toString();
    });

    await new Promise((resolve, reject) => {
      fullySyncPromiseResolve = resolve;
    });

    const nextBlock: any = helpers.generateMockBlocks({
      totalBlocks: 1,
      offset: 100,
    })[0];

    mockBlocks.push(nextBlock);

    lastBlock['height'] = nextBlock.height;
    lastBlock['current'] = nextBlock.indep_hash;

    await new Promise((resolve) => setTimeout(resolve, 12000));

    const queryResponse = await client.execute(
      'SELECT COUNT(*) FROM testway.block ALLOW FILTERING'
    );

    expect(queryResponse.rows[0].count.toString()).toEqual('101');
  });

  test('it recovers when fork changes', async () => {
    let logs = '';
    let fullySyncPromiseResolve: any;
    proc = proc || helpers.startGateway();
    proc.stderr.on('data', process.stderr.write); //  (data) => console.error(data.toString())
    proc.stdout.on('data', (log: string) => {
      if (/fully synced db/g.test(log.toString()) && fullySyncPromiseResolve) {
        fullySyncPromiseResolve();
        fullySyncPromiseResolve = undefined;
      }

      process.stderr.write(log);
      logs += log.toString();
    });

    await new Promise((resolve, reject) => {
      fullySyncPromiseResolve = resolve;
    });

    let nextFork: any[] = helpers.generateMockBlocks({
      totalBlocks: 15,
      offset: 89,
      hashPrefix: 'y',
    });

    mockBlocks = R.splitWhen(R.propEq('height', 90))(mockBlocks)[0];
    nextFork = R.concat(
      [
        R.assoc(
          'previous_block',
          R.last(mockBlocks).indep_hash,
          R.head(nextFork)
        ),
      ],
      R.slice(1, nextFork.length, nextFork)
    );
    mockBlocks = R.concat(mockBlocks, nextFork);

    lastBlock['height'] = R.last(mockBlocks).height;
    lastBlock['current'] = R.last(mockBlocks).indep_hash;

    await new Promise((resolve) => setTimeout(resolve, 40000));

    const queryResponse = await client.execute(
      'SELECT indep_hash,height FROM testway.block WHERE height>85 AND height<95 ALLOW FILTERING'
    );
    const result = queryResponse.rows.map((obj: any) => ({
      height: parseInt(obj.height),
      hash: obj.indep_hash,
    }));

    expect(R.findIndex(R.equals({ height: 86, hash: 'x86' }), result)).toEqual(
      1
    );
    expect(R.findIndex(R.equals({ height: 87, hash: 'x87' }), result)).toEqual(
      1
    );
    expect(R.findIndex(R.equals({ height: 88, hash: 'x88' }), result)).toEqual(
      1
    );
    expect(R.findIndex(R.equals({ height: 89, hash: 'x89' }), result)).toEqual(
      1
    );
    expect(R.findIndex(R.equals({ height: 90, hash: 'x90' }), result)).toEqual(
      1
    );
    expect(R.findIndex(R.equals({ height: 91, hash: 'x91' }), result)).toEqual(
      1
    );
    expect(R.findIndex(R.equals({ height: 92, hash: 'x92' }), result)).toEqual(
      1
    );
  });
});
