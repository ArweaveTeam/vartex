import * as R from 'rambda';
import net from 'net';
import path from 'path';
import child_process, { fork } from 'child_process';
import { testEnvVars } from './setup';

export function waitForCassandra(): Promise<void> {
  return new Promise((resolve, reject) => {
    const maxRetry = 100;
    let rtry = 0;
    // Wait until cassandra is reachable
    const retry = () => {
      let client = net
        .createConnection(9042, '127.0.0.1')
        .on('error', function (error: string) {
          rtry += 1;
          if (rtry < maxRetry) {
            new Promise((resolveRetry) => setTimeout(resolveRetry, 100)).then(
              retry
            );
          } else {
            throw new Error(
              "Couldn't find cassandra running after 100 retries: " + error
            );
            reject();
          }
        })
        .on('connect', function () {
          try {
            client.destroy();
          } catch (error) {}
          resolve();
        });
    };
    retry();
  });
}

export function initDb(): Promise<string> {
  return new Promise((resolve, reject) => {
    let invoked = false;
    let forkps = fork(path.resolve('./', 'cassandra/init.cjs'), {
      env: process.env,
    });

    // listen for errors as they may prevent the exit event from firing
    forkps.on('error', function (err) {
      if (invoked) return;
      invoked = true;
      reject((err || '').toString());
    });

    // execute the callback once the forkps has finished running
    forkps.on('exit', function (code) {
      if (invoked) return;
      invoked = true;
      var err = code === 0 ? null : new Error('exit code ' + code);
      resolve((err || '').toString());
    });
  });
}

export function nuke(): Promise<string> {
  return new Promise((resolve, reject) => {
    let invoked = false;
    let forkps = fork(path.resolve('./', 'cassandra/nuke.cjs'), {
      env: process.env,
    });

    // listen for errors as they may prevent the exit event from firing
    forkps.on('error', function (err) {
      if (invoked) return;
      invoked = true;
      reject((err || '').toString());
    });

    // execute the callback once the forkps has finished running
    forkps.on('exit', function (code) {
      if (invoked) return;
      invoked = true;
      var err = code === 0 ? null : new Error('exit code ' + code);
      resolve((err || '').toString());
    });
  });
}

export function generateMockBlocks({ totalBlocks }) {
  const template = {
    nonce: 'n1',
    previous_block: 'p0',
    timestamp: 1,
    last_retarget: 1,
    diff: '1111',
    height: 0,
    hash: '_____x',
    indep_hash: 'x1',
    txs: [],
    tx_root: 'root1',
    wallet_list: 'wl1',
    reward_addr: 'xyz1',
    tags: [],
    reward_pool: '123',
    weave_size: '123',
    block_size: '123',
    cumulative_diff: '123',
    hash_list_merkle: 'xxx',
    poa: {
      option: '1',
      tx_path: 'txp1',
      data_path: 'dp1',
      chunk: 'ch1',
    },
  };

  const blockHeights = R.range(0, totalBlocks);
  return blockHeights.map((height) =>
    R.pipe(
      R.assoc('height', height),
      R.assoc('indep_hash', `x${height}`)
    )(template)
  );
}

export function startGateway(): any {
  return child_process.spawn(
    'node',
    [
      '--experimental-specifier-resolution=node',
      '--max-old-space-size=4096',
      '--loader=ts-node/esm.mjs',
      'src/Gateway.ts',
    ],
    {
      env: testEnvVars,
      shell: true,
      stdio: 'inherit',
    }
  );
}
