import net from 'net';
import path from 'path';
import { fork } from 'child_process';

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
    let process = fork(path.resolve('./', 'cassandra/init.cjs'));

    // listen for errors as they may prevent the exit event from firing
    process.on('error', function (err) {
      if (invoked) return;
      invoked = true;
      reject((err || '').toString());
    });

    // execute the callback once the process has finished running
    process.on('exit', function (code) {
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
    let process = fork(path.resolve('./', 'cassandra/nuke.cjs'));

    // listen for errors as they may prevent the exit event from firing
    process.on('error', function (err) {
      if (invoked) return;
      invoked = true;
      reject((err || '').toString());
    });

    // execute the callback once the process has finished running
    process.on('exit', function (code) {
      if (invoked) return;
      invoked = true;
      var err = code === 0 ? null : new Error('exit code ' + code);
      resolve((err || '').toString());
    });
  });
}
