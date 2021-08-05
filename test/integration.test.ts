import got from 'got';
import nock from 'nock';
import net from 'net';

describe('integration suite', function () {
  jest.setTimeout(60000);
  beforeAll(function () {
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
            resolve(true);
          });
      };
      retry();
    });
  });
  beforeEach(() => {
    jest.resetModules();
    // process.env = { ...OLD_ENV };
  });

  // afterAll(() => {
  //   process.env = OLD_ENV;
  // });

  test('', () => expect(1).toEqual(2));
});
