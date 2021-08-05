import got from 'got';
import nock from 'nock';

describe('integration suite', () => {
  beforeEach(() => {
    jest.resetModules(); // Most important - it clears the cache
    process.env = { ...OLD_ENV }; // Make a copy
  });

  // afterAll(() => {
  //   process.env = OLD_ENV;
  // });

  test('', () => {});
});
