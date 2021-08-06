/** @type {import('@ts-jest/dist/types').InitialOptionsTsJest} */
module.exports = {
  rootDir: __dirname,
  preset: 'ts-jest/presets/js-with-babel-esm', // 'ts-jest',
  testEnvironment: 'jest-environment-node',
  setupFilesAfterEnv: ['./test/setup.ts'],

  globals: {
    'ts-jest': {
      useESM: true,
      tsConfig: {
        allowSyntheticDefaultImports: true,
        target: 'es2019',
        esModuleInterop: true,
        skipLibCheck: true,
      },
    },
  },
};
