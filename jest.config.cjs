/** @type {import('@ts-jest/dist/types').InitialOptionsTsJest} */
module.exports = {
  rootDir: __dirname,
  preset: 'ts-jest/presets/js-with-babel-esm', // 'ts-jest',
  testEnvironment: 'jest-environment-node',
  // transformIgnorePatterns: [
  //   'node_modules/(?!p-wait-for/)',
  //   'node_modules/(?!nock/)',
  // ],
  // transform: {
  //   '^src\\/.+\\.jsx?$': 'babel-jest',
  //   '^src\\/.+\\.tsx?$': 'ts-jest',
  //   '^test\\/.+\\.jsx?$': 'babel-jest',
  //   '^test\\/.+\\.tsx?$': 'ts-jest',
  // },

  globals: {
    'ts-jest': {
      // babelConfig: true,
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
