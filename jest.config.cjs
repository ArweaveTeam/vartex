/** @type {import('@ts-jest/dist/types').InitialOptionsTsJest} */
module.exports = {
  rootDir: __dirname,
  preset: "ts-jest/presets/js-with-babel-esm",
  testEnvironment: "jest-environment-node",
  setupFilesAfterEnv: ["./test/setup.ts"],
  moduleNameMapper: {
    "^node:(.*)$": "$1",
    "^graphql$": "<rootDir>/node_modules/graphql/index.js",
    "/(.*).d.ts$": "<rootDir>/$1.d.ts",
  },
  roots: ["<rootDir>/src", "<rootDir>/test", "<rootDir>/src/graphql"],
  globals: {
    "ts-jest": {
      useESM: true,
      tsconfig: {
        allowSyntheticDefaultImports: true,
        target: "ESNext",
        esModuleInterop: true,
        skipLibCheck: true,
        moduleResolution: "node",
      },
    },
  },
};
