# Dear arweaver: this gateway-fork is a work in progress
Please don't attempt to use at this point in time! :pray:

# Arweave Gateway

![License](https://img.shields.io/badge/license-MIT-blue.svg)

## Environment

By default, there is a default environment you can use located at `.env.example` in the repository.


Make sure you copy this configuration to `.env`.

```bash
cp .env.example .env
```

## Compilation

```bash
# with npm
npm run start

# with yarn
yarn start
```

While developing you can specify a linear range of blocks you wish to sync by specifying

```bash
DEVELOPMENT_SYNC_START={{start point from 0 to lastBlock - 1}}
DEVELOPMENT_SYNC_END={{start point from 1 to lastBlock}}
```

## Endpoints

You can test if the server and the GraphQL queries are working properly by navigating to.

```bash
http://localhost:3000/graphql
```

This webpage should look similar to.

```bash
https://arweave.dev/graphql
```
