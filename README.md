# Dear arweaver: this gateway-fork is a work in progress
Please don't attempt to use at this point in time! :pray:

# Arweave Gateway

![License](https://img.shields.io/badge/license-MIT-blue.svg)

## Prerequisites
- NodeJS v16.4.0 or higher
- Cassandra v3.11.10 or higher

## Environment

By default, there is a default environment you can use located at `.env.example` in the repository.


Make sure you copy this configuration to `.env`.

```bash
cp .env.example .env
```

## Compilation

Start Cassandra and then run the following command to compile the gateway.

```bash
# with npm
npm run start

# with yarn
yarn start
```

While developing you can specify a range of blocks you wish to sync,
the range starts from the most recent known block from the cached hash_list
down X amount of blocks specified with DEVELOPMENT_SYNC_LENGTH.

```bash
# for cached block height 1,000,000 would only sync down to 999,900
DEVELOPMENT_SYNC_LENGTH=100
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

**Note:** The docker files are still in progress and are unlikely to work at this moment.