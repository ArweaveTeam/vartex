# Arweave Gateway

![License](https://img.shields.io/badge/license-MIT-blue.svg)

Review the [documentation](https://arweaveteam.github.io/gateway/#/) to learn more about setting up and deploying a Gateway.

## Requirements

1. A Unix OS

2. Docker or Cassandra 4.x

### Suggested Hardware

There are several million transactions on the Arweave chain. In order to effectively serve content on the gateway you'll need a decent sized computer. The ideal specs for a Gateway should have the following:

1. 16GB RAM (ideally 32GB RAM)

2. ~1TB of SSD storage available

3. Intel i5 / AMD FX or greater, +4 vCPUs should be more than enough, these are typically Intel Xeon CPUs.

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

## Endpoints

You can test if the server and the GraphQL queries are working properly by navigating to.

```bash
http://localhost:3000/graphql
```

This webpage should look similar to.

```bash
https://arweave.dev/graphql
```
