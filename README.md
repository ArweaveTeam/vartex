## Status: beta (updated [06/08/2021])

We are in beta! Please give the this new gateway a spin and let us know what you think on your ArweaveDev Discord.
* :ghost: If you encounter issue please open a ticket here and we'll try to respond asap!
* :gift: If feel this code needs improvement then please open a PR.
* :pray: If a feature you'd like to see is missing, then open a feature request ticket and let's discuss it!


This project is a re-fork of the current arweave gateway, aimed to enable anyone to (relatively) quickly setup their own gateways
in order to query, transact and receive data from nodes and miners.

# Arweave Gateway

![License](https://img.shields.io/badge/license-MIT-blue.svg)

## Running with Docker Compose
The easiest way to start with your own gateway is by running the service with docker-compose.
### Requirements
- Docker
- Docker Compose

### Running
Clone this repo with:
```bash
git clone https://github.com/ArweaveTeam/gateway-cassandra.git
```

Go into this directory:
```bash
cd gateway-cassandra
```

Run the docker-compose command:
```bash
docker-compose up -d
```

Wait a couple of minutes and then you can see your gateway running on:
```bash
http://localhost:3000/graphql
```

## Run without Docker
You can also run the gateway without using Docker.
### Requirements
- NodeJS v16.4.0 or higher
- Cassandra v3.11.10 or higher

### Environment

By default, there is a default environment you can use located at `.env.example` in the repository.


Make sure you copy this configuration to `.env`.

```bash
cp .env.example .env
```

### Compilation

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

### Endpoints

You can test if the server and the GraphQL queries are working properly by navigating to.

```bash
http://localhost:3000/graphql
```

This webpage should look similar to.

```bash
https://arweave.dev/graphql
```
