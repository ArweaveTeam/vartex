<img src="https://raw.githubusercontent.com/ArweaveTeam/vartex/main/docs/logo.png"></img>

![License](https://img.shields.io/badge/license-MIT-blue.svg)

Vartex is a vortex into the permaweb -- the decentralised web on top of the Arweave protocol. Vartex nodes offer services to permaweb users -- serving data and query requests to desktop and mobile browsers.

The service builds upon [Amplify](https://www.amplify.host), a fork of the original [Arweave.net](https://arweave.net/status) gateway service.

### Current Release: BETA-1

We are in beta! Please give Vartex a spin and let us know what you think via the [Arweave Developer Discord](https://discord.gg/BXk8tq7).

* :ghost: If you encounter issues, please open a ticket here and we will try to respond ASAP!
* :gift: If you feel this code needs improvement, please open a PR.
* :pray: If a feature you'd like to see is missing, open a feature request ticket and let's discuss it!


## Running with Docker Compose

The easiest way to start with your own gateway is by running the service with docker-compose.

### Requirements

- Docker
- Docker Compose

### Running

Clone this repo with:

```bash
git clone https://github.com/ArweaveTeam/vartex.git
```

Go into the directory:

```bash
cd gateway-cassandra
```

Copy the .env.example to .env, and change the `ARWEAVE_NODES` variable to the IP addresses of your Arweave [node](https://docs.arweave.org/info/mining/mining-guide).
```bash
cp .env.example .env
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
http://localhost:1248/graphql
```

This webpage should look similar to.

```bash
https://arweave.dev/graphql
```
