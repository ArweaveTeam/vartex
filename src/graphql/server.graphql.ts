import { readFileSync } from "node:fs";
import {
  ApolloServer,
  ApolloServerExpressConfig,
  ExpressContext,
  gql,
} from "apollo-server-express";
// import {connection} from '../database/connection.database';
import { resolvers } from "./resolver.graphql.js";

const typeDefs = gql(readFileSync(`${process.cwd()}/types.graphql`, "utf8"));

export function graphServer(
    options: ApolloServerExpressConfig = {},
): ApolloServer<ExpressContext> {
  const graphServer = new (ApolloServer as any)({
    typeDefs,
    resolvers,
    debug: true,
    playground: {
      settings: {
        "schema.polling.enable": false,
        "request.credentials": "include",
      },
    },
    context: (context) => {
      return {
        req: context.req,
        conection: {},
        // connection,
      };
    },
    ...options,
  });
  return graphServer;
}
