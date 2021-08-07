import { readFileSync } from "fs";
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
    opts: ApolloServerExpressConfig = {},
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
    context: (ctx) => {
      return {
        req: ctx.req,
        conection: {},
        // connection,
      };
    },
    ...opts,
  });
  return graphServer;
}
