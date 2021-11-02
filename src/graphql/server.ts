import { readFileSync } from "node:fs";
import { ApolloServerPluginLandingPageDisabled } from "apollo-server-core";
import {
  ApolloServer,
  ApolloServerExpressConfig,
  ExpressContext,
  gql,
} from "apollo-server-express";
import { resolvers } from "./resolver";

const typeDefs = gql(readFileSync(`${process.cwd()}/types.graphql`, "utf8"));

export function graphServer(
  options: ApolloServerExpressConfig = {}
): ApolloServer<ExpressContext> {
  const graphServer = new ApolloServer({
    typeDefs,
    resolvers,
    debug: false,
    plugins: [ApolloServerPluginLandingPageDisabled()],
    context: (context) => {
      return {
        req: context.req,
        conection: {},
        tracing: false,
        // connection,
      };
    },
    ...options,
  });
  return graphServer;
}
