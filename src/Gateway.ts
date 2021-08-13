import "colors";
import exitHook from "exit-hook";
import killPort from "kill-port";
import express, { Express, Request, Response } from "express";
import gpmeImport from "graphql-playground-middleware-express";
import { config } from "dotenv";
import cors from "cors";
import { jsonMiddleware } from "./middleware/json.middleware.js";
import { log } from "./utility/log.utility.js";
import { graphServer } from "./graphql/server.graphql.js";
import {
  blockCurrentRoute,
  blockByHashRoute,
  blockByHeightRoute,
} from "./route/block.route.js";
import { statusRoute } from "./route/status.route.js";
import {
  txOffsetRoute,
  txUploadRoute,
  txGetByIdRoute,
} from "./route/transaction.route.js";
import { proxyGetRoute, proxyPostRoute } from "./route/proxy.route.js";
import { hashListRoute } from "./route/hash-list.route.js";
import { koiLogger, koiLogsRoute, koiLogsRawRoute } from "./route/koi.route.js";
import { startSync } from "./database/sync.database.js";
import { newDataItem, verifySender } from "./route/bundler.route";

const { default: expressPlayground } = gpmeImport as any;

config();

export const app: Express = express();

export function start(): void {
  app.set("trust proxy", 1);

  app.use(cors());
  // app.use(jsonMiddleware);
  app.use(koiLogger.logger);

  app.get("/", statusRoute);
  app.get("/status", statusRoute);
  app.get("/info", proxyGetRoute);
  app.get("/hash_list", hashListRoute);

  app.get("/tx/:id/offset", txOffsetRoute);
  app.use("/tx/:id/status", proxyGetRoute);
  app.get("/tx/:id", txGetByIdRoute);

  app.get("/peers", proxyGetRoute);
  app.get("/logs", koiLogsRoute);
  app.get("/logs/raw", koiLogsRawRoute);
  app.get("/tx_anchor", proxyGetRoute);

  // db endpoints
  app.get("/block/height/:height", blockByHeightRoute);
  app.get("/block/hash/:hash", blockByHashRoute);
  app.get("/block/current", blockCurrentRoute);

  app.post("/tx", txUploadRoute);
  app.post("/chunk", proxyPostRoute);
  app.post("/wallet", proxyPostRoute);
  app.post("/unsigned_tx", proxyPostRoute);
  app.post("/api", proxyPostRoute);
  app.get(/\/price.*/, proxyGetRoute);
  app.get(/\/wallet.*/, proxyGetRoute);
  app.get(/\/[a-z0-9_-]{43}/i, proxyGetRoute);

  app.post("/bundler/tx", verifySender, newDataItem)

  // graphql endpoints
  const graphqlServer = graphServer({ introspection: true });
  Promise.all([graphqlServer.start()]).then(() => {
    (graphqlServer as any).applyMiddleware({
      app,
      path: "/graphql",
      subscriptionEndpoint: "/graphql",
      disableHealthCheck: true,
      schemaPolling: false,
    });

    // Everything else
    app.all("*", (req: Request, res: Response) => {
      res.status(400).json({
        status: 400,
        error: "Not Found",
      });
    });

    startSync({ isTesting: process.env.NODE_ENV === "test" });
  });

  app.get(
    "/graphql",
    expressPlayground({
      endpoint: "/graphql",
    })
  );

  app.listen(process.env.PORT || 1248, () => {
    log.info(`[app] Started on http://localhost:${process.env.PORT || 1248}`);
    log.info(`[app] - Parallel: ${process.env.PARALLEL}`);
    log.info(
      `[app] - Nodes: ${JSON.parse(process.env.ARWEAVE_NODES).join(", ")}`
    );
  });
}

exitHook(() => killPort(process.env.PORT || 1248));

start();
