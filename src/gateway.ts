import realFs from "node:fs";
import gracefulFs from "graceful-fs";
gracefulFs.gracefulify(realFs);
import "colors";
import path from "node:path";
import exitHook from "exit-hook";
import killPort from "kill-port";
import express, { Express, Request, Response } from "express";
import gpmeImport from "graphql-playground-middleware-express";
import { config } from "dotenv";
import cors from "cors";
import { log } from "./utility/log";
import { graphServer } from "./graphql/server";
import {
  blockCurrentRoute,
  blockByHashRoute,
  blockByHeightRoute,
} from "./route/block";
import { statusRoute } from "./route/status";
import {
  txOffsetRoute,
  txUploadRoute,
  txGetByIdRoute,
} from "./route/transaction";
import { proxyGetRoute, proxyPostRoute } from "./route/proxy";
import { hashListRoute } from "./route/hash-list";
import { koiLogger, koiLogsRoute, koiLogsRawRoute } from "./route/koi";
import { startSync } from "./database/sync";

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
const { default: expressPlayground } = gpmeImport as any;

const dotenvPath = path.resolve("../.env");
const dotenvPathFallback = path.resolve("../.env.example");

if (realFs.existsSync(dotenvPath)) {
  config({ path: dotenvPath });
} else {
  config({ path: dotenvPathFallback });
}

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
  app.get(/\/[\w-]{43}/i, proxyGetRoute);

  // graphql endpoints
  const graphqlServer = graphServer({ introspection: true });
  graphqlServer.start().then(() => {
    /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
    (graphqlServer as any).applyMiddleware({
      app,
      path: "/graphql",
      subscriptionEndpoint: "/graphql",
      disableHealthCheck: true,
      schemaPolling: false,
    });

    // Everything else
    app.all("*", (request: Request, response: Response) => {
      response.status(400).json({
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
    log.info(`[app] - Parallel imports: ${process.env.PARALLEL_IMPORTS}`);
    log.info(`[app] - Parallel workers: ${process.env.PARALLEL_WORKERS}`);
    log.info(
      `[app] - Nodes: ${JSON.parse(process.env.ARWEAVE_NODES).join(", ")}`
    );
  });
}

exitHook(() => killPort(process.env.PORT || 1248));

start();
