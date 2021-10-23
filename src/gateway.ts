import realFs from "node:fs";
import gracefulFs from "graceful-fs";
gracefulFs.gracefulify(realFs);
import "colors";
import path from "node:path";
import exitHook from "exit-hook";
import killPort from "kill-port";
import express, { Express, Request, Response } from "express";
import expressSlash from "express-trailing-slash";
import gpmeImport from "graphql-playground-middleware-express";
import { config } from "dotenv";
import cors from "cors";
import { log } from "./utility/log";
import { graphServer } from "./graphql/server";
import { dataRoute } from "./route/data";
import {
  blockCurrentRoute,
  blockByHashRoute,
  blockByHeightRoute,
} from "./route/block";
import { initializeStatusSession, statusRoute } from "./route/status";
import {
  txOffsetRoute,
  txUploadRoute,
  txGetByIdRoute,
} from "./route/transaction";
import { proxyGetRoute, proxyPostRoute } from "./route/proxy";
import { hashListRoute } from "./route/hash-list";
import { types as CassandraTypes } from "cassandra-driver";
import { cassandraClient } from "./database/cassandra";
import { startSync } from "./database/sync";
import { isGatewayNodeModeEnabled } from "./constants";

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
const { default: expressPlayground } = gpmeImport as any;

const dotenvPath = path.resolve("../.env");
const dotenvPathFallback = path.resolve("../.env.example");

if (realFs.existsSync(dotenvPath)) {
  config({ path: dotenvPath });
} else {
  config({ path: dotenvPathFallback });
}

export const session: { uuid: CassandraTypes.TimeUuid } = {
  uuid: CassandraTypes.TimeUuid.fromDate(new Date()),
};

export const app: Express = express();

function poweredBy(_: Request, response: Response, next: () => void) {
  response.setHeader("X-Powered-By", "Vartex");
  if (next) {
    next();
  }
}

app.enable("strict routing");

app.use(poweredBy);

// app.use(expressSlash());

const dataPathRegex = new RegExp(
  /^\/?([a-zA-Z0-9-_]{43})\/?$|^\/?([a-zA-Z0-9-_]{43})\/(.*)$/i
);

// lack of slash causes permaweb apps to fetch from root domain.com/
// and not domain.com/path/
function appendSlashMiddleware(
  request: Request,
  response: Response,
  next: () => void
) {
  const method = request.method.toLowerCase();

  // Skip when the req method is neither a GET nor a HEAD
  if (!["get", "head"].includes(method)) {
    next();
    return;
  }

  if (request.path.split("/").pop().includes(".")) {
    // Path has an extension. Do not add slash.
    next();
    return;
  }

  // 44 = / + txid
  if (request.path.length === 44 && request.path.substr(-1) !== "/") {
    const query = request.url.slice(request.path.length);
    response.redirect(301, `${request.path}/${query}`);
    return;
  } else {
    next();
    return;
  }
}

app.use(appendSlashMiddleware);

export function start(): void {
  app.set("trust proxy", 1);

  app.use(cors());
  // app.use(jsonMiddleware);

  app.get("/", statusRoute);
  app.get("/status", statusRoute);
  app.get("/info", proxyGetRoute);
  app.get("/hash_list", hashListRoute);

  app.get("/tx/:id/offset", txOffsetRoute);
  app.use("/tx/:id/status", proxyGetRoute);
  app.get("/tx/:id", txGetByIdRoute);

  app.get("/peers", proxyGetRoute);
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
  app.use(dataPathRegex, dataRoute);
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

    initializeStatusSession(cassandraClient, session.uuid).then(
      (sessionUuid: CassandraTypes.TimeUuid) => {
        session.uuid = sessionUuid;
        // just flush
        console.log("...");
        startSync({ session, isTesting: process.env.NODE_ENV === "test" });

        if (isGatewayNodeModeEnabled) {
          // recheck every minute if session changes
          setInterval(() => {
            initializeStatusSession(cassandraClient, session.uuid).then(
              (newSessionUuid: CassandraTypes.TimeUuid) => {
                session.uuid = newSessionUuid;
              }
            );
          }, 60 * 1000);
        }
      }
    );
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
