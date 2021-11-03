import "colors";
import exitHook from "exit-hook";
import killPort from "kill-port";
import express, { Express, Request, Response } from "express";
import gpmeImport from "graphql-playground-middleware-express";
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
import { env, isGatewayNodeModeEnabled } from "./constants";

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
const { default: expressPlayground } = gpmeImport as any;

export const session: { uuid: CassandraTypes.TimeUuid } = {
  uuid: CassandraTypes.TimeUuid.fromDate(new Date()),
};

export const app: Express = express();

function poweredBy(r: Request, response: Response, next: () => void) {
  response.setHeader("X-Powered-By", "Vartex");
  if (next) {
    next();
  }
}

app.enable("strict routing");

app.use(poweredBy);

// app.use(expressSlash());

const dataPathRegex = new RegExp(/^\/?([\w-]{43})\/?$|^\/?([\w-]{43})\/(.*)$/i);

// 1. redirect domain.com/:txid -> txid.domain.org
// 2. route txid.domain.org -> domain.com/:txid
function permawebSandboxMiddleware(
  request: Partial<Request & { txid?: string }>,
  response: Response,
  next: () => void
) {
  if (
    request.subdomains &&
    request.subdomains.length > 0 &&
    /[\w-]{43}/.test(request.subdomains[0])
  ) {
    request.txid = request.subdomains[0];
    dataRoute(request, response, next);
    return;
  } else if (
    request.originalUrl.replace(/^\//, "").replace(/\/.*/, "").length === 43
  ) {
    const requestPath = request.originalUrl.replace(/^\//, "");
    const requestTxId = requestPath.replace(/\/.*/, "");
    const requestSubPath = requestPath.replace(/.*\//, "");
    const query = request.url.slice(request.path.length);
    response.redirect(
      302,
      `${request.protocol}://${requestTxId}.${
        request.host.endsWith(":80") || request.host.endsWith(":443")
          ? request.hostname
          : request.host
      }/${requestSubPath}${query}`
    );
    return;
  } else {
    next && next();
    return;
  }
}

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
  if (request.path.length === 44 && request.path.slice(-1) !== "/") {
    const query = request.url.slice(request.path.length);
    response.redirect(302, `${request.path}/${query}`);
    return;
  } else if (/\/\//.test(request.path)) {
    // prevent double slashes
    const query = request.url.slice(request.path.length);
    const cleanPath = request.path.replace(/\/\//g, "/");
    response.redirect(302, `${cleanPath === "/" ? "" : cleanPath}/${query}`);
    return;
  } else {
    next();
    return;
  }
}

app.use(permawebSandboxMiddleware);

app.use(appendSlashMiddleware);

export function start(): void {
  app.set("trust proxy", 1);
  app.set("query parser", "simple");

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
        startSync({ session });

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

  app.listen(env.PORT || 1248, () => {
    log.info(`[app] Started on http://localhost:${env.PORT || 1248}`);
    if (isGatewayNodeModeEnabled) {
      log.info(
        `[app] - Gateway only node (no imports of new blocks will be performed)`
      );
    } else {
      log.info(
        `[app] - Parallel import of blocks: ${env.PARALLEL_BLOCK_IMPORT}`
      );
      if (env.OFFLOAD_TX_IMPORT) {
        log.info(
          `[app] - Imports of incoming transactions is offloaded\n` +
            `make sure you are running import txs elsewhere, otherwise the tx import queue will grow big fast!`
        );
      } else {
        log.info(
          `[app] - Parallel import of incoming transactions: ${env.PARALLEL_TX_IMPORT}`
        );
      }

      if (env.OFFLOAD_MANIFEST_IMPORT) {
        log.info(
          `[app] - Imports of incoming manifests is offloaded\n` +
            `make sure you are running import manifests elsewhere if you indend to import them!`
        );
      } else {
        log.info(
          `[app] - Parallel import of manifests: ${env.PARALLEL_MANIFEST_IMPORT}`
        );
      }

      if (env.OFFLOAD_ANS102_IMPORT) {
        log.info(
          `[app] - Imports of incoming ANS-102 bundles is offloaded\n` +
            `make sure you are running import ans102 elsewhere if you indend to import them!`
        );
      } else {
        log.info(
          `[app] - Parallel import of ANS102 bundles: ${env.PARALLEL_ANS102_IMPORT}`
        );
      }

      if (env.OFFLOAD_ANS104_IMPORT) {
        log.info(
          `[app] - Imports of incoming ANS-104 bundles is offloaded\n` +
            `make sure you are running import ans104 elsewhere if you indend to import them!`
        );
      } else {
        log.info(
          `[app] - Parallel import of ANS104 bundles: ${env.PARALLEL_ANS104_IMPORT}`
        );
      }

      log.info(
        `[app] - Your upstream peers are: ${env.ARWEAVE_NODES.join(", ")}`
      );
    }
  });
}

exitHook(() => killPort(env.PORT || 1248));

start();
