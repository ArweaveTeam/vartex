import 'colors';
import express, { Express, Request, Response } from 'express';
import gpmeImport from 'graphql-playground-middleware-express';
import { config } from 'dotenv';
import cors from 'cors';
import { jsonMiddleware } from './middleware/json.middleware.js';
import { manifestMiddleware } from './middleware/manifest.middleware.js';
import { log } from './utility/log.utility.js';
import {
  sessionMiddleware,
  sessionPinningMiddleware,
} from './utility/session.utility.js';
import { graphServer } from './graphql/server.graphql.js';
import { blockByHeightRoute } from './route/block.route.js';
import { statusRoute } from './route/status.route.js';
import { txUploadRoute, txGetByIdRoute } from './route/transaction.route.js';
import { proxyRoute } from './route/proxy.route.js';
import { dataRouteRegex, dataRoute } from './route/data.route.js';
import { peerRoute } from './route/peer.route.js';
import { hashListRoute } from './route/hash-list.route.js';
import { koiLogger, koiLogsRoute, koiLogsRawRoute } from './route/koi.route.js';
import { findPeers } from './query/node.query.js';
import { startSync } from './database/sync.database.js';

const { default: expressPlayground } = gpmeImport as any;

config();

export const app: Express = express();

export function start(): void {
  app.set('trust proxy', 1);

  app.use(cors());
  app.use(jsonMiddleware);
  app.use(sessionMiddleware);
  app.use(sessionPinningMiddleware);
  app.use(koiLogger.logger);
  app.use(manifestMiddleware);

  app.get('/', statusRoute);
  app.get('/status', statusRoute);
  app.get('/hash_list', hashListRoute);

  app.get(dataRouteRegex, dataRoute);

  app.post('/tx', txUploadRoute);
  app.get('/tx/:id', txGetByIdRoute);

  app.get('/peers', peerRoute);
  app.get('/logs', koiLogsRoute);
  app.get('/logs/raw', koiLogsRawRoute);

  // db endpoints
  app.get(`/block/height/:height`, blockByHeightRoute);

  // app.all('*', proxyRoute);

  app.listen(process.env.PORT || 3000, () => {
    log.info(`[app] started on http://localhost:${process.env.PORT || 3000}`);
    const graphqlServer = graphServer({ introspection: true });
    Promise.all([findPeers(), graphqlServer.start()]).then(() => {
      (graphqlServer as any).applyMiddleware({
        app,
        path: '/graphql',
        subscriptionEndpoint: '/graphql',
        disableHealthCheck: true,
        schemaPolling: false,
      });
      startSync({});
    });
    app.get(
      '/graphql',
      expressPlayground({
        endpoint: '/graphql',
      })
    );
  });
}

start();
