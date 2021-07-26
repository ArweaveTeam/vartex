import 'colors';
import express, { Express, Request, Response } from 'express';
import { renderPlaygroundPage } from 'graphql-playground-html';
import expressPlayground from 'graphql-playground-middleware-express';
import { config } from 'dotenv';
import cors from 'cors';
import { jsonMiddleware } from './middleware/json.middleware';
import { logMiddleware } from './middleware/log.middleware';
import { manifestMiddleware } from './middleware/manifest.middleware';
import { log } from './utility/log.utility';
import {
  sessionMiddleware,
  sessionPinningMiddleware,
} from './utility/session.utility';
import { graphServer } from './graphql/server.graphql';
import { blockByHeightRoute } from './route/block.route';
import { statusRoute } from './route/status.route';
import { txUploadRoute, txGetByIdRoute } from './route/transaction.route';
import { proxyRoute } from './route/proxy.route';
import { dataRouteRegex, dataRoute } from './route/data.route';
import { peerRoute } from './route/peer.route';
import { koiLogger, koiLogsRoute, koiLogsRawRoute } from './route/koi.route';
import { findPeers } from './query/node.query';
import { startSync } from './database/sync.database';

config();

export const app: Express = express();

export function start(): void {
  app.set('trust proxy', 1);

  app.use(cors());
  app.use(jsonMiddleware);
  app.use(logMiddleware);
  app.use(sessionMiddleware);
  app.use(sessionPinningMiddleware);
  app.use(koiLogger.logger);
  app.use(manifestMiddleware);

  app.get('/', statusRoute);
  app.get('/status', statusRoute);

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
      graphqlServer.applyMiddleware({
        app,
        path: '/graphql',
        disableHealthCheck: true,
      });
      startSync();
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
