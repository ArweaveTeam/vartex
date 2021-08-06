import 'colors';
import express, { Express, Request, Response } from 'express';
import gpmeImport from 'graphql-playground-middleware-express';
import { config } from 'dotenv';
import cors from 'cors';
import { jsonMiddleware } from './middleware/json.middleware.js';
import { log } from './utility/log.utility.js';
import { graphServer } from './graphql/server.graphql.js';
import {
  blockCurrentRoute,
  blockByHashRoute,
  blockByHeightRoute,
} from './route/block.route.js';
import { statusRoute } from './route/status.route.js';
import {
  txOffsetRoute,
  txUploadRoute,
  txGetByIdRoute,
} from './route/transaction.route.js';
import { proxyGetRoute, proxyPostRoute } from './route/proxy.route.js';
// import { dataRouteRegex, dataRoute } from './route/data.route.js';
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
  app.use(koiLogger.logger);

  app.get('/', statusRoute);
  app.get('/status', statusRoute);
  app.get('/hash_list', hashListRoute);

  // app.get(dataRouteRegex, dataRoute);

  app.get('/tx/:id/offset', txOffsetRoute);
  app.use('/tx/:id/status', proxyGetRoute);
  app.get('/tx/:id', txGetByIdRoute);

  app.get('/peers', peerRoute);
  app.get('/logs', koiLogsRoute);
  app.get('/logs/raw', koiLogsRawRoute);
  app.get('/tx_anchor', proxyGetRoute);

  // db endpoints
  app.get(`/block/height/:height`, blockByHeightRoute);
  app.get(`/block/hash/:hash`, blockByHashRoute);
  app.get(`/block/current`, blockCurrentRoute);
  app.get('/:id', proxyGetRoute);

  app.post(`/tx`, proxyPostRoute);
  app.post('/chunk', proxyPostRoute);
  app.post(`/wallet`, proxyPostRoute);
  app.post(`/unsigned_tx`, proxyPostRoute);
  app.post(`/api`, proxyPostRoute);
  app.get(/\/price.*/, proxyGetRoute);
  app.get(/\/wallet.*/, proxyGetRoute);
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
      startSync({ isTesting: process.env.NODE_ENV === 'test' });
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
