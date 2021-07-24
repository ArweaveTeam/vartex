import 'colors';
import express, { Express, Request, Response } from 'express';
import { renderPlaygroundPage } from 'graphql-playground-html';
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
import { transactionRoute } from './route/transaction.route';
import { syncRoute } from './route/sync.route';
import { proxyRoute } from './route/proxy.route';
import { dataRouteRegex, dataRoute } from './route/data.route';
import { peerRoute } from './route/peer.route';
import { koiLogger, koiLogsRoute, koiLogsRawRoute } from './route/koi.route';
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
  app.get('/status', syncRoute);

  app.get(dataRouteRegex, dataRoute);

  app.post('/tx', transactionRoute);
  app.get('/peers', peerRoute);
  app.get('/logs', koiLogsRoute);
  app.get('/logs/raw', koiLogsRawRoute);

  // db endpoints
  app.get(`/block/height/:height`, blockByHeightRoute);

  // app.all('*', proxyRoute);

  app.listen(process.env.PORT || 3000, () => {
    log.info(`[app] started on http://localhost:${process.env.PORT || 3000}`);
    const graphqlServer = graphServer({ introspection: true });
    graphqlServer.start().then(() => {
      graphqlServer.applyMiddleware({
        app,
        path: '/graphql',
      });
      startSync();
    });
    app.get('/graphql', (req: Request, res: Response) => {
      res.setHeader('Content-Type', 'text/html');
      const playground = renderPlaygroundPage({});
      res.write(playground);
      res.end();
    });
  });
}

start();
