import { Request, Response } from 'express';
import { syncHeight, topHeight } from '../database/sync.database';
import { toLong } from '../database/cassandra.database';
import { getNodeInfo } from '../query/node.query';

export const start = Number(new Date());

export async function syncRoute(req: Request, res: Response) {
  const info = await getNodeInfo({ maxRetry: 1, keepAlive: true });

  if (info) {
    const delta = toLong(info.height).sub(syncHeight);
    const status = delta.lt(3) ? 200 : 400;

    return res.status(status).send({
      status: 'OK',
      gatewayHeight: topHeight.sub(syncHeight).toString(),
      arweaveHeight: info.height,
      delta: delta.toString(),
    });
  } else {
    return res.status(404).send();
  }
}
