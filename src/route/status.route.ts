import { Request, Response } from 'express';
import { topHeight, syncHeight } from '../database/sync.database.js';
import { getNodeInfo } from '../query/node.query.js';

export const start = Number(new Date());

export async function statusRoute(req: Request, res: Response) {
  const info = await getNodeInfo({ maxRetry: 1 });

  if (info) {
    if (!syncHeight.equals(0)) {
      const delta = syncHeight.sub(info.height).toString();
      return res.status(200).send({
        status: 'SYNCING',
        gatewayHeight: topHeight.toString(),
        arweaveHeight: info.height,
        delta,
      });
    } else {
      const delta = topHeight.sub(info.height).toString();

      return res.status(200).send({
        status: 'OK',
        gatewayHeight: topHeight.toString(),
        arweaveHeight: info.height,
        delta,
      });
    }
  } else {
    return res.status(404).send();
  }
}
