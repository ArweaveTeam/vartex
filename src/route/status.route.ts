import {Request, Response} from 'express';
import {topHeight} from '../database/sync.database.js';
import {toLong} from '../database/cassandra.database.js';
import {getNodeInfo} from '../query/node.query.js';

export const start = Number(new Date());

export async function statusRoute(req: Request, res: Response) {
  try {
    const info = await getNodeInfo({maxRetry: 1});
    const delta = toLong(info.height).sub(topHeight).toString();

    return res.status(200).send({
      status: 'OK',
      gatewayHeight: topHeight.toString(),
      arweaveHeight: info.height,
      delta,
    });
  } catch (error) {
    return res.status(503).send(error);
  }
}
