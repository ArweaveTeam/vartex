import {Request, Response} from 'express';
import { NODES } from '../query/node.query';

export async function peerRoute(req: Request, res: Response) {
  return res.status(200).send({
    status: 'OK',
    nodes: NODES,
  });
}
