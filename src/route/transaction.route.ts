import * as R from 'rambda';
import { Request, Response, NextFunction } from 'express';
import got from 'got';
import {
  transactionMapper,
  txOffsetMapper,
  txIdToBlockMapper,
  tagsByTxId,
} from '../database/mapper.database.js';
import { grabNode } from '../query/node.query.js';

export async function txUploadRoute(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    const body = await got.post(`${grabNode()}/tx`, {
      followRedirect: true,
      json: req.body,
    });

    return res.status(200).send(body);
  } catch (error) {
    console.log(error);
    return res.status(500).send(error);
  }
}

export async function txGetByIdRoute(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    const txId = req.params.id;
    const rawTx = await transactionMapper.get({
      tx_id: txId,
    });
    res.json(R.pipe(R.dissoc('tag_count'), R.dissoc('tx_index'))(rawTx));
  } catch (error) {
    return next(error);
  }
}

export async function txOffsetRoute(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    const txId = req.params.id;
    const rawTx = await txOffsetMapper.get({
      tx_id: txId,
    });
    res.json(R.dissoc('tx_id')(rawTx || { size: 0, offset: -1 }));
  } catch (error) {
    return next(error);
  }
}
