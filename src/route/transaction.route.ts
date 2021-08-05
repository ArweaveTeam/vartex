import * as R from 'rambda';
import { Request, Response, NextFunction } from 'express';
import got from 'got';
import {
  transactionMapper,
  txIdToBlockMapper,
  tagsByTxId,
} from '../database/mapper.database.js';

export async function txUploadRoute(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    const body = await got.post(`${req.session.node}/tx`, {
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
    // const txBlockMeta = await txIdToBlockMapper.get({ tx_id: txId });

    const rawTx = await transactionMapper.get({
      id: txId,
    });

    const transaction = R.pipe(
      R.assoc('tags', await tagsByTxId(txId)),
      R.dissoc('tag_count')
    )(rawTx);
    res.json(transaction);
  } catch (error) {
    // Passes errors into the error handler
    return next(error);
  }
}
