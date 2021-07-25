import { Request, Response, NextFunction } from 'express';
import { post } from 'superagent';
import {
  transactionMapper,
  txIdToBlockMapper,
} from '../database/mapper.database';

export async function txUploadRoute(
  req: Request,
  res: Response,
  next: NextFunction
) {
  try {
    const payload = await post(`${req.session.node}/tx`).send(req.body);
    return res.status(200).send(payload.body);
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

    const transaction = await transactionMapper.get({
      id: txId,
    });

    res.json(transaction);
  } catch (error) {
    // Passes errors into the error handler
    return next(error);
  }
}
