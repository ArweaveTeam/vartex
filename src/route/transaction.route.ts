import * as R from "rambda";
import { Request, Response, NextFunction } from "express";
import got from "got";
import {
  transactionMapper,
  txOffsetMapper,
  txIdToBlockMapper,
  tagsByTxId,
} from "../database/mapper.database.js";
import { grabNode } from "../query/node.query.js";
import Transaction from "arweave/node/lib/transaction";

export async function txUploadRoute(
  request: Request,
  response: Response,
  next: NextFunction
) {
  try {
    const tx = request.body as Transaction;
    console.log(`[new-tx] broadcast tx ${tx.id}`);

    const host = grabNode();

    const result = await got.post(`${host}/tx`, {
      followRedirect: true,
      json: request.body,
    });

    if ([400, 410].includes(result.statusCode)) {
      console.error("[broadcast-tx] failed", {
        id: tx.id,
        host,
        code: result.statusCode,
        error: result.statusMessage,
      });
    }

    return response.sendStatus(200).end();
  } catch (error) {
    console.log(error);
    return response.status(500).send(error);
  }
}

export async function txGetByIdRoute(
  request: Request,
  response: Response,
  next: NextFunction
) {
  try {
    const txId = request.params.id;
    const rawTx = await transactionMapper.get({
      tx_id: txId,
    });

    // would need to parse json here before returning, and probably slow everything down a lot in order to dynamically load smartweave states

    response.json(R.pipe(R.dissoc("tag_count"), R.dissoc("tx_index"))(rawTx));
  } catch (error) {
    return next(error);
  }
}

export async function txOffsetRoute(
  request: Request,
  response: Response,
  next: NextFunction
) {
  try {
    const txId = request.params.id;
    const rawTx = await txOffsetMapper.get({
      tx_id: txId,
    });
    response.json(R.dissoc("tx_id")(rawTx || { size: 0, offset: -1 }));
  } catch (error) {
    return next(error);
  }
}
