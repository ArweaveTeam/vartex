import * as R from "rambda";
import { Request, Response } from "express";
import { blockMapper, blockHeightToHashMapper } from "../database/mapper";
import { topHeight } from "../database/sync";

export async function blockByHeightRoute(
  request: Request,
  response: Response,
  next: (error?: string) => void
): Promise<void> {
  if (!request.params.height) {
    response.status(503);
    return next("Height value was not specified");
  } else {
    try {
      const height = request.params.height;
      const { block_hash } = await blockHeightToHashMapper.get({
        block_height: height,
      });
      if (!block_hash) {
        next(`{error: "not_found"}`);
        return;
      }
      const blockResult = await blockMapper.get({
        // height,
        indep_hash: block_hash,
      });

      R.pipe(
        R.dissoc("txs_count"),

        (returnValue) => response.json(returnValue)
      )(blockResult);
    } catch (error) {
      // Passes errors into the error handler
      next(JSON.stringify(error));
    }
  }
}

export async function blockByHashRoute(
  request: Request,
  response: Response,
  next: (error?: string) => void
): Promise<void> {
  if (!request.params.hash) {
    response.status(503);
    next("Height value was not specified");
  } else {
    try {
      const hash = request.params.hash;
      const blockResult = await blockMapper.get({
        indep_hash: hash,
      });

      if (!blockResult || !blockResult.height) {
        response.status(404).json({
          status: 404,
          error: "Not Found",
        });
      }

      R.pipe(R.dissoc("txs_count"), (returnValue) =>
        response.json(returnValue)
      )(blockResult);
    } catch (error) {
      // Passes errors into the error handler
      next(JSON.stringify(error));
    }
  }
}

export async function blockCurrentRoute(
  request: Request,
  response: Response,
  next: (error?: string) => void
): Promise<void> {
  try {
    const { block_hash } = await blockHeightToHashMapper.get({
      block_height: topHeight.toString(),
    });

    const blockResult = await blockMapper.get({
      indep_hash: block_hash,
    });

    if (!blockResult) {
      response.status(404);
      return next("Current block was not found");
    }

    R.pipe(R.dissoc("txs_count"), (returnValue) => response.json(returnValue))(
      blockResult
    );
  } catch (error) {
    next(JSON.stringify(error));
  }
}
