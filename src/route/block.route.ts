import * as R from 'rambda';
import {Request, Response} from 'express';
import {
  blockMapper,
  blockHeightToHashMapper,
  poaMapper,
} from '../database/mapper.database';
import {topHash, topHeight} from '../database/sync.database';

export async function blockByHeightRoute(
    req: Request,
    res: Response,
    next: (err?: string) => void,
) {
  if (!req.params.height) {
    res.status(503);
    return next('Height value was not specified');
  } else {
    try {
      const height = req.params.height;
      const {block_hash} = await blockHeightToHashMapper.get({
        block_height: height,
      });
      const poa = await poaMapper.get({block_hash, block_height: height});
      const blockResult = await blockMapper.get({
        // height,
        indep_hash: block_hash,
      });

      R.pipe(
          R.dissoc('txs_count'),
          R.assoc(
              'poa',
              R.pipe(R.dissoc('block_hash'), R.dissoc('block_height'))(poa),
          ),
          (ret) => res.json(ret),
      )(blockResult);
    } catch (error) {
      // Passes errors into the error handler
      return next(error);
    }
  }
}

export async function blockByHashRoute(
    req: Request,
    res: Response,
    next: (err?: string) => void,
) {
  if (!req.params.hash) {
    res.status(503);
    return next('Height value was not specified');
  } else {
    try {
      const hash = req.params.hash;
      const blockResult = await blockMapper.get({
        indep_hash: hash,
      });

      if (!blockResult || !blockResult.height) {
        return res.status(404).json({
          status: 404,
          error: 'Not Found',
        });
      }

      const poa = await poaMapper.get({
        block_hash: hash,
        block_height: blockResult.height,
      });

      R.pipe(
          R.dissoc('txs_count'),
          R.assoc(
              'poa',
              R.pipe(R.dissoc('block_hash'), R.dissoc('block_height'))(poa),
          ),
          (ret) => res.json(ret),
      )(blockResult);
    } catch (error) {
      // Passes errors into the error handler
      return next(error);
    }
  }
}

export async function blockCurrentRoute(
    req: Request,
    res: Response,
    next: (err?: string) => void,
) {
  try {
    const {block_hash} = await blockHeightToHashMapper.get({
      block_height: topHeight.toString(),
    });

    const blockResult = await blockMapper.get({
      indep_hash: block_hash,
    });

    if (!blockResult) {
      res.status(404);
      return next('Current block was not found');
    }

    const poa = await poaMapper.get({
      block_hash,
      block_height: topHeight.toString(),
    });

    R.pipe(
        R.dissoc('txs_count'),
        R.assoc(
            'poa',
            R.pipe(R.dissoc('block_hash'), R.dissoc('block_height'))(poa),
        ),
        (ret) => res.json(ret),
    )(blockResult);
  } catch (error) {
    return next(error);
  }
}
