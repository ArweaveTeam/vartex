import Fluture, { fork, parallel } from "fluture";
import { log } from 'console';
import pWaitFor from 'p-wait-for';
import { toLong, makeBlockImportQuery } from '../database/cassandra.database';
import { MAX_TX_PER_BLOCK } from '../database/constants.database';
import { storeTransaction } from '../database/sync.database';
import PriorityQueue from "../utility/priority.queue";
import { types as CassandraTypes } from "cassandra-driver";
import { ThreadWorker } from 'poolifier';

import {
  fetchBlockByHash,
  getBlock as queryGetBlock,
} from "../query/block.query";

const PARALLEL = (Number.isNaN)(process.env["PARALLEL"])
  ? 36
  : Number.parseInt(process.env["PARALLEL"] || "36");

const blockQueue = new PriorityQueue(function (
  a,
  b
) {
  return a.height.compare(b.height);
});

export function storeBlock({
  height,
  next,
  hash,
  getProgress,
  gauge,
}) {

  // Convert strings back to functions
  getProgress = Function(getProgress);
  gauge = Function(gauge);

  let isCancelled = false;
  return Fluture((reject, resolve) => {
    async function getBlock(retry = 0) {
      
      const newSyncBlock = await queryGetBlock({
        hash,
        height,
        gauge,
        getProgress,
      });

      if (newSyncBlock && newSyncBlock.height === height) {
        const newSyncBlockHeight = toLong(newSyncBlock.height);
        await Promise.all(
          (newSyncBlock.txs || []).map(async (txId, index) => {
            const txIndex = newSyncBlockHeight.mul(MAX_TX_PER_BLOCK).add(index);
            await storeTransaction(
              txId,
              txIndex,
              newSyncBlockHeight,
              newSyncBlock
            );
          })
        );
        blockQueue.enqueue({
          callback: makeBlockImportQuery(newSyncBlock),
          height: newSyncBlockHeight,
          txCount: newSyncBlock.txs ? newSyncBlock.txs.length : 0,
          nextHeight: toLong(next),
          type: "block",
        });
        return;
      } else {
        await new Promise((resolve) => setTimeout(resolve, 100));
        if (retry >= 250) {
          console.info(`Could not retrieve block at height ${height}`);
          reject("Failed to fetch block after 250 retries");
        } else {
          return await getBlock(retry + 1);
        }
      }
    }

    blockQueue.sortQueue();

    pWaitFor(
      () =>
        blockQueue.isEmpty() ||
        blockQueue.peek().height.gt(height) ||
        blockQueue.getSize() < PARALLEL + 1
    )
      .then(() => getBlock())
      .then(resolve);

    return () => {
      isCancelled = true;
    };
  });
}

export default new ThreadWorker(storeBlock, {
  maxInactiveTime: 60000,
  async: false
});