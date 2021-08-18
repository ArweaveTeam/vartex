import { log } from 'console';
import { toLong, makeBlockImportQuery } from '../database/cassandra.database';
import { MAX_TX_PER_BLOCK } from '../database/constants.database';
import PriorityQueue from "../utility/priority.queue";
import { ThreadWorker } from 'poolifier';

import {
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

const sortBlockQueue = blockQueue.sortQueue.bind(blockQueue);

const getBlock = async (retry = 0) => {
  const newSyncBlock = await queryGetBlock({
    hash,
    height,
    gauge,
    getProgress,
  });

  if (newSyncBlock && newSyncBlock.height === height) {
    const newSyncBlockHeight = toLong(newSyncBlock.height);

    await Promise.all(
      R.uniq(newSyncBlock.txs || []).map(async function (txId, index) {
        const txIndex = newSyncBlockHeight.mul(MAX_TX_PER_BLOCK).add(index);
        // log.info("TXINDX " + txIndex.toString() + " " + index);

        const txIncomingIntegrity = await putCache(
          "inconming:" + txIndex.toString(),
          JSON.stringify({
            type: "incoming" + index,
            txId,
            block: R.dissoc("nextHeight", newSyncBlock),
            height: newSyncBlockHeight.toString(),
            txIndex: txIndex.toString(),
          })
        );

        enqueueIncomingTxQueue({
          height: newSyncBlockHeight,
          txIndex,
          next: incomingTxCallback(
            txIncomingIntegrity,
            txIndex,
            gauge,
            getProgress
          ),
        });
      })
    );

    const nextHeightString = next
      ? next.toString()
      : newSyncBlockHeight.add(1).toString();

    const integrity = await putCache(
      newSyncBlock.indep_hash,
      JSON.stringify({
        block: R.assoc("next_height", nextHeightString, newSyncBlock),
        height: newSyncBlockHeight.toString(),
        nextHeight: nextHeightString,
      })
    );

    const blockCallback = async function () {
      const { block } = JSON.parse(await getCache(integrity));
      await makeBlockImportQuery(block)();
      await rmCache(block.indep_hash);
    };
    await new Promise(function (resolve) {
      setTimeout(resolve, 0);
    });
    enqueueBlockQueue({
      callback: blockCallback.bind(blockQueue),
      height: newSyncBlockHeight,
      txCount: newSyncBlock.txs ? newSyncBlock.txs.length : 0,
      nextHeight: toLong(next),
      fresolve,
      type: "block",
    });
  } else {
    await new Promise(function (resolve) {
      setTimeout(resolve, 100);
    });
    if (retry >= 250) {
      log.info(`Could not retrieve block at height ${height}`);
      reject("Failed to fetch block after 250 retries");
    } else {
      await getBlock(retry + 1);
      return;
    }
  }
};

function storeBlock({
  height,
  hash,
  next,
  getProgress,
  gauge,
}) {
  // Convert strings back to functions
  if(getProgress) getProgress = Function(getProgress);
  if(gauge) gauge = Function(gauge);

    sortBlockQueue();
    getBlock();

}

export default new ThreadWorker(storeBlock, {
  maxInactiveTime: 60000,
  async: false
});