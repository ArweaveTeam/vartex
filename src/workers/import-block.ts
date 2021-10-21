import * as R from "rambda";
import { IGatsbyWorkerMessenger, getMessenger } from "../gatsby-worker/child";
import { MessagesFromParent, MessagesFromWorker } from "./message-types";
import Fluture, { forkCatch, parallel } from "fluture/index.js";
import PriorityQueue, { ITxIncoming } from "../utility/priority-queue";
import pWaitFor from "p-wait-for";
import { types as CassandraTypes } from "cassandra-driver";
import { getCache, putCache, rmCache } from "../caching/cacache";
import { MAX_TX_PER_BLOCK } from "../database/constants";
import { mkWorkerLog } from "../utility/log";
import { getBlock as queryGetBlock } from "../query/block";
import { getTransaction, getTxOffset } from "../query/transaction";
import { TxQueueState } from "../types/cassandra";
import {
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from "../database/cassandra";

const PARALLEL_IMPORTS = Number.isNaN(process.env["PARALLEL_IMPORTS"])
  ? 1
  : Number.parseInt(process.env["PARALLEL_IMPORTS"] || "1");

const messenger = getMessenger<MessagesFromParent, MessagesFromWorker>();

if (messenger) {
  messenger.sendMessage({
    type: "worker:ready",
  });
}

const log = mkWorkerLog(messenger);

let topTxIndex: CassandraTypes.Long = toLong(0);
let txInFlight = 0;

const txIncomingQueue = new PriorityQueue(function (
  a: { txIndex: CassandraTypes.Long },
  b: { txIndex: CassandraTypes.Long }
) {
  return a.txIndex.compare(b.txIndex);
});

const isIncomingTxQueueEmpty = txIncomingQueue.isEmpty.bind(txIncomingQueue);
// const hasIncomingTxQueueNoneLt = txIncomingQueue.hasNoneLt.bind(
//   txIncomingQueue
// );
export const getIncomingTxQueueSize =
  txIncomingQueue.getSize.bind(txIncomingQueue);
const getIncomingTxQueuePeek = txIncomingQueue.peek.bind(txIncomingQueue);
const popIncomingTxQueue = txIncomingQueue.pop.bind(txIncomingQueue);
const sortIncomingTxQueue = txIncomingQueue.sortQueue.bind(txIncomingQueue);
// const removeItemIncomingTxQueue = txIncomingQueue.removeItem.bind(
//   txIncomingQueue
// );
const enqueueIncomingTxQueue = txIncomingQueue.enqueue.bind(txIncomingQueue);
const getEntriesTxIncoming = txIncomingQueue.entries.bind(txIncomingQueue);

// const txQueueArr = [];
const txQueue = new PriorityQueue(function (
  a: { txIndex: CassandraTypes.Long },
  b: { txIndex: CassandraTypes.Long }
) {
  return a.txIndex.compare(b.txIndex);
});

const isTxQueueEmpty = txQueue.isEmpty.bind(txQueue);
// const hasTxQueueNoneLt = txQueue.hasNoneLt.bind(txQueue);
// const getTxQueueSize = txQueue.getSize.bind(txQueue);
const getTxQueuePeek = txQueue.peek.bind(txQueue);
const popTxQueue = txQueue.pop.bind(txQueue);
const sortTxQueue = txQueue.sortQueue.bind(txQueue);
const enqueueTxQueue = txQueue.enqueue.bind(txQueue);

const txQueueState: TxQueueState = {
  isProcessing: false,
  isStarted: false,
  nextTxIndex: toLong(-1),
};

let txIncomingIsConsuming = false;

function unlockIncomingQueue() {
  txIncomingIsConsuming = false;
}

const handleTxImportError = (reason: Error | string | undefined): void => {
  log("Fatal", reason || "");
  process.exit(1);
};

const txIncomingParallelConsume = () => {
  if (txIncomingIsConsuming && !isTxQueueEmpty()) {
    return;
  } else {
    txIncomingIsConsuming = true;
  }
  sortIncomingTxQueue();
  const entries: ITxIncoming[] = [...(getEntriesTxIncoming() as any)];

  if (entries.length === 0) {
    return;
  }
  txInFlight += entries.length;
  messenger.sendMessage({
    type: "stats:tx:flight",
    payload: txInFlight,
  });

  while (
    getIncomingTxQueuePeek() &&
    entries.some((entry: ITxIncoming) =>
      entry.txIndex.equals(getIncomingTxQueuePeek().txIndex)
    )
  ) {
    popIncomingTxQueue();
  }

  const batch = entries.map((incTx) => {
    return new Fluture(
      (reject: (msg?: string) => void, fresolve: () => void) => {
        incTx.next.call(this, fresolve);
        return () => {
          log("enqueueing of txId " + incTx.txId + " failed!");
          process.exit(1);
        };
      }
    );
  });

  forkCatch(handleTxImportError)(handleTxImportError)(unlockIncomingQueue)(
    parallel(PARALLEL_IMPORTS)(batch)
  );
};

function processTxQueue(): void {
  if (isTxQueueEmpty() || txQueueState.isProcessing) {
    return;
  }

  sortTxQueue();
  const peek = !isTxQueueEmpty() && getTxQueuePeek();
  const fresolve = peek && peek.fresolve;

  if (CassandraTypes.Long.isLong(peek.txIndex)) {
    txQueueState.isProcessing = true;
    txQueueState.nextTxIndex = peek.nextTxIndex;
    popTxQueue();
    peek
      .callback()
      .then(() => {
        txQueueState.isProcessing = false;

        if (peek.txIndex.gt(topTxIndex)) {
          topTxIndex = peek.txIndex;
        }

        if (fresolve) {
          txInFlight -= 1;
          messenger.sendMessage({
            type: "stats:tx:flight",
            payload: txInFlight,
          });

          fresolve();
        }
      })
      .catch((error) => {
        log("FATAL error calling txQueueCallback", error);
        rmCache(`incoming:${peek.txIndex.toString()}`).then(() =>
          process.exit(1)
        );
      });
  }
}

setInterval(() => {
  processTxQueue();
  if (!isIncomingTxQueueEmpty() && isTxQueueEmpty()) {
    txIncomingParallelConsume();
  }
}, 80);

function incomingTxCallback(
  fileCacheKeyTx: string,
  txIndex_: CassandraTypes.Long
) {
  return async function (fresolve?: () => void) {
    let cacheData;
    let retry = 0;

    while (!cacheData && retry < 100) {
      cacheData = await getCache(fileCacheKeyTx);
      await new Promise((resolve) => setTimeout(resolve, 1));
      retry += 1;
    }

    if (!cacheData) {
      log("Cache disappeared with txIndex", txIndex_ && txIndex_.toString());
      await rmCache(fileCacheKeyTx);
      process.exit(1);
    }

    const {
      txId,
      block: txNewSyncBlock,
      height: txNewSyncBlockHeight,
      txIndex,
    } = JSON.parse(cacheData.toString());

    await storeTransaction(
      txId,
      toLong(txIndex || txIndex_),
      toLong(txNewSyncBlockHeight),
      txNewSyncBlock,
      fresolve
    );
    await rmCache(fileCacheKeyTx);
  };
}

function txImportCallback(fileCacheKey: string) {
  return async function () {
    let cached;
    try {
      await pWaitFor(async () => !!(cached = await getCache(fileCacheKey)), {
        timeout: 60 * 1000,
        interval: 200,
      });
    } catch (error) {
      log(`Failed waiting for tx in cache with key: ${fileCacheKey}`, error);
      await rmCache(fileCacheKey);
      process.exit(1);
    }
    const { height, index, tx, block } = JSON.parse(cached);
    try {
      await makeTxImportQuery(toLong(height), toLong(index), tx, block)();
    } catch (error) {
      log(error);
      await rmCache(fileCacheKey);
      process.exit(1);
    }

    await rmCache(fileCacheKey);
  };
}

export async function storeTransaction(
  txId: string,
  txIndex: CassandraTypes.Long,
  height: CassandraTypes.Long,
  blockData: { [k: string]: unknown },
  fresolve: () => void
): Promise<void> {
  if (!height) {
    log("storeTransaction called without height");
    process.exit(1);
  }
  if (!txIndex) {
    log("storeTransaction called without index");
    process.exit(1);
  }

  const currentTransaction = await getTransaction({ txId });

  if (!currentTransaction) {
    fresolve(); // error message has already been printed
  } else {
    let maybeTxOffset;
    const dataSize = toLong(currentTransaction.data_size);
    if (dataSize && dataSize.gt(0)) {
      maybeTxOffset = await getTxOffset({ txId });
    }
    const txCacheId = "tx:" + txId;
    await putCache(
      txCacheId,
      JSON.stringify({
        block: R.dissoc("nextHeight", blockData),
        tx: R.mergeAll([
          currentTransaction,
          maybeTxOffset ? { tx_offset: maybeTxOffset } : {},
        ]),
        txId,
        height: height.toString(),
        index: txIndex.toString(),
      })
    );

    enqueueTxQueue({
      height,
      callback: txImportCallback(txCacheId),
      fresolve,
      txId,
      txIndex,
      type: "tx",
    });
  }
}

async function getNewBlock(height: number) {
  const newBlock = await queryGetBlock({
    height,
  });

  const newBlockHeight = toLong(newBlock.height);

  if (newBlock && newBlock.height === height) {
    await Promise.all(
      R.uniq(newBlock.txs || []).map(async function (
        txId: string,
        index: number
      ) {
        const txIndex = newBlockHeight.mul(MAX_TX_PER_BLOCK).add(index);

        if (!newBlockHeight) {
          log("getNewBlock called without height");
          process.exit(1);
        }
        if (!txIndex) {
          log("getNewBlock didn't produce index");
          process.exit(1);
        }
        const fileCacheKeyTx = "inconming:" + txIndex.toString();
        await putCache(
          fileCacheKeyTx,
          JSON.stringify({
            type: "incoming" + index,
            txId,
            block: R.dissoc("nextHeight", newBlock),
            height: newBlockHeight.toString(),
            txIndex: txIndex.toString(),
          })
        );

        enqueueIncomingTxQueue({
          height: newBlockHeight,
          txIndex,
          next: incomingTxCallback(fileCacheKeyTx, txIndex),
        } as any);
      })
    );

    await putCache(
      newBlock.indep_hash,
      JSON.stringify({
        block: newBlock,
        height: newBlockHeight.toString(),
      })
    );
    return newBlock.indep_hash;
  } else {
    throw new Error(
      "Block couldn't be fetched or has some terrible block height mismatch going on"
    );
  }
}

export async function importBlock(height: number): Promise<boolean> {
  let success = false;
  let blockCacheKey = "";

  try {
    blockCacheKey = await getNewBlock(height);
    success = true;
  } catch (error) {
    log(
      `Failed to fetch new block at height: ${height} ${error} will retry...`
    );
  }

  if (!success) {
    process.exit(1);
  }

  await pWaitFor(() => isTxQueueEmpty() && isIncomingTxQueueEmpty());

  try {
    const data = JSON.parse(await getCache(blockCacheKey));

    const callback = await makeBlockImportQuery(data.block);
    await callback();
    await rmCache(data.block.indep_hash);
  } catch (error) {
    log(`Failed to import fetched block: ${error}`);
    success = false;
    process.exit(1);
  }

  return success;
}
