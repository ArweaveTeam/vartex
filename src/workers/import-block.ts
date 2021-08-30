import * as R from "rambda";
import { getMessenger } from "../gatsby-worker/child";
import { MessagesFromParent, MessagesFromWorker } from "./message-types";
import Fluture, { forkCatch, parallel } from "fluture/index.js";
import PriorityQueue, { ITxIncoming } from "../utility/priority.queue";
import pWaitFor from "p-wait-for";
import { types as CassandraTypes } from "cassandra-driver";
import { getCache, putCache, rmCache } from "../caching/cacache";
import { MAX_TX_PER_BLOCK } from "../database/constants.database";
import { mkWorkerLog } from "../utility/log.utility";
import { getBlock as queryGetBlock } from "../query/block.query";
import { getTransaction, getTxOffset } from "../query/transaction.query";
import { TxQueueState } from "../types/cassandra.types";
import {
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from "../database/cassandra.database";

const messenger = getMessenger<MessagesFromParent, MessagesFromWorker>();

if (messenger) {
  messenger.sendMessage({
    type: "worker:ready",
  });
}

const log = mkWorkerLog(messenger);
let topTxIndex: CassandraTypes.Long = toLong(0);
// let txInFlight = 0;

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
export const getIncomingTxQueueSize = txIncomingQueue.getSize.bind(
  txIncomingQueue
);
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

let txIncomingIsConsuming;

function unlockIncomingQueue() {
  txIncomingIsConsuming = undefined;
}

const handleTxImportError = (reason: string | undefined): void => {
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
  const entries: ITxIncoming[] = [...getEntriesTxIncoming()];

  if (entries.length === 0) {
    return;
  }
  // txInFlight += entries.length;

  while (
    getIncomingTxQueuePeek() &&
    entries.some((entry: ITxIncoming) =>
      entry.txIndex.equals(getIncomingTxQueuePeek().txIndex)
    )
  ) {
    popIncomingTxQueue();
  }

  const batch = entries.map((incTx) => {
    return new (Fluture as (any) => void)((reject, fresolve) => {
      incTx.next.call(this, fresolve);
      return () => {
        log("enqueueing of txId " + incTx.txId + " failed!");
        process.exit(1);
      };
    });
  });

  forkCatch(handleTxImportError)(handleTxImportError)(unlockIncomingQueue)(
    parallel(4)(batch)
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

    peek.callback().then(() => {
      popTxQueue();
      txQueueState.isProcessing = false;

      if (peek.txIndex.gt(topTxIndex)) {
        topTxIndex = peek.txIndex;
      }

      if (isTxQueueEmpty()) {
        log("import queues have been consumed");
      }

      if (fresolve) {
        // txInFlight -= 1;
        fresolve();
      }
    });
  }
}

setInterval(() => {
  processTxQueue();
  if (!isIncomingTxQueueEmpty() && isTxQueueEmpty()) {
    txIncomingParallelConsume();
  }
}, 80);

function incomingTxCallback(integrity: string, txIndex_: CassandraTypes.Long) {
  return async function (fresolve?: () => void) {
    let cacheData;
    let retry = 0;

    while (!cacheData && retry < 100) {
      cacheData = await getCache(integrity);
      await new Promise((resolve) => setTimeout(resolve, 1));
      retry += 1;
    }

    if (!cacheData) {
      log("Cache disappeared with txIndex", txIndex_.toString());
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
    await rmCache("inconming:" + (txIndex || txIndex_).toString());
  };
}

function txImportCallback(integrity: string) {
  return async function () {
    const cached = await getCache(integrity);
    const { height, index, tx, block } = JSON.parse(cached);
    log(JSON.stringify(cached));
    await makeTxImportQuery(toLong(height), toLong(index), tx, block)();
    await rmCache("tx:" + tx.id);
  };
}

export async function storeTransaction(
  txId: string,
  txIndex: CassandraTypes.Long,
  height: CassandraTypes.Long,
  blockData: { [k: string]: unknown },
  fresolve: () => void
): Promise<void> {
  const currentTransaction = await getTransaction({ txId });

  if (!currentTransaction) {
    fresolve(); // error message has already been printed
  } else {
    let maybeTxOffset;
    const dataSize = toLong(currentTransaction.data_size);
    if (dataSize && dataSize.gt(0)) {
      maybeTxOffset = await getTxOffset({ txId });
    }

    const integrity = await putCache(
      "tx:" + txId,
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
      callback: txImportCallback(integrity),
      fresolve,
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

        const txIncomingIntegrity = await putCache(
          "inconming:" + txIndex.toString(),
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
          next: incomingTxCallback(txIncomingIntegrity, txIndex),
        });
      })
    );

    return await putCache(
      newBlock.indep_hash,
      JSON.stringify({
        block: newBlock,
        height: newBlockHeight.toString(),
      })
    );
  } else {
    throw new Error(
      "Block couldn't be fetched or has some terrible block height mismatch going on"
    );
  }
}

export async function importBlock(height: number): Promise<boolean> {
  let success = false;
  let blockCacheIntegrity = "";

  try {
    blockCacheIntegrity = await getNewBlock(height);
    success = true;
  } catch (error) {
    log(error.toString());
  }

  if (!success) {
    return false;
  }

  await pWaitFor(() => isTxQueueEmpty() && isIncomingTxQueueEmpty());

  try {
    const { block } = JSON.parse(await getCache(blockCacheIntegrity));
    await makeBlockImportQuery(block)();
    await rmCache(block.indep_hash);
  } catch (error) {
    log(error.toString());
    success = false;
  }

  return success;
}
