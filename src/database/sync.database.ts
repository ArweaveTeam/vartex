import * as R from "rambda";
import Fluture, { chain, fork, parallel } from "fluture/index.js";
import PriorityQueue from "../utility/priority.queue";
import pWaitFor from "p-wait-for";
import { DataItemJson } from "arweave-bundles";
import Gauge from "gauge";
import GaugeThemes from "gauge/themes";
import { config } from "dotenv";
import { types as CassandraTypes } from "cassandra-driver";
import {
  getCache,
  getCacheByKey,
  putCache,
  // purgeCache,
  rmCache,
} from "../caching/cacache";
import { KEYSPACE, POLLTIME_DELAY_SECONDS } from "../constants";
import { MAX_TX_PER_BLOCK } from "./constants.database";
import { log } from "../utility/log.utility";
import { ansBundles } from "../utility/ans.utility";
import mkdirp from "mkdirp";
import {
  getDataFromChunks,
  getHashList,
  getNodeInfo,
} from "../query/node.query";
import {
  fetchBlockByHash,
  getBlock as queryGetBlock,
} from "../query/block.query";
import {
  getTransaction,
  getTxOffset,
  tagValue,
  Tag,
} from "../query/transaction.query";
import {
  DeleteRowData,
  ImportQueue,
  UnsyncedBlock,
  TxQueueState,
  BlockQueueState,
} from "../types/cassandra.types";
import {
  cassandraClient,
  getMaxHeightBlock,
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from "./cassandra.database";
import * as Dr from "./doctor.database";

process.env.NODE_ENV !== "test" && config();
mkdirp.sync("cache");

const trackerTheme = GaugeThemes.newTheme(
  GaugeThemes({
    hasUnicode: false,
    hasColor: true,
  })
);

export const SIGINT = false;
export let SIGKILL = false;
const PARALLEL = (Number.isNaN as any)(process.env["PARALLEL"])
  ? 36
  : Number.parseInt(process.env["PARALLEL"] || "36");

export let topHash = "";
export let topHeight: CassandraTypes.Long = toLong(0);
export let topTxIndex: CassandraTypes.Long = toLong(0);

const developmentSyncLength: number | undefined =
  !process.env["DEVELOPMENT_SYNC_LENGTH"] ||
  R.isEmpty(process.env["DEVELOPMENT_SYNC_LENGTH"])
    ? undefined
    : Number.parseInt(process.env["DEVELOPMENT_SYNC_LENGTH"] as string);

// eslint-disable-next-line use-isnan
if (developmentSyncLength === Number.NaN) {
  console.error("Development sync range variable produced, illegal value NaN");
  process.exit(1);
}

const isQueueProcessorStarted = false;
let isPollingStarted = false;
const isSyncing = true;
let isPaused = false;

export function togglePause(): void {
  isPaused = !isPaused;
}

// const blockQueueArr = [];

const blockQueue = new PriorityQueue(function (
  a: { height: CassandraTypes.Long },
  b: { height: CassandraTypes.Long }
) {
  return a.height.compare(b.height);
});

const txIncomingQueueState = {
  isEmpty: false,
  size: 0,
  isProcessing: false,
};

// const txIncomingQueueArr = [];

const txIncomingQueue = new PriorityQueue(function (
  a: { txIndex: CassandraTypes.Long },
  b: { txIndex: CassandraTypes.Long }
) {
  return a.txIndex.compare(b.txIndex);
});

const isIncomingTxQueueEmpty = txIncomingQueue.isEmpty.bind(txIncomingQueue);
const hasIncomingTxQueueNoneLt = txIncomingQueue.hasNoneLt.bind(
  txIncomingQueue
);
const getIncomingTxQueueSize = txIncomingQueue.getSize.bind(txIncomingQueue);
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
const hasTxQueueNoneLt = txQueue.hasNoneLt.bind(txQueue);
const getTxQueueSize = txQueue.getSize.bind(txQueue);
const getTxQueuePeek = txQueue.peek.bind(txQueue);
const popTxQueue = txQueue.pop.bind(txQueue);
const sortTxQueue = txQueue.sortQueue.bind(txQueue);
const enqueueTxQueue = txQueue.enqueue.bind(txQueue);

const blockQueueState: BlockQueueState = {
  isProcessing: false,
  isStarted: false,
  nextHeight: toLong(-1),
};

const txQueueState: TxQueueState = {
  isProcessing: false,
  isStarted: false,
  nextTxIndex: toLong(-1),
};

const isBlockQueueEmpty = blockQueue.isEmpty.bind(blockQueue);
const hasBlockQueueNoneLt = blockQueue.hasNoneLt.bind(blockQueue);
const getBlockQueueSize = blockQueue.getSize.bind(blockQueue);
const getBlockQueuePeek = blockQueue.peek.bind(blockQueue);
const popBlockQueue = blockQueue.pop.bind(blockQueue);
const sortBlockQueue = blockQueue.sortQueue.bind(blockQueue);
const enqueueBlockQueue = blockQueue.enqueue.bind(blockQueue);

// const tagsQueueState: QueueState = { isProcessing: false, isStarted: false };

function processBlockQueue(): void {
  if (
    isBlockQueueEmpty() ||
    isPaused ||
    blockQueueState.isProcessing ||
    getIncomingTxQueueSize() > 1000
  ) {
    return;
  }

  sortBlockQueue();
  const peek = !isBlockQueueEmpty() && getBlockQueuePeek();

  if (
    (CassandraTypes.Long.isLong(peek.height) && isPollingStarted) ||
    (CassandraTypes.Long.isLong(peek.height) &&
      hasTxQueueNoneLt(blockQueueState.nextHeight) &&
      hasIncomingTxQueueNoneLt(blockQueueState.nextHeight) &&
      (blockQueueState.nextHeight.lt(1) ||
        peek.height.lt(1) ||
        peek.height.lessThanOrEqual(blockQueueState.nextHeight) ||
        hasBlockQueueNoneLt(blockQueueState.nextHeight)))
  ) {
    blockQueueState.isProcessing = true;

    peek.callback.call(blockQueue).then(function () {
      popBlockQueue();
      blockQueueState.isProcessing = false;

      if (peek.height.gt(topHeight)) {
        topHeight = peek.height;
      }

      sortBlockQueue();
      blockQueueState.nextHeight =
        !isBlockQueueEmpty() &&
        getBlockQueuePeek().nextHeight &&
        peek.height.lt(getBlockQueuePeek().nextHeight)
          ? toLong(getBlockQueuePeek().nextHeight)
          : toLong(-1);

      peek.fresolve && peek.fresolve();
      if (isBlockQueueEmpty() && isTxQueueEmpty()) {
        log.info(
          "import queues have been consumed" + isIncomingTxQueueEmpty()
            ? "\n"
            : ", waiting for txs...\n"
        );
      }
    });
  }
}

function processTxQueue(): void {
  if (isTxQueueEmpty() || isPaused || txQueueState.isProcessing) {
    return;
  }

  sortTxQueue();
  const peek = !isTxQueueEmpty() && getTxQueuePeek();
  const fresolve = peek && peek.fresolve;

  if (CassandraTypes.Long.isLong(peek.txIndex)) {
    txQueueState.isProcessing = true;
    txQueueState.nextTxIndex = peek.nextTxIndex;

    peek.callback
      .bind(txQueue)()
      .then(function () {
        popTxQueue();
        txQueueState.isProcessing = false;

        if (peek.txIndex.gt(topTxIndex)) {
          topTxIndex = peek.txIndex;
        }

        if (isTxQueueEmpty() && isBlockQueueEmpty()) {
          log.info("import queues have been consumed");
        }

        setTimeout(fresolve, 1);
      });
  }
}

function startQueueProcessors() {
  if (!blockQueueState.isStarted) {
    blockQueueState.isStarted = true;
    setInterval(function () {
      processBlockQueue.bind(this)();
    }, 120);
  }
  if (!txQueueState.isStarted) {
    txQueueState.isStarted = true;
    setInterval(function () {
      processTxQueue.bind(this)();
    }, 80);
  }
}

async function resolveFork(previousBlock: any): Promise<void> {
  isPaused = true;
  const pprevBlock = await fetchBlockByHash(previousBlock.previous_block);

  const blockQueryResult = await cassandraClient.execute(
    `SELECT height FROM ${KEYSPACE}.block WHERE indep_hash=?`,
    [pprevBlock.indep_hash]
  );

  if (blockQueryResult.rowLength > 0) {
    cassandraClient.eachRow(
      `SELECT height, indep_hash
       FROM ${KEYSPACE}.block
       WHERE height > ${blockQueryResult.rows[0].height.toString()} ALLOW FILTERING`,
      [],
      {
        autoPage: true,
        prepare: false,
        executionProfile: "fast",
      },
      async function (n, row) {
        await cassandraClient.execute(
          `DELETE
           FROM ${KEYSPACE}.block
           WHERE indep_hash = '${row.indep_hash}'`
        );
      },

      function (error, result) {
        isPaused = false;
        log.info(
          "fork diverges at " +
            blockQueryResult.rows[0].height.toString() +
            " waiting for missing blocks to be imported..."
        );
      }
    );
  } else {
    const blockQueryCallback = makeBlockImportQuery(pprevBlock);
    enqueueBlockQueue({
      callback: blockQueryCallback.bind(blockQueue),
      height:
        pprevBlock.height !== null && !Number.isNaN(pprevBlock.height)
          ? toLong(pprevBlock.height)
          : toLong(0),
      type: "block",
      nextHeight: toLong(-1),
      txCount: pprevBlock.txs ? pprevBlock.txs.length : 0,
    });

    return await resolveFork(pprevBlock);
  }
}

async function startPolling(): Promise<void> {
  if (SIGINT || SIGKILL) {
    process.exit(1);
  }
  if (!isPollingStarted) {
    isPollingStarted = true;
    log.info(
      "polling for new blocks every " + POLLTIME_DELAY_SECONDS + " seconds"
    );
  }

  const nodeInfo = await getNodeInfo({});

  if (!nodeInfo) {
    await new Promise(function (resolve) {
      setTimeout(resolve, POLLTIME_DELAY_SECONDS * 1000);
    });
    return startPolling();
  }

  [topHash, topHeight] = await getMaxHeightBlock();

  if (nodeInfo.current === topHash) {
    // wait before polling again

    await new Promise(function (resolve) {
      setTimeout(resolve, POLLTIME_DELAY_SECONDS * 1000);
    });
    return startPolling();
  } else {
    const currentRemoteBlock = await fetchBlockByHash(nodeInfo.current);
    const previousBlock = await fetchBlockByHash(
      currentRemoteBlock.previous_block
    );

    // fork recovery
    if (previousBlock.indep_hash !== topHash) {
      log.info(
        "blocks out of sync with the remote node " +
          previousBlock.indep_hash +
          "!= " +
          topHash
      );
      await resolveFork(currentRemoteBlock);
      await pWaitFor(
        function () {
          return isBlockQueueEmpty();
        },
        { interval: 500 }
      );
      log.info("blocks are back in sync!");
    } else {
      const newBlock = await queryGetBlock({
        height: nodeInfo.height,
        hash: nodeInfo.current,
      });
      if (newBlock !== undefined) {
        const newBlockHeight =
          newBlock.height !== null && !Number.isNaN(newBlock.height)
            ? toLong(newBlock.height)
            : toLong(0);
        log.info("new block arrived at height " + newBlockHeight.toString());
        const blockQueryCallback = makeBlockImportQuery(newBlock);
        enqueueBlockQueue({
          nextHeight: toLong(-1),
          callback: blockQueryCallback.bind(blockQueue),
          height: newBlockHeight,
          type: "block",
          txCount: newBlock.txs ? newBlock.txs.length : 0,
        });
      } else {
        console.error("Querying for new tx failed");
      }
      await new Promise(function (resolve) {
        setTimeout(resolve, POLLTIME_DELAY_SECONDS * 1000);
      });
    }
  }
  return startPolling();
}

async function detectFirstRun(): Promise<boolean> {
  const queryResponse = await cassandraClient.execute(
    `SELECT height
     FROM ${KEYSPACE}.block LIMIT 1`
  );
  return queryResponse && queryResponse.rowLength > 0 ? false : true;
}

function findMissingBlocks(
  hashList: string[],
  gauge: any
): Promise<UnsyncedBlock[]> {
  const hashListObject = hashList.reduce((accumulator, hash, height) => {
    accumulator[height] = { height, hash };
    return accumulator;
  }, {});
  gauge.enable();
  log.info("[database] Looking for missing blocks...");
  return new Promise(function (
    resolve: (value: any) => void,
    reject: (error: string) => void
  ) {
    cassandraClient.eachRow(
      `SELECT height, indep_hash, timestamp, txs
         FROM ${KEYSPACE}.block`,
      [],
      {
        autoPage: true,
        prepare: false,
        executionProfile: "fast",
      },
      async function (n, row) {
        gauge.show(`Looking for missing blocks: ${n}/${hashList.length}`);
        if (SIGINT || SIGKILL) {
          process.exit(1);
        }
        const matchingRow = hashListObject[row.height];

        if (
          matchingRow &&
          R.equals(matchingRow["hash"], row.indep_hash) &&
          R.equals(matchingRow["height"], row.height)
        ) {
          // log.info('DEQUEUEING' + row.height);
          delete hashListObject[row.height];
        }
      },
      async function (error, result) {
        gauge.disable();
        if (error) {
          reject((error || "").toString());
        } else {
          const returnValue = R.pipe(
            R.values,
            R.sortBy(R.prop("height")),
            (missingBlocksList) =>
              missingBlocksList.reduce((accumulator, value, index) => {
                // adding .next to each unsynced blocked
                const nextHeight =
                  index + 1 < missingBlocksList.length
                    ? missingBlocksList[index + 1]
                    : -1;
                (value as any)["next"] = nextHeight;
                accumulator.push(value);
                return accumulator;
              }, [])
          )(hashListObject);
          resolve(returnValue);
        }
      }
    );
  });
}

function txIncomingParallelConsume() {
  sortIncomingTxQueue();
  const entries = getEntriesTxIncoming();

  while (
    getIncomingTxQueuePeek() &&
    R.find((entry: any) =>
      entry.txIndex.equals(getIncomingTxQueuePeek().txIndex)
    )(entries)
  ) {
    popIncomingTxQueue();
  }

  const batch = entries.map((incTx) => {
    return new (Fluture as (any) => void)((reject, fresolve) => {
      incTx.next(fresolve);
      return () => {
        log.info("enqueueing of txId " + incTx.txId + " failed!");
        process.exit(1);
      };
    });
  });

  fork((reason: string | void) => {
    console.error("Fatal", reason || "");
    process.exit(1);
  })(function () {
    if (isIncomingTxQueueEmpty()) {
      txIncomingQueueState.isProcessing = false;
    } else {
      return txIncomingParallelConsume();
    }
  })(parallel(PARALLEL)(batch));
}

export async function startSync({ isTesting = false }) {
  signalHook();
  startQueueProcessors();

  const hashList: string[] = await getHashList({});
  const firstRun = await detectFirstRun();
  let lastBlock: CassandraTypes.Long = toLong(-1);
  let lastTx: CassandraTypes.Long = toLong(-1);

  if (!firstRun) {
    await Dr.enqueueUnhandledCache(
      enqueueIncomingTxQueue,
      enqueueTxQueue,
      txImportCallback,
      incomingTxCallback,
      txQueue
    );

    const isMaybeMissingBlocks = await Dr.checkForBlockGaps();

    if (isMaybeMissingBlocks) {
      const blockGap = await Dr.findBlockGaps();
      if (!R.isEmpty(blockGap)) {
        console.error("Repairing missing block(s):", blockGap);
        let doneSignalResolve;
        const doneSignal = new Promise(function (resolve) {
          doneSignalResolve = resolve;
        });
        fork((error) => console.error(error))(function () {
          doneSignalResolve();
          console.log("Block repair done!");
        })(
          (parallel as any)(PARALLEL)(
            blockGap.map(function (gap, index) {
              return storeBlock({
                height: gap,
                next: toLong(-1),
              });
            })
          )
        );
        await doneSignal;
        blockQueueState.nextHeight = toLong(R.head(blockGap) + 1);
        await pWaitFor(
          function () {
            return isBlockQueueEmpty() && isTxQueueEmpty();
          },
          {
            interval: 1000,
          }
        );
        blockQueueState.nextHeight = toLong(-1);
      }
      // process.exit(1);
    }

    // await Dr.findTxGaps();
    try {
      lastBlock = (
        await cassandraClient.execute(
          `SELECT height
           FROM ${KEYSPACE}.block_gql_desc LIMIT 1`
        )
      ).rows[0].height;
      lastTx = (
        await cassandraClient.execute(
          `SELECT tx_index
           FROM ${KEYSPACE}.tx_id_gql_desc LIMIT 1`
        )
      ).rows[0].tx_index;
    } catch {
      // console.error(error);
    }
  }

  const gauge = new Gauge(process.stderr, {
    // tty: 79,
    template: [
      { type: "progressbar", length: 0 },
      { type: "activityIndicator", kerning: 1, length: 2 },
      { type: "section", kerning: 1, default: "" },
      { type: "subsection", kerning: 1, default: "" },
    ],
  });
  gauge.setTheme(trackerTheme);

  let unsyncedBlocks: UnsyncedBlock[] = firstRun
    ? hashList.map((hash, height) => ({ hash, height }))
    : await findMissingBlocks(hashList, gauge);

  const initialLastBlock = toLong(
    unsyncedBlocks[0] ? unsyncedBlocks[0].height : 0
  );

  if (developmentSyncLength) {
    unsyncedBlocks = R.slice(
      developmentSyncLength,
      unsyncedBlocks.length,
      unsyncedBlocks
    );

    // initialLastBlock = toLong(developmentSyncLength).sub(1);
    topTxIndex = initialLastBlock.mul(MAX_TX_PER_BLOCK);
    topHeight = initialLastBlock;
  } else {
    topTxIndex = lastTx;
    topHeight = lastBlock;
  }

  blockQueueState.nextHeight = initialLastBlock.lt(1)
    ? toLong(1)
    : initialLastBlock;
  txQueueState.nextTxIndex = initialLastBlock.mul(MAX_TX_PER_BLOCK);

  if (firstRun) {
    log.info(
      "[sync] database seems to be empty, starting preperations for import..."
    );
  } else if (R.isEmpty(unsyncedBlocks)) {
    log.info("[sync] fully synced db");
    startPolling();
    return;
  } else {
    log.info(
      `[sync] missing ${unsyncedBlocks.length} blocks, starting sync...`
    );
  }
  // check health
  // if (!firstRun) {
  //   await Dr.fixNonLinearBlockOrder();
  // }

  gauge.enable();

  fork(function (reason: string | void) {
    console.error("Fatal", reason || "");
    process.exit(1);
  })(function () {
    gauge.disable();
    pWaitFor(
      function () {
        return (
          isIncomingTxQueueEmpty() && isBlockQueueEmpty() && isTxQueueEmpty()
        );
      },
      {
        interval: 1000,
      }
    ).then(function () {
      log.info("Database fully in sync with block_list");
      !isPollingStarted && startPolling();
    });
  })(
    parallel(PARALLEL)(
      (unsyncedBlocks as any).map(function ({
        height,
        hash,
        next,
      }: {
        height: number;
        hash: string;
        next: number;
      }): any {
        const getProgress = () => {
          return `blocks: ${height}/${
            hashList.length
          }/${getBlockQueueSize()} txs: ${getIncomingTxQueueSize()}/${getTxQueueSize()}/${PARALLEL}`;
        };

        return storeBlock({
          height,
          hash,
          next: toLong(next || -1),
          getProgress,
          gauge,
        });
      })
    )
  );
}

let lastTimeEmptyTxQueue = false;

const incomingTxCallback = (
  integrity: string,
  txIndex_: CassandraTypes.Long,
  gauge?: any,
  getProgress?: () => string
) => async (fresolve?: () => void) => {
  gauge && gauge.show(`${getProgress ? getProgress() || "" : ""}`);
  let cacheData;
  let retry = 0;

  while (!cacheData && retry < 100) {
    cacheData = await getCache(integrity);
    await new Promise((resolve) => setTimeout(resolve, 1));
    retry += 1;
  }

  if (!cacheData) {
    console.error("Cache disappeared with txIndex", txIndex_.toString());
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

export function storeBlock({
  height,
  hash,
  next,
  getProgress,
  gauge,
}: {
  height: number;
  next: CassandraTypes.Long;
  hash?: string;
  getProgress?: () => string;
  gauge?: any;
}): unknown {
  let isCancelled = false;
  return Fluture(function (reject: any, fresolve: any) {
    fresolve = fresolve.bind(this);

    const getBlock = async (retry = 0) => {
      if (isPaused || isCancelled) {
        return;
      }
      const newSyncBlock = await queryGetBlock({
        hash,
        height,
        gauge,
        getProgress,
      });

      if (newSyncBlock && newSyncBlock.height === height) {
        const newSyncBlockHeight = toLong(newSyncBlock.height);

        await Promise.all(
          R.uniq(newSyncBlock.txs || []).map(async function (
            txId: string,
            index: number
          ) {
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
              next: incomingTxCallback.bind(txQueue)(
                txIncomingIntegrity,
                txIndex,
                gauge,
                getProgress
              ),
            });
          })
        );

        lastTimeEmptyTxQueue = isTxQueueEmpty();

        if (
          isTxQueueEmpty() &&
          !isIncomingTxQueueEmpty() &&
          !txIncomingQueueState.isProcessing
        ) {
          txIncomingQueueState.isProcessing = true;
          txIncomingParallelConsume();
        }

        const nextHeightStr = next
          ? next.toString()
          : newSyncBlockHeight.add(1).toString();

        const integrity = await putCache(
          newSyncBlock.indep_hash,
          JSON.stringify({
            block: R.assoc("next_height", nextHeightStr, newSyncBlock),
            height: newSyncBlockHeight.toString(),
            nextHeight: nextHeightStr,
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
          return await getBlock(retry + 1);
        }
      }
    };

    sortBlockQueue();
    getBlock();

    return () => {
      isCancelled = true;
    };
  });
}

function txImportCallback(integrity: string) {
  return async function () {
    const cached = await getCache(integrity);
    const { height, index, txOffset, tx, block } = JSON.parse(cached);

    await makeTxImportQuery(toLong(height), toLong(index), tx, block)();
    await rmCache("tx:" + tx.id);
  };
}

export async function storeTransaction(
  txId: string,
  txIndex: CassandraTypes.Long,
  height: CassandraTypes.Long,
  blockData: { [k: string]: any },
  fresolve: () => void
) {
  const currentTransaction = await getTransaction({ txId });

  if (!currentTransaction) {
    fresolve(); // error message has alrady been printed
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

    // console.error("POST |", txIndex.toString(), "|");
    enqueueTxQueue({
      height,
      callback: txImportCallback(integrity).bind(txQueue),
      fresolve,
      txIndex,
      type: "tx",
    });
  }
}

export function signalHook() {
  process.on("SIGINT", () => {
    log.info(
      "[database] ensuring all blocks are stored before exit, you may see some extra output in console"
    );
    SIGKILL = true;
    setInterval(() => {
      if (SIGINT === false) {
        log.info("[database] block sync state preserved, now exiting");
        console.log("");
        process.exit();
      }
    }, 100);
  });
}
