import * as R from "rambda";
import Fluture, { fork, parallel } from "fluture/index.js";
import PriorityQueue from "../utility/priority.queue";
import pWaitFor from "p-wait-for";
import { DataItemJson } from "arweave-bundles";
import Gauge from "gauge";
import GaugeThemes from "gauge/themes";
import { config } from "dotenv";
import { types as CassandraTypes } from "cassandra-driver";
import { getCache, putCache, rmCache } from "../caching/cacache";
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
export const togglePause = (): void => {
  isPaused = !isPaused;
};

const blockQueue = new PriorityQueue(function (
  a: { height: CassandraTypes.Long },
  b: { height: CassandraTypes.Long }
) {
  return a.height.compare(b.height);
});

const txQueue = new PriorityQueue(function (
  a: { txIndex: CassandraTypes.Long },
  b: { txIndex: CassandraTypes.Long }
) {
  return a.txIndex.equals(0) || b.txIndex.equals(0)
    ? -1
    : a.txIndex.compare(b.txIndex);
});

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

// const tagsQueueState: QueueState = { isProcessing: false, isStarted: false };

const processBlockQueue = (
  queueSource: any,
  queueState: BlockQueueState
): void => {
  // console.log(
  //   !txQueue.hasNoneLt(queueState.lastPrio ? toLong(0) : queueState.lastPrio),
  //   queueSource.isEmpty(),
  //   isPaused,
  //   queueState.isProcessing
  // );
  if (queueSource.isEmpty() || isPaused || queueState.isProcessing) {
    return;
  }

  queueSource.sortQueue();
  const peek = !queueSource.isEmpty() && queueSource.peek();
  // console.error(
  //   "|",
  //   peek && peek.height.toString(),
  //   "<=",
  //   queueState.nextHeight.toString(),
  //   "|",
  //   "has noneLT",
  //   queueSource.hasNoneLt(queueState.nextHeight),
  //   "|"
  // );

  if (
    (CassandraTypes.Long.isLong(peek.height) && isPollingStarted) ||
    (CassandraTypes.Long.isLong(peek.height) &&
      txQueue.hasNoneLt(queueState.nextHeight) &&
      (queueState.nextHeight.lt(1) ||
        peek.height.lt(1) ||
        peek.height.lessThanOrEqual(queueState.nextHeight) ||
        queueSource.hasNoneLt(queueState.nextHeight)))
  ) {
    queueState.isProcessing = true;

    peek.callback().then(() => {
      queueSource.pop();
      queueState.isProcessing = false;

      if (peek.height.gt(topHeight)) {
        topHeight = peek.height;
      }

      queueSource.sortQueue();
      queueState.nextHeight =
        !queueSource.isEmpty() &&
        queueSource.peek().nextHeight &&
        peek.height.lt(queueSource.peek().nextHeight)
          ? toLong(queueSource.peek().nextHeight)
          : toLong(-1);

      if (queueSource.isEmpty() && txQueue.isEmpty()) {
        log.info("import queues have been consumed");
      }
    });
  }
};

const processTxQueue = (queueSource: any, queueState: TxQueueState): void => {
  if (queueSource.isEmpty() || isPaused || queueState.isProcessing) {
    return;
  }
  queueSource.sortQueue();
  const peek = !queueSource.isEmpty() && queueSource.peek();

  if (CassandraTypes.Long.isLong(peek.txIndex)) {
    queueState.isProcessing = true;
    queueState.nextTxIndex = peek.nextTxIndex;
    // const currentImportCnt = queueState.importedHeights[peek.height.toString()];
    // queueState.importedHeights[peek.height.toString()] = currentImportCnt
    //   ? 1
    //   : currentImportCnt;
    peek.callback().then(() => {
      queueSource.pop();
      queueState.isProcessing = false;

      if (peek.txIndex.gt(topTxIndex)) {
        topTxIndex = peek.txIndex;
      }

      if (queueSource.isEmpty() && blockQueue.isEmpty()) {
        log.info("import queues have been consumed");
      }
    });
  }
};

function startQueueProcessors() {
  if (!blockQueueState.isStarted) {
    blockQueueState.isStarted = true;
    setInterval(function processQ() {
      processBlockQueue(blockQueue, blockQueueState);
    }, 120);
  }
  if (!txQueueState.isStarted) {
    txQueueState.isStarted = true;
    setInterval(function processTxQ() {
      processTxQueue(txQueue, txQueueState); // follow block height when syncing tx's
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
    blockQueue.enqueue({
      callback: blockQueryCallback,
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

  const nodeInfo = await getNodeInfo({ keepAlive: true });

  if (!nodeInfo) {
    await new Promise((resolve) =>
      setTimeout(resolve, POLLTIME_DELAY_SECONDS * 1000)
    );
    return startPolling();
  }

  [topHash, topHeight] = await getMaxHeightBlock();

  if (nodeInfo.current === topHash) {
    // wait before polling again

    await new Promise((resolve) =>
      setTimeout(resolve, POLLTIME_DELAY_SECONDS * 1000)
    );
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
      await pWaitFor(() => blockQueue.isEmpty(), { interval: 500 });
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
        blockQueue.enqueue({
          nextHeight: toLong(-1),
          callback: blockQueryCallback,
          height: newBlockHeight,
          type: "block",
          txCount: newBlock.txs ? newBlock.txs.length : 0,
        });
      } else {
        console.error("Querying for new tx failed");
      }
      await new Promise((resolve) =>
        setTimeout(resolve, POLLTIME_DELAY_SECONDS * 1000)
      );
    }
  }
  return startPolling();
}

const detectFirstRun = async (): Promise<boolean> => {
  const queryResponse = await cassandraClient.execute(
    `SELECT height
     FROM ${KEYSPACE}.block LIMIT 1`
  );
  return queryResponse && queryResponse.rowLength > 0 ? false : true;
};

const findMissingBlocks = (
  hashList: string[],
  gauge: any
): Promise<UnsyncedBlock[]> => {
  const hashListObject = hashList.reduce((accumulator, hash, height) => {
    accumulator[height] = { height, hash };
    return accumulator;
  }, {});
  gauge.enable();
  log.info("[database] Looking for missing blocks...");
  return new Promise(
    (resolve: (value: any) => void, reject: (error: string) => void) => {
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
    }
  );
};

export async function startSync({ isTesting = false }) {
  signalHook();
  startQueueProcessors();

  const hashList: string[] = await getHashList({});
  const firstRun = await detectFirstRun();
  let lastBlock: CassandraTypes.Long = toLong(-1);
  let lastTx: CassandraTypes.Long = toLong(-1);

  if (!firstRun) {
    const isMaybeMissingBlocks = await Dr.checkForBlockGaps();
    if (isMaybeMissingBlocks) {
      const blockGap = await Dr.findBlockGaps();
      if (!R.isEmpty(blockGap)) {
        console.error("Repairing missing block(s):", blockGap);
        let doneSignalResolve;
        const doneSignal = new Promise((resolve) => {
          doneSignalResolve = resolve;
        });
        fork((error) => console.error(error))(() => {
          doneSignalResolve();
          console.log("Block repair done!");
        })(
          (parallel as any)(PARALLEL)(
            blockGap.map((gap, index) =>
              storeBlock({
                height: gap,
                next: -1,
              })
            )
          )
        );
        await doneSignal;
        blockQueueState.nextHeight = toLong(R.head(blockGap) + 1);
        await pWaitFor(() => blockQueue.isEmpty() && txQueue.isEmpty(), {
          interval: 1000,
        });
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

  fork((reason: string | void) => {
    console.error("Fatal", reason || "");
    process.exit(1);
  })(() => {
    gauge.disable();
    pWaitFor(() => blockQueue.isEmpty() && txQueue.isEmpty(), {
      interval: 1000,
    }).then(() => {
      log.info("Database fully in sync with block_list");
      !isPollingStarted && startPolling();
    });
  })(
    parallel(PARALLEL)(
      (unsyncedBlocks as any).map(
        ({
          height,
          hash,
          next,
        }: {
          height: number;
          hash: string;
          next: number;
        }): any => {
          const getProgress = () =>
            `${height}/${hashList.length}/${blockQueue.getSize()}`;
          return storeBlock({ height, hash, next, getProgress, gauge });
        }
      )
    )
  );
}

export function storeBlock({
  height,
  hash,
  next,
  getProgress,
  gauge,
}: {
  height: number;
  next: number;
  hash?: string;
  getProgress?: () => string;
  gauge?: any;
}): unknown {
  let isCancelled = false;
  return Fluture((reject: any, fresolve: any) => {
    async function getBlock(retry = 0) {
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
          (newSyncBlock.txs || []).map(async (txId: string, index: number) => {
            const txIndex = newSyncBlockHeight.mul(MAX_TX_PER_BLOCK).add(index);
            await new Promise((resolve) =>
              setTimeout(
                resolve,
                toLong(index).div(Math.min(PARALLEL, 1)).toInt()
              )
            ); // no black-friday rush here!
            storeTransaction(txId, txIndex, newSyncBlockHeight, newSyncBlock);
          })
        );

        const integrity = await putCache(
          newSyncBlock.indep_hash,
          JSON.stringify({
            block: newSyncBlock,
            height: newSyncBlockHeight.toString(),
            nextHeight: `${next ? next : newSyncBlockHeight.add(1).toString()}`,
          })
        );

        blockQueue.enqueue({
          callback: async () => {
            const { block } = JSON.parse(await getCache(integrity));
            await makeBlockImportQuery(block)();
            await rmCache(block.indep_hash);
          },
          height: newSyncBlockHeight,
          txCount: newSyncBlock.txs ? newSyncBlock.txs.length : 0,
          nextHeight: toLong(next),
          type: "block",
        });
        fresolve(true);
        return;
      } else {
        await new Promise((resolve) => setTimeout(resolve, 100));
        if (retry >= 250) {
          log.info(`Could not retrieve block at height ${height}`);
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
        blockQueue.getSize() < PARALLEL + 1,
      { interval: 500 }
    ).then(() => getBlock());

    return () => {
      isCancelled = true;
    };
  });
}

export async function storeTransaction(
  txId: string,
  txIndex: CassandraTypes.Long,
  height: CassandraTypes.Long,
  blockData: { [k: string]: any }
) {
  txQueue.sortQueue();

  await pWaitFor(
    () => {
      return (
        txQueue.isEmpty() ||
        txQueue.peek().txIndex.gt(txIndex) ||
        txQueue.getSize() < PARALLEL + 1
      );
    },
    { interval: 200 }
  );

  const currentTransaction = await getTransaction({ txId });

  if (currentTransaction) {
    let maybeTxOffset;
    const dataSize = toLong(currentTransaction.data_size);
    if (dataSize && dataSize.gt(0)) {
      maybeTxOffset = await getTxOffset({ txId });
    }

    const integrity = await putCache(
      txId,
      JSON.stringify({
        block: blockData,
        tx: R.mergeAll([
          currentTransaction,
          maybeTxOffset ? { tx_offset: maybeTxOffset } : {},
        ]),
        height: height.toString(),
        index: txIndex.toString(),
      })
    );

    txQueue.enqueue({
      height,
      callback: async () => {
        const { height, index, txOffset, tx, block } = JSON.parse(
          await getCache(integrity)
        );
        await makeTxImportQuery(toLong(height), toLong(txIndex), tx, block)();
        await rmCache(txId);
      },
      txIndex,
      type: "tx",
    });
    return;
  } else {
    console.error("Fatal network error");
    process.exit(1);
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
