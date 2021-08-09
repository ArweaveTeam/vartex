import * as R from "rambda";
import Fluture, { fork, parallel } from "fluture/index.js";
import PriorityQueue from "../utility/priority.queue";
import pWaitFor from "p-wait-for";
import { DataItemJson } from "arweave-bundles";
import Gauge from "gauge";
import GaugeThemes from "gauge/themes";
import { config } from "dotenv";
import { types as CassandraTypes } from "cassandra-driver";
import { KEYSPACE, POLLTIME_DELAY_SECONDS } from "../constants";
import { MAX_TX_PER_BLOCK } from "./constants.database";
import { log } from "../utility/log.utility";
import { ansBundles } from "../utility/ans.utility";
import { mkdir } from "../utility/file.utility";
import { getDataFromChunks, getHashList, getNodeInfo } from "../query/node.query";
import { fetchBlockByHash, getBlock as queryGetBlock } from "../query/block.query";
import { getTransaction, getTxOffset } from "../query/transaction.query";
import { QueueState, UnsyncedBlock } from "../types/cassandra.types";
import {
  cassandraClient,
  getMaxHeightBlock,
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from "./cassandra.database";
import * as Dr from "./doctor.database";
import { cacheANSEntries } from "../caching/ans.entry.caching";

process.env.NODE_ENV !== "test" && config();
mkdir("cache");

const trackerTheme = GaugeThemes.newTheme(
    GaugeThemes({
      hasUnicode: false,
      hasColor: true,
    }),
);

export const SIGINT = false;
export let SIGKILL = false;
const PARALLEL = (isNaN as any)(process.env["PARALLEL"]) ?
  36 :
  parseInt(process.env["PARALLEL"] || "36");

export let topHash = "";
export let topHeight: CassandraTypes.Long = toLong(0);
export let topTxIndex: CassandraTypes.Long = toLong(0);

const developmentSyncLength: number | undefined =
  !process.env["DEVELOPMENT_SYNC_LENGTH"] ||
  R.isEmpty(process.env["DEVELOPMENT_SYNC_LENGTH"]) ?
    undefined :
    parseInt(process.env["DEVELOPMENT_SYNC_LENGTH"] as string);

if (isNaN(developmentSyncLength)) {
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

const blockQueue = new PriorityQueue(function(
    a: { height: CassandraTypes.Long },
    b: { height: CassandraTypes.Long },
) {
  return a.height.compare(b.height);
});

const txQueue = new PriorityQueue(function(
    a: { txIndex: CassandraTypes.Long },
    b: { txIndex: CassandraTypes.Long },
) {
  if (a.txIndex.equals(0) || b.txIndex.equals(0)) {
    return -1;
  } else {
    return a.txIndex.compare(b.txIndex);
  }
});

// const txQueue: ImportQueue = {};
// const tagsQueue: ImportQueue = {};
const blockQueueState: QueueState = {
  isProcessing: false,
  isStarted: false,
  lastPrio: toLong(-1),
};
const txQueueState: QueueState = {
  isProcessing: false,
  isStarted: false,
  lastPrio: toLong(-1),
  importedHeights: {},
};

const tagsQueueState: QueueState = { isProcessing: false, isStarted: false };

const createQueue = (
    queueSource: Record<string, any>,
    queueState: QueueState,
) => (): void => {
  if (queueState.isProcessing) return;

  const items: string[] = R.keys(queueSource);
  if (items.length > 0) {
    // name could be misleading as this can be a batch of db-batches
    const batchPrio = items.sort()[0];
    queueState.isProcessing = true;
    const batch = queueSource[batchPrio]();
    (Array.isArray(batch) ? Promise.all(batch) : batch)
        .then(function(ret: any) {
          delete (queueSource as any)[batchPrio];
          queueState.isProcessing = false;
        })
        .catch(function(err: any) {
          console.error("FATAL", err);
          process.exit(1);
        });
  }
};

const processBlockQueue = (queueSource: any, queueState: QueueState): void => {
  // console.log(
  //   !txQueue.hasNoneLt(queueState.lastPrio ? toLong(0) : queueState.lastPrio),
  //   queueSource.isEmpty(),
  //   isPaused,
  //   queueState.isProcessing
  // );
  if (
    !txQueue.hasNoneLt(queueState.lastPrio ? toLong(0) : queueState.lastPrio) ||
    queueSource.isEmpty() ||
    isPaused ||
    queueState.isProcessing
  ) {
    return;
  }
  queueSource.sortQueue();
  const nextHeight = queueState.lastPrio ?
    queueState.lastPrio.add(1) :
    toLong(0);
  const peek = !queueSource.isEmpty() && queueSource.peek();

  if (
    (CassandraTypes.Long.isLong(peek.height) && isPollingStarted) ||
    (CassandraTypes.Long.isLong(peek.height) &&
    peek.height.equals(nextHeight) &&
    txQueueState.importedHeights[queueState.lastPrio.toString()]
      ? txQueueState.importedHeights[queueState.lastPrio.toString()] ===
        peek.txCount
      : true)
  ) {
    queueState.isProcessing = true;
    queueState.lastPrio = peek.height;

    peek.callback().then(() => {
      queueSource.pop();
      queueState.isProcessing = false;

      if (peek.height.gt(topHeight)) {
        topHeight = peek.height;
      }

      if (queueSource.isEmpty() && txQueue.isEmpty()) {
        log.info('import queues have been consumed');
      }
    });
  }
};

const processTxQueue = (queueSource: any, queueState: QueueState): void => {
  if (queueSource.isEmpty() || isPaused || queueState.isProcessing) {
    return;
  }
  queueSource.sortQueue();
  const peek = !queueSource.isEmpty() && queueSource.peek();

  if (CassandraTypes.Long.isLong(peek.txIndex)) {
    queueState.isProcessing = true;
    queueState.lastPrio = peek.txIndex;
    const currentImportCnt = queueState.importedHeights[peek.height.toString()];
    queueState.importedHeights[peek.height.toString()] = currentImportCnt
      ? 1
      : currentImportCnt;
    peek.callback().then(() => {
      queueSource.pop();
      queueState.isProcessing = false;

      if (peek.txIndex.gt(topTxIndex)) {
        topTxIndex = peek.txIndex;
      }

      if (queueSource.isEmpty() && blockQueue.isEmpty()) {
        log.info('import queues have been consumed');
      }
    });
  }
};

function startQueueProcessors() {
  if (!blockQueueState.isStarted) {
    blockQueueState.isStarted = true;
    setInterval(function processQ() {
      processBlockQueue(blockQueue, blockQueueState);
    }, 100);
  }
  if (!txQueueState.isStarted) {
    txQueueState.isStarted = true;
    setInterval(function processTxQ() {
      processTxQueue(txQueue, txQueueState); // follow block height when syncing tx's
    }, 100);
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
        async function(n, row) {
          await cassandraClient.execute(
              `DELETE
           FROM ${KEYSPACE}.block
           WHERE indep_hash = '${row.indep_hash}'`,
          );
        },

      function (err, res) {
        isPaused = false;
        log.info(
          'fork diverges at ' +
            blockQueryResult.rows[0].height.toString() +
            ' waiting for missing blocks to be imported...'
        );
      }
    );
  } else {
    const blockQueryCallback = makeBlockImportQuery(pprevBlock);
    blockQueue.enqueue({
      callback: blockQueryCallback,
      height:
        pprevBlock.height !== null && !isNaN(pprevBlock.height) ?
          toLong(pprevBlock.height) :
          toLong(0),
      type: "block",
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
      'polling for new blocks every ' + POLLTIME_DELAY_SECONDS + ' seconds'
    );
  }

  const nodeInfo = await getNodeInfo({ keepAlive: true });

  if (!nodeInfo) {
    await new Promise((res) => setTimeout(res, POLLTIME_DELAY_SECONDS * 1000));
    return startPolling();
  }

  [topHash, topHeight] = await getMaxHeightBlock();

  if (nodeInfo.current === topHash) {
    // wait before polling again

    await new Promise((res) => setTimeout(res, POLLTIME_DELAY_SECONDS * 1000));
    return startPolling();
  } else {
    const currentRemoteBlock = await fetchBlockByHash(nodeInfo.current);
    const previousBlock = await fetchBlockByHash(
        currentRemoteBlock.previous_block,
    );

    // log.info('prevIndep: ' + previousBlock.indep_hash + ' topHash: ' + topHash);
    // fork recovery
    if (previousBlock.indep_hash !== topHash) {
      log.info(
        'blocks out of sync with the remote node ' +
          previousBlock.indep_hash +
          '!= ' +
          topHash
      );
      await resolveFork(currentRemoteBlock);
      await pWaitFor(() => blockQueue.isEmpty());
      log.info('blocks are back in sync!');
    } else {
      const newBlock = await queryGetBlock({
        height: nodeInfo.height,
        hash: nodeInfo.current,
      });
      if (newBlock !== undefined) {
        const newBlockHeight =
          newBlock.height !== null && !isNaN(newBlock.height) ?
            toLong(newBlock.height) :
            toLong(0);
        log.info("new block arrived at height " + newBlockHeight.toString());
        const blockQueryCallback = makeBlockImportQuery(newBlock);
        blockQueue.enqueue({
          callback: blockQueryCallback,
          height: newBlockHeight,
          type: "block",
          txCount: newBlock.txs ? newBlock.txs.length : 0,
        });
      } else {
        console.error("Querying for new tx failed");
      }
      await new Promise((res) =>
        setTimeout(res, POLLTIME_DELAY_SECONDS * 1000),
      );
    }
  }
  return startPolling();
}

const detectFirstRun = async (): Promise<boolean> => {
  const queryResponse = await cassandraClient.execute(
      `SELECT height
     FROM ${KEYSPACE}.block LIMIT 1`,
  );
  if (queryResponse && queryResponse.rowLength > 0) {
    return false;
  } else {
    return true;
  }
};

const findMissingBlocks = (
    hashList: string[],
    gauge: any,
): Promise<UnsyncedBlock[]> => {
  const hashListObj = hashList.reduce((acc, hash, height) => {
    acc[height] = { height, hash };
    return acc;
  }, {});
  gauge.enable();
  log.info("[database] Looking for missing blocks...");
  return new Promise(
      (resolve: (val?: any) => void, reject: (err: string) => void) => {
        cassandraClient.eachRow(
            `SELECT height, indep_hash, timestamp, txs
         FROM ${KEYSPACE}.block`,
            [],
            {
              autoPage: true,
              prepare: false,
              executionProfile: "fast",
            },
            async function(n, row) {
              gauge.show(`Looking for missing blocks: ${n}/${hashList.length}`);
              if (SIGINT || SIGKILL) {
                process.exit(1);
              }
              const matchingRow = hashListObj[row.height];

              if (
                matchingRow &&
            R.equals(matchingRow["hash"], row.indep_hash) &&
            R.equals(matchingRow["height"], row.height)
              ) {
                // log.info('DEQUEUEING' + row.height);
                delete hashListObj[row.height];
              }
            },
            async function(err, res) {
              gauge.disable();
              if (err) {
                reject((err || "").toString());
              } else {
                resolve(R.pipe(R.values, R.sortBy(R.prop("height")))(hashListObj));
              }
            },
        );
      },
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
        console.error("Found missing block(s):", blockGap);
      }
      // process.exit(1);
    }

    await Dr.findTxGaps();

    try {
      lastBlock = (
        await cassandraClient.execute(
            `SELECT height
           FROM ${KEYSPACE}.block_gql_desc LIMIT 1`,
        )
      ).rows[0].height;
      lastTx = (
        await cassandraClient.execute(
            `SELECT tx_index
           FROM ${KEYSPACE}.tx_id_gql_desc LIMIT 1`,
        )
      ).rows[0].tx_index;
    } catch (error) {
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

  let unsyncedBlocks: UnsyncedBlock[] = firstRun ?
    hashList.map((hash, height) => ({ hash, height })) :
    await findMissingBlocks(hashList, gauge);

  let initialLastBlock = toLong(
    unsyncedBlocks[0] ? unsyncedBlocks[0].height : 0,
  ).add(-1);

  if (developmentSyncLength) {
    unsyncedBlocks = R.slice(
        developmentSyncLength,
        unsyncedBlocks.length,
        unsyncedBlocks,
    );

    initialLastBlock = toLong(developmentSyncLength).sub(1);
    topTxIndex = initialLastBlock.mul(MAX_TX_PER_BLOCK);
    topHeight = initialLastBlock;
  } else {
    topTxIndex = lastTx;
    topHeight = lastBlock;
  }

  blockQueueState.lastPrio = initialLastBlock;
  txQueueState.lastPrio = initialLastBlock.mul(MAX_TX_PER_BLOCK);

  if (firstRun) {
    log.info(
        "[sync] database seems to be empty, starting preperations for import...",
    );
  } else if (R.isEmpty(unsyncedBlocks)) {
    log.info("[sync] fully synced db");
    startPolling();
    return;
  } else {
    log.info(
        `[sync] missing ${unsyncedBlocks.length} blocks, starting sync...` +
      unsyncedBlocks.map(JSON.stringify as any).join(", "),
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
    pWaitFor(() => blockQueue.isEmpty() && txQueue.isEmpty()).then(() => {
      log.info("Database fully in sync with block_list");
      !isPollingStarted && startPolling();
    });
  })(
      parallel(PARALLEL)(
          (unsyncedBlocks as any).map(
              ({ height, hash }: { height: any; hash: string }): any => {
                const getProgress = () =>
                  `${height}/${hashList.length}/${blockQueue.getSize()}`;
                return storeBlock(height, hash, getProgress, gauge);
              },
          ),
      ),
  );
}

export function storeBlock(
    height: number,
    hash: string,
    getProgress: () => string,
    gauge: any,
): unknown {
  let isCancelled = false;
  return Fluture((reject: any, resolve: any) => {
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
              await storeTransaction(
                  txId,
                  txIndex,
                  newSyncBlockHeight,
                  newSyncBlock,
              );
            }),
        );
        blockQueue.enqueue({
          callback: makeBlockImportQuery(newSyncBlock),
          height: newSyncBlockHeight,
          txCount: newSyncBlock.txs ? newSyncBlock.txs.length : 0,
          type: "block",
        });
        return;
      } else {
        await new Promise((res) => setTimeout(res, 100));
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
    )
        .then(() => getBlock())
        .then(resolve);

    return () => {
      isCancelled = true;
    };
  });
}

export async function storeTransaction(
    txId: string,
    txIndex: CassandraTypes.Long,
    height: CassandraTypes.Long,
    blockData: { [k: string]: any },
) {
  txQueue.sortQueue();

  await pWaitFor(
      () =>
        txQueue.isEmpty() ||
      txQueue.peek().txIndex.gt(txIndex) ||
      txQueue.getSize() < PARALLEL + 1,
  );

  const currentTransaction = await getTransaction({ txId });

  if (currentTransaction) {
    let maybeTxOffset = {};
    const dataSize = toLong(currentTransaction.data_size);
    if (dataSize && dataSize.gt(0)) {
      maybeTxOffset = await getTxOffset({ txId });
    }

    // streams.transaction.cache.write(input);

    // storeTags(formattedTransaction.id, preservedTags);

    // const ans102 = tagValue(preservedTags, 'Bundle-Type') === 'ANS-102';

    // if (ans102) {
    //   await processAns(formattedTransaction.id, height);
    // }

    txQueue.enqueue({
      height,
      callback: makeTxImportQuery(
          height,
          txIndex,
          currentTransaction,
          blockData,
      ),
      txIndex: txIndex,
      type: "tx",
    });
  } else {
    console.error("Fatal network error");
    process.exit(1);
  }
}

export async function processAns(
    id: string,
    height: number,
    retry = true,
) {
  try {
    const ansPayload = await getDataFromChunks({
      id,
      startOffset: CassandraTypes.Long.fromNumber(0), // FIXEME
      endOffset: CassandraTypes.Long.fromNumber(0), // FIXME
    });
    const ansTxs = await ansBundles.unbundleData(ansPayload.toString("utf-8"));

    await cacheANSEntries(ansTxs);
    await processANSTransaction(ansTxs, height);
  } catch (error) {
    if (retry) {
      await processAns(id, height, false);
    } else {
      log.info(
          `[database] malformed ANS payload at height ${height} for tx ${id}`,
      );
      // streams.rescan.cache.write(`${id}|${height}|ans\n`);
    }
  }
}

export async function processANSTransaction(
    ansTxs: Array<DataItemJson>,
    height: number,
) {
  for (let i = 0; i < ansTxs.length; i++) {
    // const ansTx = ansTxs[i];
    // const { ansTags, input } = serializeAnsTransaction(ansTx, height);
    // streams.transaction.cache.write(input);
    // for (let ii = 0; ii < ansTags.length; ii++) {
    //   const ansTag = ansTags[ii];
    //   const { name, value } = ansTag;
    // const tag: DatabaseTag = {
    //   tx_id: ansTx.id,
    //   index: ii,
    //   name: name || '',
    //   value: value || '',
    // };
    // const input = `"${tag.tx_id}"|"${tag.index}"|"${tag.name}"|"${tag.value}"\n`;
    // streams.tags.cache.write(input);
    // }
  }
}

export function signalHook() {
  process.on("SIGINT", () => {
    log.info(
        "[database] ensuring all blocks are stored before exit, you may see some extra output in console",
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
