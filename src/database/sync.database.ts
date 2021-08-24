import * as R from "rambda";
import path from "path";
import Fluture, { fork, forkCatch, parallel } from "fluture/index.js";
import PriorityQueue from "../utility/priority.queue";
import pWaitFor from "p-wait-for";
// import { DataItemJson } from "arweave-bundles";
import Gauge from "gauge";
import GaugeThemes from "gauge/themes";
import { config } from "dotenv";
import { types as CassandraTypes } from "cassandra-driver";
import {
  getCache,
  putCache,
  gcImportCache,
  lastGcImportCacheRun,
  rmCache,
} from "../caching/cacache";
import { KEYSPACE, POLLTIME_DELAY_SECONDS } from "../constants";
import { MAX_TX_PER_BLOCK } from "./constants.database";
import { log } from "../utility/log.utility";
import mkdirp from "mkdirp";
import { WorkerPool } from "../gatsby-worker";
import os from "os";

import {
  // getDataFromChunks,
  getHashList,
  getNodeInfo,
} from "../query/node.query";
import {
  BlockType,
  fetchBlockByHash,
  getBlock as queryGetBlock,
} from "../query/block.query";
import { getTransaction, getTxOffset } from "../query/transaction.query";
import {
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

const PARALLEL = (Number.isNaN as any)(process.env["PARALLEL"])
  ? 36
  : Number.parseInt(process.env["PARALLEL"] || "36");

export let topHash = "";
export let topHeight: CassandraTypes.Long = toLong(0);

// export let topTxIndex: CassandraTypes.Long = toLong(0);
// export let txInFlight = 0;

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

let isPollingStarted = false;
let isImportCacheGcRunning = true; // true because we want to wait before starting periodic gc runs
let isPaused = false;

export function togglePause(): void {
  isPaused = !isPaused;
}

// // const blockQueueArr = [];
// const blockQueue = new PriorityQueue(function (
//   a: { height: CassandraTypes.Long },
//   b: { height: CassandraTypes.Long }
// ) {
//   return a.height.compare(b.height);
// });

// const blockQueueState: BlockQueueState = {
//   isProcessing: false,
//   isStarted: false,
//   nextHeight: toLong(-1),
// };

// const isBlockQueueEmpty = blockQueue.isEmpty.bind(blockQueue);
// const hasBlockQueueNoneLt = blockQueue.hasNoneLt.bind(blockQueue);
// export const getBlockQueueSize = blockQueue.getSize.bind(blockQueue);
// const getBlockQueuePeek = blockQueue.peek.bind(blockQueue);
// const popBlockQueue = blockQueue.pop.bind(blockQueue);
// const sortBlockQueue = blockQueue.sortQueue.bind(blockQueue);
// const enqueueBlockQueue = blockQueue.enqueue.bind(blockQueue);

// const tagsQueueState: QueueState = { isProcessing: false, isStarted: false };

// function processBlockQueue(): void {
//   if (
//     isBlockQueueEmpty() ||
//     isPaused ||
//     blockQueueState.isProcessing ||
//     getIncomingTxQueueSize() > 1000
//   ) {
//     return;
//   }

//   sortBlockQueue();
//   const peek = !isBlockQueueEmpty() && getBlockQueuePeek();

//   if (
//     (CassandraTypes.Long.isLong(peek.height) && isPollingStarted) ||
//     (CassandraTypes.Long.isLong(peek.height) &&
//       hasTxQueueNoneLt(blockQueueState.nextHeight) &&
//       hasIncomingTxQueueNoneLt(blockQueueState.nextHeight) &&
//       (blockQueueState.nextHeight.lt(1) ||
//         peek.height.lt(1) ||
//         peek.height.lessThanOrEqual(blockQueueState.nextHeight) ||
//         hasBlockQueueNoneLt(blockQueueState.nextHeight)))
//   ) {
//     blockQueueState.isProcessing = true;

//     peek.callback.call(blockQueue).then(function () {
//       popBlockQueue();
//       blockQueueState.isProcessing = false;

//       if (peek.height.gt(topHeight)) {
//         topHeight = peek.height;
//       }

//       sortBlockQueue();
//       blockQueueState.nextHeight =
//         !isBlockQueueEmpty() &&
//         getBlockQueuePeek().nextHeight &&
//         peek.height.lt(getBlockQueuePeek().nextHeight)
//           ? toLong(getBlockQueuePeek().nextHeight)
//           : toLong(-1);

//       peek.fresolve && peek.fresolve();
//       // if (isBlockQueueEmpty() && isTxQueueEmpty()) {
//       // log.info(
//       //   "import queues have been consumed" + isIncomingTxQueueEmpty()
//       //     ? "\n"
//       //     : ", waiting for txs...\n"
//       // );
//       // }
//     });
//   }
// }

/*
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

      function () {
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
*/

/*
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
*/

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
      async function (error) {
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

function filterNpmFlags(obj) {
  const keyz: string[] = Object.keys(obj);
  const out = {};
  for (const key of keyz) {
    if (!key.startsWith("npm")) {
      out[key] = obj[key];
    }
  }
  return out;
}

const workerPool = new WorkerPool<typeof import("../workers/import-block")>(
  process.cwd() + "/src/workers/import-block",
  {
    numWorkers: PARALLEL,
    env: R.mergeAll([
      {
        PWD: process.cwd(),
        TS_NODE_FILES: true,
        NODE_PATH: process.cwd() + "/node_modules",
        NODE_OPTIONS: `--require ${
          process.cwd() + "/node_modules/ts-node/register"
        }`,
      },
      filterNpmFlags(process.env),
    ]),

    // forkOptions: {
    //   env: ,
    //   cwd: process.cwd(),
    //   execArgv: [
    //     "--trace-warnings",
    //     "--experimental-specifier-resolution=node",
    //     "--loader",
    //     `${process.cwd()}/node_modules/ts-node/esm.mjs`,
    //   ],
    // },
  }
);

// workerPool.onMessage( .getStdout().pipe(process.stdout);

// workerPool.getStderr().pipe(process.stderr);

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export async function startSync({ isTesting = false }) {
  const hashList: string[] = await getHashList({});
  const firstRun = await detectFirstRun();
  let lastBlock: CassandraTypes.Long = toLong(-1);
  // let lastTx: CassandraTypes.Long = toLong(-1);

  if (!firstRun) {
    // await Dr.enqueueUnhandledCache(
    //   enqueueIncomingTxQueue,
    //   enqueueTxQueue,
    //   txImportCallback,
    //   incomingTxCallback,
    //   txQueue
    // );

    const isMaybeMissingBlocks = await Dr.checkForBlockGaps();

    if (isMaybeMissingBlocks) {
      const blockGap = await Dr.findBlockGaps();
      if (!R.isEmpty(blockGap)) {
        console.error("Repairing missing block(s):", blockGap);

        let doneSignalResolve;
        const doneSignal = new Promise(function (resolve) {
          doneSignalResolve = resolve;
        });

        fork((error) => console.error(error))(() => {
          doneSignalResolve();
          console.log("Block repair done!");
        })(
          (parallel as any)(PARALLEL)(
            blockGap.map((height) =>
              Fluture(function (reject: any, fresolve: any) {
                workerPool.single.default(height).then(fresolve).catch(reject);
                return () => {
                  console.error(`Fluture.Parallel crashed`);
                  process.exit(1);
                };
              })
            )
          )
        );
        await doneSignal;
      }
    }

    // await Dr.findTxGaps();
    try {
      lastBlock = (
        await cassandraClient.execute(
          `SELECT height
           FROM ${KEYSPACE}.block_gql_desc LIMIT 1`
        )
      ).rows[0].height;
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
    topHeight = initialLastBlock;
  } else {
    topHeight = lastBlock;
  }

  // blockQueueState.nextHeight = initialLastBlock.lt(1)
  //   ? toLong(1)
  //   : initialLastBlock;
  // txQueueState.nextTxIndex = initialLastBlock.mul(MAX_TX_PER_BLOCK);
  isImportCacheGcRunning = false;

  if (firstRun) {
    log.info(
      "[sync] database seems to be empty, starting preperations for import..."
    );
  } else if (R.isEmpty(unsyncedBlocks)) {
    log.info("[sync] fully synced db");
    // startPolling();
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
    log.info("Database fully in sync with block_list");
    // !isPollingStarted && startPolling();
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
          return Fluture(function (reject: any, fresolve: any) {
            workerPool.single
              .default(height)
              .then(() => {
                fresolve();
                gauge.show(`blocks: ${height}/${hashList.length}`);
              })
              .catch(reject);

            return () => {
              console.error(
                "Fatal error while importing block at height",
                height
              );
            };
          });
        }
      )
    )
  );
}
