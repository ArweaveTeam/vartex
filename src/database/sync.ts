import * as R from "rambda";
// import pWaitFor from "p-wait-for";
import pMinDelay from "p-min-delay";
import pWhilst from "p-whilst";
import pLimit from "p-limit";
import exitHook from "exit-hook";
import Gauge from "gauge";
import GaugeThemes from "gauge/themes";
import { types as CassandraTypes } from "cassandra-driver";
import { env, KEYSPACE, isGatewayNodeModeEnabled } from "../constants";
import { log } from "../utility/log";
import { getHashList, getNodeInfo } from "../query/node";
import { BlockType, fetchBlockByHash } from "../query/block";
import { UnsyncedBlock } from "../types/cassandra";
import { cassandraClient } from "./cassandra";
import { statusMapper } from "./mapper";
import { getMaxHeightBlock, toLong } from "./utils";
import {
  blockImportWorkerPool,
  // txsImportWorkerPool,
  manifestImportWorkerPool,
  // ans102ImportWorkerPool,
  // ans104ImportWorkerPool,
  workerReadyPromises,
} from "./worker-pools";
import * as Dr from "./doctor";

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
let session_: any;

const currentImports = new Set();

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
// const messagePromiseReceivers: Record<string, any> = {};

// export const txInFlight: Record<string, number> = {};

// const numberOr0 = (n: number | undefined): number => (Number.isNaN(n) ? 0 : n);

// export const getTxsInFlight = (): number =>
//   Object.values(txInFlight).reduce((a, b) => numberOr0(a) + numberOr0(b));

const trackerTheme = GaugeThemes.newTheme(
  GaugeThemes({
    hasUnicode: false,
    hasColor: true,
  })
);

export let topHash = "";
export let gatewayHeight: CassandraTypes.Long = toLong(0);
export let topHeight = 0;
export let currentHeight = 0;

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
let isPaused = false;

async function resolveFork(previousBlock: BlockType): Promise<void> {
  isPaused = true;
  const pprevBlock = await fetchBlockByHash(previousBlock.previous_block);

  const blockQueryResult = await cassandraClient.execute(
    `SELECT height FROM ${KEYSPACE}.block WHERE indep_hash=?`,
    [pprevBlock.indep_hash]
  );

  if (blockQueryResult.rowLength > 0) {
    log.info("fork diverges at " + blockQueryResult.rows[0].height.toString());

    // if (getTxsInFlight() > 0) {
    //   log.info(
    //     "waiting for " + getTxsInFlight() + " txs in flight to settle..."
    //   );
    //   await pWaitFor(() => getTxsInFlight() === 0, { interval: 200 });
    // }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const result = await cassandraClient.execute(
      `SELECT block_height,block_hash
       FROM ${KEYSPACE}.block_height_by_block_hash
       WHERE block_height > ${blockQueryResult.rows[0].height.toString()}
       ALLOW FILTERING`,
      [],
      { prepare: true }
    );

    log.info(
      `[fork recovery] abandoned blocks removal done, re-importing missing blocks...`
    );

    const nodeInfo = await getNodeInfo({});

    for (const newForkHeight of R.range(
      blockQueryResult.rows[0].height.toInt() + 1,
      typeof nodeInfo.height === "number"
        ? nodeInfo.height
        : Number.parseInt(nodeInfo.height)
    )) {
      await blockImportWorkerPool.single.importBlock(newForkHeight);
    }

    log.info(`[fork recovery] all done!`);
  } else {
    return await resolveFork(pprevBlock);
  }
}

let poller: Promise<void>;
let exited = false;

exitHook(() => {
  exited = true;
});

const pollNewBlocks = async (): Promise<void> => {
  if (isPaused) return;
  if (!isPollingStarted && !poller) {
    isPollingStarted = true;
    poller = pWhilst(
      () => !exited,
      async () => {
        try {
          /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
          await (pMinDelay as any)(await pollNewBlocks(), 120 * 1000);
        } catch (error) {
          console.error(error);
        }
      }
    );
    log.info("[sync] polling for new incoming blocks..");
  }

  const nodeInfo = await getNodeInfo({});

  if (!nodeInfo) {
    return;
  }

  topHeight = nodeInfo.height;

  [topHash, gatewayHeight] = await getMaxHeightBlock(cassandraClient);

  statusMapper.update({
    session: session_.uuid,
    gateway_height: `${gatewayHeight}`,
    arweave_height: `${topHeight}`,
  });

  if (nodeInfo.current !== topHash) {
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
      isPaused = false;
      log.info("blocks are back in sync!");
    } else {
      await blockImportWorkerPool.single.importBlock(nodeInfo.height);
    }
  }
};

async function detectFirstRun(): Promise<boolean> {
  const queryResponse = await cassandraClient.execute(
    `SELECT height
     FROM ${KEYSPACE}.block LIMIT 1`
  );
  return queryResponse && queryResponse.rowLength > 0 ? false : true;
}

async function findMissingBlocks(hashList: string[]): Promise<UnsyncedBlock[]> {
  const hashListObject = hashList.reduce((accumulator, hash, height) => {
    accumulator[height] = { height, hash };
    return accumulator;
  }, {} as Record<string, UnsyncedBlock>);

  log.info("[database] Looking for missing blocks...");
  const result = await cassandraClient.execute(
    `SELECT height, indep_hash, timestamp, txs
         FROM ${KEYSPACE}.block`,
    [],
    { prepare: true, executionProfile: "fast" }
  );

  for await (const rowResult of result) {
    const matchingRow = hashListObject[rowResult.height.toString()];

    if (
      matchingRow &&
      R.equals(matchingRow["hash"], rowResult.indep_hash) &&
      R.equals(matchingRow["height"], rowResult.height)
    ) {
      delete hashListObject[rowResult.height];
    } else {
      if (!matchingRow) {
        log.info(`Found missing block: ${rowResult.height}`);
      } else if (!R.equals(matchingRow["height"], rowResult.height)) {
        log.info(
          `Found mismatching block at: ${rowResult.height} because ${matchingRow["height"]} != ${rowResult.height}`
        );
      } else if (!R.equals(matchingRow["hash"], rowResult.indep_hash)) {
        log.info(
          `Found mismatching block at: ${rowResult.height} because ${matchingRow["hash"]} != ${rowResult.indep_hash}`
        );
      }
    }
  }
  return R.sortBy(R.prop("height"))(
    R.values(hashListObject) as UnsyncedBlock[]
  );
}

async function startGatewayNodeMode(): Promise<void> {
  // TODO: read stats from cassandra
  [topHash, gatewayHeight] = await getMaxHeightBlock(cassandraClient);
}

async function startManifestImportWorker(): Promise<void> {
  try {
    /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
    await (pMinDelay as any)(
      manifestImportWorkerPool.single.importManifests(),
      120 * 1000
    );
  } catch (error) {
    console.error(error);
  }

  return await startManifestImportWorker();
}

export async function startSync({
  session,
}: {
  session: { uuid: CassandraTypes.TimeUuid };
}): Promise<void> {
  session_ = session;

  if (isGatewayNodeModeEnabled) {
    log.info(
      "[sync] vartex gateway-node mode is enabled so no syncing will be performed (aka read-only mode)"
    );
    await startGatewayNodeMode();
    return;
  }

  // wait until worker threads are ready
  await Promise.all(R.map(R.prop("promise"))(R.values(workerReadyPromises)));

  const hashList: string[] = await getHashList({});
  topHeight = hashList.length;
  const firstRun = await detectFirstRun();
  let lastBlock: CassandraTypes.Long = toLong(-1);

  try {
    const result_ = await getMaxHeightBlock(cassandraClient);
    lastBlock = result_[1];
  } catch {}

  if (!firstRun && !developmentSyncLength) {
    const isMaybeMissingBlocks = await Dr.checkForBlockGaps(lastBlock);

    if (isMaybeMissingBlocks) {
      const blockGap = await Dr.findBlockGaps(lastBlock);
      if (!R.isEmpty(blockGap)) {
        console.error("Repairing missing block(s):", blockGap);
        await Promise.all(
          blockGap.map((height) =>
            blockImportWorkerPool.single.importBlock(height)
          )
        );
      }
      try {
        const result_ = await getMaxHeightBlock(cassandraClient);
        lastBlock = result_[1];
      } catch {}
    }
  }

  const gauge = new Gauge(process.stderr, {
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
    : await findMissingBlocks(hashList);

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
    gatewayHeight = initialLastBlock;
    statusMapper.update({
      session: session.uuid,
      gateway_height: `${gatewayHeight}`,
    });
  } else {
    gatewayHeight = lastBlock;
    statusMapper.update({
      session: session.uuid,
      gateway_height: `${lastBlock}`,
    });
  }

  // wait a minute until starting to poll for unimported manifest
  !env.OFFLOAD_MANIFEST_IMPORT &&
    setTimeout(startManifestImportWorker, 60 * 1000);

  if (firstRun) {
    log.info(
      "[sync] database seems to be empty, starting preperations for import..."
    );
  } else if (R.isEmpty(unsyncedBlocks)) {
    log.info("[sync] fully synced db");
    pollNewBlocks();
    return;
  } else {
    log.info(
      `[sync] missing ${unsyncedBlocks.length} blocks, starting sync...`
    );
  }

  gauge.enable();

  const limit = pLimit(2);
  const importBlocksJob = unsyncedBlocks.map(
    async ({ height }: { height: number }) =>
      limit(async () => {
        statusMapper.update({
          session: session.uuid,
          current_imports: [...currentImports].map((x) => `${x}`),
        });
        currentImports.add(height);
        await blockImportWorkerPool.single.importBlock(height);

        currentImports.delete(height);

        currentHeight = Math.max(currentHeight, height);

        statusMapper.update({
          session: session.uuid,
          status: "OK",
          gateway_height: `${currentHeight}`,
          arweave_height: `${topHeight}`,
          current_imports: [...currentImports].map((x) => `${x}`),
        });
        gauge.show(`blocks: ${currentHeight}/${topHeight}\t tx: FIXME`);
      })
  );

  await Promise.all(importBlocksJob);

  log.info("Database fully in sync with block_list");
  !isPollingStarted && pollNewBlocks();
}
