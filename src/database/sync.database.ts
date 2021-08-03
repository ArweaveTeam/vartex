import * as R from 'rambda';
import Fluture from 'fluture';
import * as F from 'fluture';
import PriorityQueue from '../utility/priority.queue';
import pWaitFor from 'p-wait-for';
import { DataItemJson } from 'arweave-bundles';
import Gauge from 'gauge';
import GaugeThemes from 'gauge/themes';
import { config } from 'dotenv';
import { types as CassandraTypes } from 'cassandra-driver';
import { KEYSPACE } from '../constants';
import { MAX_TX_PER_BLOCK } from './constants.database';
import { log } from '../utility/log.utility';
import { ansBundles } from '../utility/ans.utility';
import { mkdir } from '../utility/file.utility';
import { sleep } from '../utility/sleep.utility';
import { getHashList, getNodeInfo } from '../query/node.query';
import { getBlock as queryGetBlock } from '../query/block.query';
import {
  getTransaction,
  getTxOffset,
  tagValue,
  Tag,
} from '../query/transaction.query';
import { getDataFromChunks } from '../query/node.query';
import { ImportQueue, QueueState } from '../types/cassandra.types';
import {
  cassandraClient,
  getMaxHeightBlock,
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from './cassandra.database';
import { blockMapper, blockHeightToHashMapper } from './mapper.database';
import { cacheANSEntries } from '../caching/ans.entry.caching';

config();
mkdir('cache');

const trackerTheme = GaugeThemes.newTheme(
  GaugeThemes({
    hasUnicode: false,
    hasColor: true,
  })
);

export let SIGINT: boolean = false;
export let SIGKILL: boolean = false;
const PARALLEL = (isNaN as any)(process.env['PARALLEL'])
  ? 36
  : parseInt(process.env['PARALLEL'] || '36');

export let topHash: string = '';
export let topHeight: CassandraTypes.Long = toLong(0);

const developmentSyncLength: number | undefined =
  !process.env['DEVELOPMENT_SYNC_LENGTH'] ||
  R.isEmpty(process.env['DEVELOPMENT_SYNC_LENGTH'])
    ? undefined
    : parseInt(process.env['DEVELOPMENT_SYNC_LENGTH'] as string);

if (developmentSyncLength === NaN) {
  console.error('Development sync range variable produced, illegal value NaN');
  process.exit(1);
}

let isQueueProcessorStarted = false;
let isPollingStarted = false;
let isSyncing: boolean = true;
let isRepairing: boolean = false;

const blockQueue = new PriorityQueue(function (
  a: { height: CassandraTypes.Long },
  b: { height: CassandraTypes.Long }
) {
  return a.height.compare(b.height);
});

const txQueue = new PriorityQueue(function (
  a: { height: CassandraTypes.Long; index: number },
  b: { height: CassandraTypes.Long; index: number }
) {
  if (a.height === b.height) {
    return a.index - b.index;
  } else {
    return a.height.compare(b.height);
  }
});

// const txQueue: ImportQueue = {};
const tagsQueue: ImportQueue = {};
const blockQueueState: QueueState = {
  isProcessing: false,
  isStarted: false,
  lastBatchPrio: toLong(-1),
};
const txQueueState: QueueState = {
  isProcessing: false,
  isStarted: false,
  lastBatchPrio: toLong(-1),
};

const tagsQueueState: QueueState = { isProcessing: false, isStarted: false };

const createQueue = (
  queueSource: Record<string, any>,
  queueState: QueueState
) => (): void => {
  if (queueState.isProcessing) return;

  const items: string[] = R.keys(queueSource);
  if (items.length > 0) {
    // name could be misleading as this can be a batch of db-batches
    const batchPrio = items.sort()[0];
    queueState.isProcessing = true;
    const batch = queueSource[batchPrio]();
    (Array.isArray(batch) ? Promise.all(batch) : batch)
      .then(function (ret: any) {
        delete (queueSource as any)[batchPrio];
        queueState.isProcessing = false;
      })
      .catch(function (err: any) {
        console.error('FATAL', err);
        process.exit(1);
      });
  }
};

const createPriorityQueue = (queueSource: any, queueState: QueueState) => (
  nextHeight: CassandraTypes.Long
): void => {
  if (
    queueSource.isEmpty() ||
    !nextHeight ||
    isRepairing ||
    queueState.isProcessing
  ) {
    return;
  }
  queueSource.sortQueue();
  const peek = !queueSource.isEmpty() && queueSource.peek();

  if (
    CassandraTypes.Long.isLong(peek.height) &&
    nextHeight.equals(peek.height)
  ) {
    queueState.isProcessing = true;
    queueState.lastBatchPrio = peek.height;

    Promise.all(peek.callback()).then(() => {
      queueSource.pop();
      queueState.isProcessing = false;

      if (peek.height.gt(topHeight)) {
        topHeight = peek.height;
      }
    });
  }
};

const processBlockQueue = createPriorityQueue(blockQueue, blockQueueState);
const processTxQueue = createPriorityQueue(txQueue, txQueueState);
const processTagsQueue = createQueue(tagsQueue, tagsQueueState);

function startQueueProcessors() {
  if (!blockQueueState.isStarted) {
    blockQueueState.isStarted = true;
    setInterval(function processQ() {
      !blockQueueState.isProcessing &&
        processBlockQueue(blockQueueState.lastBatchPrio.add(1));
    }, 50);
  }
  if (!txQueueState.isStarted) {
    txQueueState.isStarted = true;
    setInterval(processTxQueue, 10);
  }
  if (!tagsQueueState.isStarted) {
    tagsQueueState.isStarted = true;
    setInterval(processTagsQueue, 10);
  }
}

async function startPolling(): Promise<void> {
  if (!isPollingStarted) {
    isPollingStarted = true;
  }
  // const lastPollStatus = R.head(await pollStatusMapper.findAll());
  const nodeInfo = await getNodeInfo({ keepAlive: true });

  if (!nodeInfo) {
    await new Promise((res) => setTimeout(res, 5 * 1000));
    return startPolling();
  }

  [topHash, topHeight] = await getMaxHeightBlock();
  if (nodeInfo.current === topHash) {
    // wait 5 seconds before polling again
    log.info('[poll] fully aligned at height ' + topHeight.toString());
    await new Promise((res) => setTimeout(res, 5 * 1000));
    return startPolling();
  } else if (nodeInfo) {
    // TODO fork recovery
    const newBlock = await queryGetBlock({
      height: nodeInfo.height,
      hash: nodeInfo.current,
    });
    if (newBlock !== undefined) {
      const blockQueryCallback = makeBlockImportQuery(newBlock);
      blockQueue.enqueue({
        callback: blockQueryCallback,
        height:
          newBlock.height !== null && !isNaN(newBlock.height)
            ? toLong(newBlock.height)
            : toLong(0),
      });
    } else {
      console.error('Querying for new tx failed');
    }
    await new Promise((res) => setTimeout(res, 5 * 1000));
    return startPolling();
  }
}

const detectFirstRun = async (): Promise<boolean> => {
  const queryResponse = await cassandraClient.execute(
    `SELECT height FROM ${KEYSPACE}.block LIMIT 1`
  );
  if (queryResponse && queryResponse.rowLength > 0) {
    return false;
  } else {
    return true;
  }
};

interface UnsyncedBlock {
  height: number;
  hash: string;
}

interface DeleteRowData extends UnsyncedBlock {
  timestamp: CassandraTypes.Long;
}

const deleteBlock = async ({
  hash,
  height,
  timestamp,
}: {
  hash: string;
  height: string;
  timestamp: string;
}): Promise<void> => {
  try {
    await cassandraClient.execute(
      `DELETE FROM ${KEYSPACE}.block WHERE indep_hash='${hash}' IF EXISTS`
    );
    await cassandraClient.execute(
      `DELETE FROM ${KEYSPACE}.block_gql_asc WHERE indep_hash='${hash}' AND height=${height} AND timestamp=${timestamp} IF EXISTS`
    );
    await cassandraClient.execute(
      `DELETE FROM ${KEYSPACE}.block_gql_desc WHERE indep_hash='${hash}' AND height=${height} AND timestamp=${timestamp} IF EXISTS`
    );
  } catch (error) {
    console.error('FATAL error while deleting block with hash:', hash);
    console.error(error);
    process.exit(1);
  }
};

const fixNonLinearBlockOrder = (): Promise<void> => {
  let cutAfter: any;
  let lastRow: any;

  return new Promise(
    (resolve: (val?: any) => void, reject: (err: string) => void) => {
      cassandraClient.eachRow(
        `SELECT height,indep_hash,timestamp FROM ${KEYSPACE}.block`,
        [],
        {
          autoPage: true,
          prepare: false,
          executionProfile: 'fast',
        },
        function (n, row) {
          if (!lastRow || row.height.add(1).equals(lastRow.height)) {
            lastRow = row;
          } else {
            cutAfter = lastRow.height.lt(row.height) ? lastRow : row;
            lastRow = row;
          }
        },
        async function (err, res) {
          const maxHeightRes = await cassandraClient.execute(
            `SELECT MAX(height) FROM ${KEYSPACE}.block_gql ALLOW FILTERING`,
            { executionProfile: 'fast' }
          );
          const maxHeight =
            maxHeightRes.rows[0]['system.max(height)'] || toLong(0);

          for (const blockHeightToDelete of R.range(
            cutAfter ? cutAfter.height.sub(1).toInt() : 0,
            maxHeight.add(1).toInt()
          )) {
            const { block_hash } = await blockHeightToHashMapper.get({
              block_height: blockHeightToDelete,
            });
            const currentBlock = await blockMapper.get({
              height: blockHeightToDelete,
              indep_hash: block_hash,
            });
            await deleteBlock({
              hash: block_hash.toString(),
              height: blockHeightToDelete.toString(),
              timestamp: currentBlock.timestamp.toString(),
            });
          }
        }
      );
    }
  );
};

const findMissingBlocks = (
  hashList: string[],
  gauge: any
): Promise<UnsyncedBlock[]> => {
  const hashListObj = hashList.reduce((acc, hash, height) => {
    acc[height] = { height, hash };
    return acc;
  }, {});
  const scheduledForDelete: DeleteRowData[] = [];

  gauge.enable();
  log.info(`[database] Looking for missing blocks...`);
  return new Promise(
    (resolve: (val?: any) => void, reject: (err: string) => void) => {
      cassandraClient.eachRow(
        `SELECT height,indep_hash,timestamp FROM ${KEYSPACE}.block`,
        [],
        {
          autoPage: true,
          prepare: false,
          executionProfile: 'fast',
        },
        async function (n, row) {
          gauge.show(`Looking for missing blocks: ${n}/${hashList.length}`);
          if (SIGINT || SIGKILL) {
            process.exit(1);
          }
          const matchingRow = hashListObj[row.height];

          // mismatch situation, remove both of them from the db
          if (
            matchingRow &&
            (!R.equals(matchingRow['hash'], row.indep_hash) ||
              !R.equals(matchingRow['height'], row.height))
          ) {
            scheduledForDelete.push({
              hash: row.hash,
              height: row.height,
              timestamp: row.timestamp,
            });
          } else if (
            matchingRow &&
            R.equals(matchingRow['hash'], row.indep_hash) &&
            R.equals(matchingRow['height'], row.height)
          ) {
            // log.info('DEQUEUEING' + row.height);
            delete hashListObj[row.height];
          }
        },
        async function (err, res) {
          gauge.disable();
          if (err) {
            reject((err || '').toString());
          } else {
            if (!R.isEmpty(scheduledForDelete)) {
              const uniqueDeletes = R.uniq(scheduledForDelete);
              log.info(
                `[sync] ${uniqueDeletes.length} inconsistent rows scheduled for deletion...`
              );
              for (const { hash, height, timestamp } of uniqueDeletes) {
                await deleteBlock({
                  hash: hash.toString(),
                  height: height.toString(),
                  timestamp: timestamp.toString(),
                });
              }
            }

            resolve(R.pipe(R.values, R.sortBy(R.prop('height')))(hashListObj));
          }
        }
      );
    }
  );
};

export async function startSync() {
  signalHook();
  startQueueProcessors();

  const hashList: string[] = await getHashList({});
  const firstRun = await detectFirstRun();
  const lastBlock: CassandraTypes.Long = firstRun
    ? toLong(-1)
    : (
        await cassandraClient.execute(
          `SELECT height FROM ${KEYSPACE}.block LIMIT 1`
        )
      ).rows[0].height;

  const gauge = new Gauge(process.stderr, {
    template: [
      { type: 'progressbar', length: 0 },
      { type: 'activityIndicator', kerning: 1, length: 2 },
      { type: 'section', kerning: 1, default: '' },
      { type: 'subsection', kerning: 1, default: '' },
    ],
  });
  gauge.setTheme(trackerTheme);

  const unsyncedBlocks: UnsyncedBlock[] = firstRun
    ? hashList.map((hash, height) => ({ hash, height }))
    : await findMissingBlocks(hashList, gauge);

  const initialLastBlock = toLong(unsyncedBlocks[0].height).add(-1);
  blockQueueState.lastBatchPrio = initialLastBlock;
  txQueueState.lastBatchPrio = initialLastBlock;

  if (firstRun) {
    log.info(
      `[sync] database seems to be empty, starting preperations for import...`
    );
  } else if (R.isEmpty(unsyncedBlocks)) {
    log.info(`[sync] fully synced db`);
    startPolling();
    return;
  } else {
    log.info(
      `[sync] missing ${unsyncedBlocks.length} blocks, starting sync...`
    );
  }

  gauge.enable();

  const hashListLength = hashList.length;

  F.fork((reason: string | void) => {
    console.error('Fatal', reason || '');
    process.exit(1);
  })(() => {
    gauge.disable();
    log.info(`Database fully in sync with block_list`);
    !isPollingStarted && startPolling();
  })(
    F.parallel(PARALLEL)(
      unsyncedBlocks.map(({ height, hash }) => {
        const getProgress = () =>
          `${height}/${hashList.length}/${blockQueue.getSize()}`;
        return storeBlock(height, hash, getProgress, gauge);
      })
    )
  );
}

export function storeBlock(
  height: number,
  hash: string,
  getProgress: () => string,
  gauge: any
): unknown {
  let isCancelled = false;
  return Fluture(
    (reject: (reason: string | void) => void, resolve: () => void) => {
      async function getBlock(retry = 0) {
        if (isRepairing || isCancelled) {
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
          blockQueue.enqueue({
            callback: makeBlockImportQuery(newSyncBlock),
            height: newSyncBlockHeight,
          });
          (newSyncBlock.txs || []).forEach((txId: string, index: number) => {
            const txIndex = newSyncBlockHeight.mul(MAX_TX_PER_BLOCK).add(index);
            storeTransaction(txId, txIndex, newSyncBlockHeight, newSyncBlock);
          });
          return;
        } else {
          await new Promise((res) => setTimeout(res, 100));
          if (retry >= 250) {
            log.info(`Could not retrieve block at height ${height}`);
            reject('Failed to fetch block after 250 retries');
          } else {
            return await getBlock(retry + 1);
          }
        }
      }

      pWaitFor(() => blockQueue.getSize() < PARALLEL + 1)
        .then(() => getBlock())
        .then(resolve);

      return () => {
        isCancelled = true;
      };
    }
  );
}

export async function storeTransaction(
  txId: string,
  txIndex: CassandraTypes.Long,
  height: CassandraTypes.Long,
  blockData: { [k: string]: any }
) {
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
      callback: makeTxImportQuery(txIndex, currentTransaction, blockData),
      height: toLong(txIndex),
    });
  } else {
    console.error('Fatal network error');
    process.exit(1);
  }
}

export async function processAns(
  id: string,
  height: number,
  retry: boolean = true
) {
  try {
    const ansPayload = await getDataFromChunks({
      id,
      startOffset: CassandraTypes.Long.fromNumber(0), // FIXEME
      endOffset: CassandraTypes.Long.fromNumber(0), // FIXME
    });
    const ansTxs = await ansBundles.unbundleData(ansPayload.toString('utf-8'));

    await cacheANSEntries(ansTxs);
    await processANSTransaction(ansTxs, height);
  } catch (error) {
    if (retry) {
      await processAns(id, height, false);
    } else {
      log.info(
        `[database] malformed ANS payload at height ${height} for tx ${id}`
      );
      // streams.rescan.cache.write(`${id}|${height}|ans\n`);
    }
  }
}

export async function processANSTransaction(
  ansTxs: Array<DataItemJson>,
  height: number
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
  process.on('SIGINT', () => {
    log.info(
      '[database] ensuring all blocks are stored before exit, you may see some extra output in console'
    );
    SIGKILL = true;
    setInterval(() => {
      if (SIGINT === false) {
        log.info('[database] block sync state preserved, now exiting');
        console.log('');
        process.exit();
      }
    }, 100);
  });
}
