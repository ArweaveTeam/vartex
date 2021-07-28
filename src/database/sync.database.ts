import * as R from 'rambda';
import Fluture from 'fluture';
import * as F from 'fluture';
import { DataItemJson } from 'arweave-bundles';
import Gauge from 'gauge';
import GaugeThemes from 'gauge/themes.js';
import { config } from 'dotenv';
import { types as CassandraTypes } from 'cassandra-driver';
import { log } from '../utility/log.utility.js';
import { ansBundles } from '../utility/ans.utility.js';
import { mkdir } from '../utility/file.utility.js';
import { sleep } from '../utility/sleep.utility.js';
import { getHashList, getNodeInfo } from '../query/node.query.js';
import { getBlock as queryGetBlock } from '../query/block.query.js';
import {
  getTransaction,
  getTxOffset,
  tagValue,
  Tag,
} from '../query/transaction.query.js';
import { getDataFromChunks } from '../query/node.query.js';
import { ImportQueue, QueueState } from '../types/cassandra.types.js';
import {
  cassandraClient,
  getMaxHeightBlock,
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from './cassandra.database.js';
import { pollStatusMapper } from './mapper.database.js';
import { cacheANSEntries } from '../caching/ans.entry.caching.js';

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
export let topHash: string = '';
export let topHeight: CassandraTypes.Long = toLong(0);
export let syncHash: string = '';
export let syncHeight: CassandraTypes.Long = toLong(0);
// export let unsyncedBlocks: string[] = [];
export let timer = setTimeout(() => {}, 0);

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
const blockQueue: ImportQueue = {};
const txQueue: ImportQueue = {};
const tagsQueue: ImportQueue = {};
const blockQueueState: QueueState = { isProcessing: false, isStarted: false };
const txQueueState: QueueState = { isProcessing: false, isStarted: false };
const tagsQueueState: QueueState = { isProcessing: false, isStarted: false };

const createQueue = (
  queueSource: Record<number, any>,
  queueState: QueueState
) => (): void => {
  if (queueState.isProcessing) return;

  const items: number[] = R.keys(queueSource);
  if (items.length > 0) {
    // name could be misleading as this can be a batch of db-batches
    const batchPrio: number = items.sort()[0];
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

const processBlockQueue = createQueue(blockQueue, blockQueueState);
const processTxQueue = createQueue(txQueue, txQueueState);
const processTagsQueue = createQueue(tagsQueue, tagsQueueState);

function startQueueProcessors() {
  if (!blockQueueState.isStarted) {
    blockQueueState.isStarted = true;
    setInterval(processBlockQueue, 10);
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
  [topHash, topHeight] = await getMaxHeightBlock();
  if (!nodeInfo || nodeInfo.current === topHash) {
    // wait 5 seconds before polling again
    log.info(
      '[poll] fully aligned at block ' +
        topHash +
        ' height ' +
        topHeight.toString()
    );
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

      (blockQueue as any)[
        typeof newBlock.height === 'string'
          ? parseInt(newBlock.height)
          : newBlock.height || 0
      ] = blockQueryCallback;
    } else {
      console.error('FATAL Querying for new block failed');
    }
    await new Promise((res) => setTimeout(res, 5 * 1000));
    return startPolling();
  }
}

const detectFirstRun = async (): Promise<boolean> => {
  const queryResponse = await cassandraClient.execute(
    `SELECT * FROM gateway.block LIMIT 1`
  );
  if (!queryResponse.rows || R.isEmpty(queryResponse.rows)) {
    return true;
  } else {
    return false;
  }
};

interface UnsyncedBlock {
  height: number;
  hash: string;
}

interface DeleteRowData extends UnsyncedBlock {
  timestamp: CassandraTypes.Long;
}

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
        `SELECT height,indep_hash,timestamp FROM gateway.block`,
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
            !matchingRow ||
            !R.equals(matchingRow, { hash: row.indep_hash, height: row.height })
          ) {
            scheduledForDelete.push({
              hash: row.hash,
              height: row.height,
              timestamp: row.timestamp,
            });
          } else {
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
                try {
                  await cassandraClient.execute(
                    `DELETE FROM gateway.block WHERE indep_hash='${hash}' IF EXISTS`
                  );
                  await cassandraClient.execute(
                    `DELETE FROM gateway.block_gql_asc WHERE indep_hash='${hash}' AND height=${height} AND timestamp=${timestamp} IF EXISTS`
                  );
                  await cassandraClient.execute(
                    `DELETE FROM gateway.block_gql_desc WHERE indep_hash='${hash}' AND height=${height} AND timestamp=${timestamp} IF EXISTS`
                  );
                } catch (error) {
                  console.error(
                    'FATAL error while deleting block with hash:',
                    hash
                  );
                  console.error(error);
                  process.exit(1);
                }
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

  // unsyncedBlocks = firstRun
  //   ? hashList
  //   : developmentSyncLength && typeof developmentSyncLength === 'number'
  //   ? R.slice(hashListLength - developmentSyncLength, hashListLength)(hashList)
  //   : lastSessionHash && typeof lastSessionHash === 'string'
  //   ? (R.splitWhen(R.equals(lastSessionHash))(hashList)[1] as string[])
  //   : (hashList as string[]);

  // const unsyncedBlockHeights = R.range(
  //   firstRun && !developmentSyncLength
  //     ? 0
  //     : Math.max(
  //         0,
  //         developmentSyncLength
  //           ? hashListLength - developmentSyncLength
  //           : R.findIndex(R.equals(lastSessionHash), hashList)
  //       ),
  //   hashListLength
  // );

  F.fork((reason: string | void) => {
    console.error('Fatal', reason || '');
    process.exit(1);
  })(() => {
    gauge.disable();
    log.info(`Database fully in sync with block_list`);
    !isPollingStarted && startPolling();
  })(
    F.parallel(
      (isNaN as any)(process.env['PARALLEL'])
        ? 36
        : parseInt(process.env['PARALLEL'] || '36')
    )(
      unsyncedBlocks.map(({ height, hash }) => {
        const progress = `${height}/${hashList.length}`;
        return storeBlock(height, hash, progress, gauge);
      })
    )
  );
}

export function storeBlock(
  height: number,
  hash: string,
  progress: string,
  gauge: any
): Promise<unknown> {
  return Fluture(
    (reject: (reason: string | void) => void, resolve: () => void) => {
      let isCancelled = false;
      function getBlock(retry = 0) {
        !isCancelled &&
          queryGetBlock({
            hash,
            height,
            gauge,
            completed: progress,
          })
            .then((newSyncBlock) => {
              if (newSyncBlock) {
                syncHeight = toLong(newSyncBlock.height);
                (blockQueue as any)[
                  newSyncBlock.height.toString()
                ] = makeBlockImportQuery(newSyncBlock);
                (newSyncBlock.txs || []).forEach((txId: string) => {
                  storeTransaction(txId, newSyncBlock);
                });
                resolve();
              } else {
                new Promise((res) => setTimeout(res, 100)).then(() => {
                  if (retry >= 250) {
                    log.info(`Could not retrieve block at height ${height}`);
                    reject('Failed to fetch block after 250 retries');
                  } else {
                    return getBlock(retry + 1);
                  }
                });
              }
            })
            .catch((error) => {
              log.error(`error ${error}`);
              if (SIGKILL === false) {
                if (retry >= 250) {
                  log.info(`there were problems retrieving ${height}`);
                  reject(error);
                } else {
                  return getBlock(retry + 1);
                }
              }
            });
      }

      getBlock();
      return () => {
        isCancelled = true;
      };
    }
  );
}

export async function storeTransaction(
  txId: string,
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
    txQueue[txId] = makeTxImportQuery(
      R.mergeAll([currentTransaction, maybeTxOffset]),
      blockData
    );
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
