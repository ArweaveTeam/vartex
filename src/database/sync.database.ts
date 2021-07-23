import * as R from 'rambda';
import Fluture from 'fluture';
import * as F from 'fluture';
import { DataItemJson } from 'arweave-bundles';
import Gauge from 'gauge';
import GaugeThemes from 'gauge/themes';
import { config } from 'dotenv';
import { types as CassandraTypes } from 'cassandra-driver';
import {
  serializeBlock,
  serializeTransaction,
  serializeAnsTransaction,
  serializeTags,
} from '../utility/serialize.utility';
import { log } from '../utility/log.utility';
import { ansBundles } from '../utility/ans.utility';
import { mkdir } from '../utility/file.utility';
import { sleep } from '../utility/sleep.utility';
import { getHashList, getNodeInfo } from '../query/node.query';
import { getBlock as queryGetBlock } from '../query/block.query';
import { transaction, tagValue, Tag } from '../query/transaction.query';
import { getDataFromChunks } from '../query/node.query';
import {
  cassandraClient,
  getMaxHeightBlock,
  getPlaceholderCount,
  makeBlockImportQuery,
  newSession,
  toLong,
} from './cassandra.database';
import { DatabaseTag } from './transaction.database';
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
export let topHeight = 0;
export let currentHash: string = '';
export let currentHeight = 0;
export let unsyncedBlocks = [];
export let timer = setTimeout(() => {}, 0);

interface QueueState {
  isProcessing: boolean;
  isStarted: boolean;
}

const developmentSyncStart: number | undefined = R.isEmpty(
  process.env['DEVELOPMENT_SYNC_START']
)
  ? undefined
  : parseInt(process.env['DEVELOPMENT_SYNC_START'] as string);
const developmentSyncEnd: number | undefined = R.isEmpty(
  process.env['DEVELOPMENT_SYNC_END']
)
  ? undefined
  : parseInt(process.env['DEVELOPMENT_SYNC_END'] as string);

if (developmentSyncStart == NaN || developmentSyncEnd === NaN) {
  console.error('Development sync range variables produced, illegal value NaN');
}

let isQueueProcessorStarted = false;
const blockQueue: { [v: number]: any } = {};
const txQueue: { [v: number]: any } = {};
const tagsQueue: { [v: number]: any } = {};
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
    const batch = blockQueue[batchPrio]();
    (Array.isArray(batch) ? Promise.all(batch) : batch)
      .then(function (ret: any) {
        delete (blockQueue as any)[batchPrio];
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
  const nodeInfo = await getNodeInfo({ fullySynced: true });
  if (!nodeInfo || nodeInfo.current === currentHash) {
    // wait 5 seconds before polling again
    await new Promise((res) => setTimeout(res, 5 * 1000));
    return startPolling();
  } else if (nodeInfo) {
    // TODO fork recovery
    const newBlock = await queryGetBlock({
      height: nodeInfo.height,
      hash: nodeInfo.current,
    });
    if (newBlock) {
      currentHeight = Math.max(currentHeight, newBlock.height);
      (blockQueue as any)[
        typeof newBlock.height === 'string'
          ? parseInt(newBlock.height)
          : newBlock.height || 0
      ] = makeBlockImportQuery(newBlock);
    }
    await new Promise((res) => setTimeout(res, 5 * 1000));
    return startPolling();
  }
}

async function prepareBlockStatuses(
  unsyncedBlocks: number[],
  hashList: string[]
) {
  log.info(
    `[database] intitializing placeholder fields for blocks in cassandra...`
  );

  const gauge = new Gauge(process.stderr, {
    template: [
      { type: 'progressbar', length: 0 },
      { type: 'activityIndicator', kerning: 1, length: 2 },
      { type: 'section', kerning: 1, default: '' },
      { type: 'subsection', kerning: 1, default: '' },
    ],
  });
  gauge.setTheme(trackerTheme);
  gauge.enable();
  for (const blockHeight of unsyncedBlocks) {
    gauge.show(`${blockHeight}/${topHeight}`);

    await cassandraClient.execute(
      `INSERT INTO gateway.block_status (block_height, block_hash, synced) VALUES (?, ?, ?) IF NOT EXISTS`,
      [toLong(blockHeight), hashList[blockHeight], false]
    );
  }
  gauge.disable();
  log.info(`[database] done intitializing`);
}

export function startSync() {
  startQueueProcessors();
  newSession().then(
    ([lastSessionHash, lastSessionHeight]: [string, number]) => {
      const startHeight = lastSessionHeight || 0;
      log.info(`[database] starting sync at block: ${startHeight}`);
      signalHook();
      const gauge = new Gauge(process.stderr, {
        template: [
          { type: 'progressbar', length: 0 },
          { type: 'activityIndicator', kerning: 1, length: 2 },
          { type: 'section', kerning: 1, default: '' },
          { type: 'subsection', kerning: 1, default: '' },
        ],
      });
      gauge.setTheme(trackerTheme);
      gauge.enable();
      getHashList({}).then((hashList) => {
        if (hashList) {
          topHeight = hashList.length;
          if (startHeight < hashList.length) {
            const unsyncedBlocks: number[] = R.range(
              developmentSyncStart ? developmentSyncStart : startHeight,
              developmentSyncEnd ? developmentSyncEnd : topHeight
            );
            getPlaceholderCount().then((currentCount) => {
              prepareBlockStatuses(
                R.range(
                  developmentSyncStart
                    ? developmentSyncStart
                    : currentCount.toInt(),
                  developmentSyncEnd ? developmentSyncEnd : topHeight
                ),
                hashList
              ).then(() => {
                F.fork((reason: string | void) => {
                  console.error('Fatal', reason || '');
                  process.exit(1);
                })(() => {
                  gauge.disable();
                  log.info(
                    `Database fully in sync with block_list height ${
                      developmentSyncEnd ? developmentSyncEnd : topHeight
                    }`
                  );
                  // startPolling();
                })(
                  F.parallel(
                    (isNaN as any)(process.env['PARALLEL'])
                      ? 36
                      : parseInt(process.env['PARALLEL'] || '36')
                  )(
                    R.slice(
                      0,
                      // topHeight - 50
                      100
                    )(unsyncedBlocks).map((height) => {
                      // WIP: we store the last 50 in mute-able tables
                      return storeBlock(height, hashList, gauge);
                    })
                  )
                );
              });
            });
          } else {
            console.log(
              'database was found to be in sync, starting to poll for new blocks...'
            );
            startPolling();
          }
        } else {
          console.error(
            'Failed to establish any connection to Nodes after 100 retries'
          );
          process.exit(1);
        }
      });
    }
  );
}

export function storeBlock(
  height: number,
  hashList: string[],
  gauge: any
): Promise<void> {
  const syncDatabaseJobTracker = `${height}/${hashList.length}`;
  return Fluture(
    (reject: (reason: string | void) => void, resolve: () => void) => {
      let isCancelled = false;
      function getBlock(retry = 0) {
        !isCancelled &&
          queryGetBlock({
            hash: hashList[height],
            height,
            gauge,
            completed: syncDatabaseJobTracker,
          })
            .then((currentBlock) => {
              if (currentBlock) {
                currentHeight = Math.max(currentHeight, currentBlock.height);
                const thisBlockHeight =
                  typeof currentBlock.height === 'string'
                    ? parseInt(currentBlock.height)
                    : currentBlock.height || 0;
                (blockQueue as any)[thisBlockHeight] = makeBlockImportQuery(
                  currentBlock
                );
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
      // console.log('fetching', height, hashList.slice(0, 10));
      getBlock();
      return () => {
        isCancelled = true;
      };
    }
  );
}

export async function storeTransaction(tx: string, height: number) {
  const currentTransaction = await transaction(tx);
  if (currentTransaction) {
    const { formattedTransaction, preservedTags, input } = serializeTransaction(
      currentTransaction,
      height
    );

    // streams.transaction.cache.write(input);

    storeTags(formattedTransaction.id, preservedTags);

    const ans102 = tagValue(preservedTags, 'Bundle-Type') === 'ANS-102';

    if (ans102) {
      await processAns(formattedTransaction.id, height);
    }
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
    const ansPayload = await getDataFromChunks(id);
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
    const ansTx = ansTxs[i];
    const { ansTags, input } = serializeAnsTransaction(ansTx, height);

    // streams.transaction.cache.write(input);

    for (let ii = 0; ii < ansTags.length; ii++) {
      const ansTag = ansTags[ii];
      const { name, value } = ansTag;

      const tag: DatabaseTag = {
        tx_id: ansTx.id,
        index: ii,
        name: name || '',
        value: value || '',
      };

      const input = `"${tag.tx_id}"|"${tag.index}"|"${tag.name}"|"${tag.value}"\n`;

      // streams.tags.cache.write(input);
    }
  }
}

export function storeTags(tx_id: string, tags: Array<Tag>) {
  for (let i = 0; i < tags.length; i++) {
    const tag = tags[i];
    const { input } = serializeTags(tx_id, i, tag);
    // streams.tags.cache.write(input);
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
