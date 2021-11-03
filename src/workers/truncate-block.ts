import * as R from "rambda";
import { types as CassandraTypes } from "cassandra-driver";
import { MessagesFromParent, MessagesFromWorker } from "./message-types";
import { ownerToAddress } from "../utility/encoding";
import { DropTagQueryParameters, dropTagQuery } from "../database/tags-mapper";
import { DropTxsQueryParameters, dropTxsQuery } from "../database/txs-mapper";
import {
  cassandraClient,
  blockMapper,
  blockSortedAscMapper,
  blockSortedDescMapper,
  blockQueueMapper,
  blockHeightToHashMapper,
  manifestMapper,
  manifestQueueMapper,
  permawebPathMapper,
  transactionMapper,
  txsSortedAscMapper,
  txsSortedDescMapper,
  txOffsetMapper,
} from "./mapper";
import { toLong } from "../database/utils";
import { getMessenger } from "../gatsby-worker/child";
import { mkWorkerLog } from "../utility/log";

enum TruncateBlockReturnCode {
  OK,
  REQUEUE,
  DEQUEUE,
}

let messenger = getMessenger<MessagesFromParent, MessagesFromWorker>();

if (messenger) {
  messenger.sendMessage({
    type: "worker:ready",
  });
} else {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (messenger as any) = { sendMessage: console.log };
}

const log = mkWorkerLog(messenger);

export async function truncateBlock(
  blockHash: string
): TruncateBlockReturnCode {
  const abandonedBlock = await blockMapper.get({
    indep_hash: blockHash,
  });

  try {
    // dequeue it right away
    await blockQueueMapper.remove({
      block_hash: blockHash,
      block_height: abandonedBlock.height,
    });
    log(`block import of ${blockHash} was dequeued`);
  } catch {}

  if (!abandonedBlock) {
    log(
      `block truncation of ${blockHash}. The block wasn't ever imported, dequeing...`
    );
    return TruncateBlockReturnCode.DEQUEUE;
  }

  const blockHashMapperResult = await blockHeightToHashMapper.get({
    block_height: abandonedBlock.height,
  });

  // this table is ambigious about block hashes, so we remove it only if the hash matches
  if (blockHashMapperResult && blockHashMapperResult.block_hash === blockHash) {
    try {
      await blockHeightToHashMapper.get({
        block_height: abandonedBlock.height,
      });
    } catch {}
  }

  if (abandonedBlock && !R.isEmpty(abandonedBlock.txs)) {
    let index = -1;
    for (const abandonedTx of abandonedBlock.txs) {
      index += 1;
      const txIndex = toLong(abandonedBlock.height).mul(1000).add(index);
      const nthMillion = txIndex.div(1e6).toInt();

      try {
        await txsSortedAscMapper.remove({
          nth_million: nthMillion,
          tx_id: abandonedTx,
          tx_index: txIndex,
          data_item_index: -1,
        });
      } catch {}

      try {
        await txsSortedDescMapper.remove({
          nth_million: nthMillion,
          tx_id: abandonedTx,
          tx_index: txIndex,
          data_item_index: -1,
        });
      } catch {}

      try {
        await txOffsetMapper.remove({ tx_id: abandonedTx });
      } catch {}

      const tx = await transactionMapper.get({ tx_id: abandonedTx });

      if (tx && tx.tags && Array.isArray(tx.tags) && !R.isEmpty(tx.tags)) {
        const abandonedTxTags = tx.tags.map((t: CassandraTypes.Tuple) =>
          t.values()
        );
        const isManifest = hasManifestContentType(abandonedTxTags);
        let tagIndex = -1;

        for (const abandonedTag of abandonedTxTags) {
          tagIndex += 1;

          const [tagName, tagValue] = abandonedTag;

          const owner = ownerToAddress(tx.owner);
          const tagDropParameters: DropTagQueryParameters = {
            tagName,
            tagValue,
            owner,
            bundledIn: tx.bundled_in,
            dataItemIndex: -1,
            dataRoot: tx.data_root,
            tagIndex: `${tagIndex}`,
            target: tx.target,
            txId: tx.tx_id,
            txIndex,
          };
          await cassandraClient.exec(dropTagQuery(tagDropParameters));
        }
        if (isManifest) {
          const maybeManifest = await manifestMapper.get({
            tx_id: abandonedTx.tx_id,
          });

          if (maybeManifest) {
            const manifestPaths = JSON.parse(maybeManifest.manifest_paths);
            const manifestFiles = Object.keys(manifestPaths);

            if (manifestFiles.includes(maybeManifest.manifest_index)) {
              try {
                await permawebPathMapper.remove({
                  domain_id: abandonedTx.tx_id,
                  uri_path: "",
                });
              } catch {}
            }
            for (const manifestFile of manifestFiles) {
              try {
                await permawebPathMapper.remove({
                  domain_id: abandonedTx.tx_id,
                  uri_path: escape(manifestFile),
                });
              } catch {}
            }
          }
          try {
            await manifestQueueMapper.remove(
              { tx_id: abandonedTx.tx_id },
              { ifExists: true }
            );
          } catch {}
        }
      }

      if (tx) {
        try {
          await transactionMapper.remove({ tx_id: abandonedTx });
        } catch {}

        const dropTxsParameters: DropTxsQueryParameters = {
          txIndex,
          txId: abandonedTx,
          bundledIn: tx.bundled_in,
          dataItemIndex: tx.data_item_index,
          dataRoot: tx.data_root,
          owner: tx.owner,
          target: tx.target,
        };
        try {
          await cassandraClient.exec(dropTxsQuery(dropTxsParameters));
        } catch {}
      }
    }
  }

  const nthMillBlock = abandonedBlock.height.div(1e6).toInt();

  try {
    const maybeMatchingBlock = await blockSortedAscMapper({
      nth_million: nthMillBlock,
      block_height: abandonedBlock.block_height,
    });
    if (maybeMatchingBlock && maybeMatchingBlock.block_hash === blockHash) {
      await blockSortedAscMapper.remove({
        nth_million: nthMillBlock,
        block_height: abandonedBlock.block_height,
      });
    }
  } catch {}
  try {
    const maybeMatchingBlock = await blockSortedDescMapper({
      nth_million: nthMillBlock,
      block_height: abandonedBlock.block_height,
    });
    if (maybeMatchingBlock && maybeMatchingBlock.block_hash === blockHash) {
      await blockSortedDescMapper.remove({
        nth_million: nthMillBlock,
        block_height: abandonedBlock.block_height,
      });
    }
  } catch {}

  try {
    await blockMapper.remove({
      indep_hash: blockHash,
    });
  } catch {
    return TruncateBlockReturnCode.REQUEUE;
  }

  return TruncateBlockReturnCode.OK;
}

// await blockMapper.remove({ indep_hash: block.block_hash });
// await blockHeightToHashMapper.remove({
//   block_height: block.block_height,
// });
