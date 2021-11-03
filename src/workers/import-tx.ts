import * as R from "rambda";
import { types as CassandraTypes } from "cassandra-driver";
import { Transaction, TxOffset, UpstreamTag } from "../types/cassandra";
import { getTransaction, getTxOffset } from "../query/transaction";
import { ownerToAddress } from "../utility/encoding";
import {
  blockMapper,
  blockHeightToHashMapper,
  tagsMapper,
  tagModels,
  transactionMapper,
  txOffsetMapper,
  txQueueMapper,
} from "../database/mapper";
import { toLong } from "../database/utils";

enum TxReturnCode {
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
  (messenger as any) = { sendMessage: console.log };
}

const log = mkWorkerLog(messenger);

const commonFields = ["tx_index", "data_item_index", "tx_id"];

export const insertGqlTag = async (
  tags: CassandraTypes.TimeUuid[]
): Promise<void> => {
  if (!R.isEmpty(tags)) {
    for (const tagModelName of Object.keys(tagModels)) {
      const tagMapper = tagsMapper.forModel(tagModelName);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const allFields: any = [...R, ...commonFields].concat(
        tagModels[tagModelName]
      );
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const environment: any = R.pickAll(allFields, tx);

      // until ans104 comes
      if (!environment["data_item_index"]) {
        environment["data_item_index"] = toLong(0);
      }
      if (
        typeof environment.owner === "string" &&
        environment.owner.length > 43
      ) {
        environment.owner = ownerToAddress(environment.owner);
      }

      let index = 0;
      for (const tuple of tx.tags) {
        const [tag_name, tag_value] = tuple.values();

        const insertObject = R.merge(environment, {
          tag_pair: `${tag_name}|${tag_value}`,
          tag_index: index,
        });

        await tagMapper.insert(insertObject);
        index += 1;
      }
    }
  }
};

export const importTx = (txId: string, blockHeight: string) => {
  const blockHash = await blockHeightToHashMapper({
    block_height: blockHeight,
  });

  if (!blockHash) {
    log(
      `blockHeight ${blockHeight} has not been imported, therefore not importing ${txId}`
    );
    return TxReturnCode.REQUEUE;
  }

  const block = await blockMapper({ indep_hash: blockHash });

  if (!block) {
    log(
      `blockHash ${blockHash} has not been imported (or was removed?), therefore not importing ${txId}`
    );
    return TxReturnCode.REQUEUE;
  }

  if (!block.txs.includes(txId)) {
    log(
      `Abandoned tx detected? It was not found in txs of block ${blockHash}, therefore dequeue-ing ${txId}`
    );

    return TxReturnCode.DEQUEUE;
  }

  // check if it's already imported, or is attached to abandoned fork
  const maybeImportedTx = await transactionMapper.get({ txId });

  if (maybeImportedTx) {
    if (maybeImportedTx.block_hash === blockHash) {
      log(
        `Already imported transaction ${txId}! If you want to force a re-import, please remove the old one first.`
      );
      return TxReturnCode.DEQUEUE;
    } else {
      log(
        `Misplaced transaction ${txId}! Perhaps block with hash ${maybeImportedTx.block_hash} was abandoned?\n` +
          `Moving on to import the tx to block ${blockHash} at height ${blockHeight}`
      );
      return TxReturnCode.DEQUEUE;
    }
  }

  const tx = await getTransaction({ txId });

  if (!tx) {
    log(`Failed to fetch ${txId} from nodes`);
    return TxReturnCode.REQUEUE;
  }

  const dataSize = toLong(tx.data_size);

  if (dataSize && dataSize.gt(0)) {
    const maybeTxOffset = await getTxOffset({ txId });
    if (!maybeTxOffset) {
      log(`Failed to fetch data offset for ${txId} from nodes`);
      return TxReturnCode.REQUEUE;
    } else {
      try {
        await txOffsetMapper.insert({
          tx_id: tx.id,
          size: maybeTxOffset.size,
          offset: maybeTxOffset.offset,
        });
      } catch (error) {
        log(JSON.stringify(error));
        return TxReturnCode.REQUEUE;
      }
    }
  }

  const txIndex = toLong(blockHeight)
    .multiply(1000)
    .add(block.txs.indexOf(txId));

  let tags = [];

  if (!R.isEmpty(tx.tags) && Array.isArray(tx.tags)) {
    tags = tx.tags.map(({ name, value }: UpstreamTag) =>
      CassandraTypes.Tuple.fromArray([name, value])
    );
  }

  try {
    await insertGqlTag(tags);
  } catch (error) {
    log(JSON.stringify(error));
    return TxReturnCode.REQUEUE;
  }

  try {
    await transactionMapper.insert({
      tx_index: txIndex,
      data_item_index: toLong(-1),
      block_height: block.height,
      block_hash: block.indep_hash,
      bundled_in: null,
      data_root: tx.data_root,
      data_size: tx.data_size,
      data_tree: tx.data_tree || [],
      format: tx.format,
      tx_id: tx.id,
      last_tx: tx.last_tx,
      owner: tx.owner,
      quantity: tx.quantity,
      reward: tx.reward,
      signature: tx.signature,
      tags,
      tag_count: tags.length,
      target: tx.target,
    });
  } catch (error) {
    log(JSON.stringify(error));
    return TxReturnCode.REQUEUE;
  }

  return TxReturnCode.DEQUEUE;
};
