import * as R from "rambda";
import { types as CassandraTypes } from "cassandra-driver";
import { getTransaction, getTxOffset } from "../query/transaction";
import {
  cassandraClient,
  transactionMapper,
  blockMapper,
  blockHeightToHashMapper,
  txQueueMapper,
} from "../database/mapper";
import {
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from "../database/cassandra";

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
  const maybeImportedTx = transactionMapper({ txId });

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

  const currentTx = await getTransaction({ txId });

  if (!currentTx) {
    log(`Failed to fetch ${txId} from nodes`);
    return TxReturnCode.REQUEUE;
  }

  let maybeTxOffset;

  const dataSize = toLong(currentTransaction.data_size);

  if (dataSize && dataSize.gt(0)) {
    maybeTxOffset = await getTxOffset({ txId });
    if (!maybeTxOffset) {
      log(`Failed to fetch data offset for ${txId} from nodes`);
      return TxReturnCode.REQUEUE;
    }
  }

  const txIndex = toLong(blockHeight)
    .multiply(1000)
    .add(block.txs.indexOf(txId));

  const tx = R.mergeAll([
    currentTransaction,
    maybeTxOffset ? { tx_offset: maybeTxOffset } : {},
  ]);

  const callback = await makeTxImportQuery(
    toLong(blockHeight),
    txIndex,
    tx,
    block
  );

  try {
    await callback();
  } catch (error) {
    log(JSON.stringify(error));
    return TxReturnCode.REQUEUE;
  }

  return TxReturnCode.DEQUEUE;
};
