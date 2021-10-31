import * as R from "rambda";
import { types as CassandraTypes } from "cassandra-driver";
import { getBlock as queryGetBlock } from "../query/block";
import {
  blockHeightToHashMapper,
  blockMapper,
  cassandraClient,
  txQueueMapper,
} from "../database/mapper";
import {
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from "../database/cassandra";

let messenger = getMessenger<MessagesFromParent, MessagesFromWorker>();

if (messenger) {
  messenger.sendMessage({
    type: "worker:ready",
  });
} else {
  (messenger as any) = { sendMessage: console.log };
}

const log = mkWorkerLog(messenger);

enum BlockImportReturnCode {
  OK,
  REQUEUE,
  DEQUEUE,
}

export async function importBlock(
  height: number
): Promise<BlockImportReturnCode> {
  const newBlock = await queryGetBlock({ height });

  if (!newBlock || typeof newBlock !== "object") {
    log(
      `Couldn't find block at height ${height} from any node, will retry later...`
    );
    return BlockImportReturnCode.REQUEUE;
  }

  if (Array.isArray(newBlock.txs)) {
    for (const txId of newBlock.txs) {
      await txQueueMapper.insert({
        tx_id: txId,
        block_height: height,
        import_attempt_cnt: 0,
      });
    }
  }

  try {
    await blockHeightToHashMapper.insert({
      block_heigh: height,
      block_hash: newBlock.indep_hash,
    });
  } catch (error) {
    log(`Error inserting to table blockHeightToHash\n` + JSON.stringify(error));
    return BlockImportReturnCode.REQUEUE;
  }

  try {
    await blockMapper.insert(newBlock);
  } catch (error) {
    log(`Error inserting block to database\n` + JSON.stringify(error));
    return BlockImportReturnCode.REQUEUE;
  }

  return BlockImportReturnCode.OK;
}
