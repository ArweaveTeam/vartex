// import { types as CassandraTypes } from "cassandra-driver";
import { MessagesFromParent, MessagesFromWorker } from "./message-types";
import { getBlock as queryGetBlock } from "../query/block";
import {
  blockHeightToHashMapper,
  blockMapper,
  blockSortedAscMapper,
  blockSortedDescMapper,
  txQueueMapper,
} from "../database/mapper";
import { toLong } from "../database/utils";
import { getMessenger } from "../gatsby-worker/child";
import { mkWorkerLog } from "../utility/log";

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
        block_hash: newBlock.indep_hash,
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
    log(
      `Error inserting to table blockHeightToHash with height: ${height} and hash: ${newBlock.indep_hash}` +
        error
        ? "\n" + JSON.stringify(error)
        : ""
    );
    return BlockImportReturnCode.REQUEUE;
  }

  try {
    await blockMapper.insert(newBlock);
  } catch (error) {
    log(
      `Error inserting block to database` + error
        ? "\n" + JSON.stringify(error)
        : ""
    );
    return BlockImportReturnCode.REQUEUE;
  }

  let nthMillBlock;
  try {
    nthMillBlock = toLong(newBlock.height).div(1e6).toInt();
  } catch {}

  if (typeof nthMillBlock !== "number") {
    log(
      `Error while parsing the block height of ${newBlock.indep_hash} from response`
    );
    return BlockImportReturnCode.REQUEUE;
  }

  try {
    await blockSortedAscMapper.insert({
      block_hash: newBlock.indep_hash,
      nth_million: nthMillBlock,
    });
  } catch {
    return BlockImportReturnCode.REQUEUE;
  }

  try {
    await blockSortedDescMapper.insert({
      block_hash: newBlock.indep_hash,
      nth_million: nthMillBlock,
    });
  } catch {
    return BlockImportReturnCode.REQUEUE;
  }

  return BlockImportReturnCode.OK;
}
