import { types as CassandraTypes } from "cassandra-driver";

export type ImportQueue = { [v: string]: any };

export interface BlockQueueState {
  isProcessing: boolean;
  isStarted: boolean;
  nextHeight: CassandraTypes.Long;
}

export interface TxQueueState {
  isProcessing: boolean;
  isStarted: boolean;
  nextTxIndex: CassandraTypes.Long;
}

export interface Poa {
  option: string;
  tx_path: string;
  data_path: string;
  chunk: string;
  block_hash: string;
  block_height: CassandraTypes.Long;
}

export interface TxOffset {
  tx_id: string;
  size: CassandraTypes.Long;
  offset: CassandraTypes.Long;
}

export interface UnsyncedBlock {
  height: number;
  hash: string;
}

export interface DeleteRowData extends UnsyncedBlock {
  timestamp: CassandraTypes.Long;
}
