import { types as CassandraTypes } from 'cassandra-driver';

export type ImportQueue = { [v: string]: any };

export interface QueueState {
  isProcessing: boolean;
  isStarted: boolean;
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
