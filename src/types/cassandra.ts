import { types as CassandraTypes } from "cassandra-driver";

// export type ImportQueue = { [v: string]: any };

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

export interface UpstreamTag {
  name: string;
  value: string;
}

export interface Transaction {
  tx_index: CassandraTypes.Long;
  block_height: CassandraTypes.Long;
  block_hash: string;
  bundled_in: string;
  data_root: string;
  data_size: CassandraTypes.Long;
  data_tree: string[];
  format: number;
  tx_id: string;
  last_tx: string;
  owner: string;
  quantity: CassandraTypes.Long;
  reward: CassandraTypes.Long;
  signature: string;
  tags: CassandraTypes.Tuple[];
  tag_count: number;
  target: number;
}
