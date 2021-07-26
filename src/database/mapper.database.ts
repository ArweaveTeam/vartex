import * as R from 'rambda';
import { mapping } from 'cassandra-driver';
import { cassandraClient } from './cassandra.database';

const { Mapper } = mapping;

// prune the null values away
const withDefault = ({ name, fallback }: { name: string; fallback: any }) => ({
  [name]: {
    name,
    toModel: (v: any) => v || fallback,
    fromModel: (v: any) => v || fallback,
  },
});

const mapper = new Mapper(cassandraClient, {
  models: {
    BlockHeightByBlockHash: {
      keyspace: 'gateway',
      tables: ['block_height_by_hash'],
    },
    Block: {
      keyspace: 'gateway',
      tables: ['block'],
      columns: R.mergeAll([
        withDefault({ name: 'cumulative_diff', fallback: '' }),
        withDefault({ name: 'hash_list_merkle', fallback: '' }),
        withDefault({ name: 'previous_block', fallback: '' }), // only block 0
        withDefault({ name: 'tags', fallback: [] }),
        withDefault({ name: 'tx_root', fallback: '' }),
        withDefault({ name: 'tx_tree', fallback: '' }),
        withDefault({ name: 'txs', fallback: [] }),
      ]),
    },
    BlockByTxId: {
      keyspace: 'gateway',
      tables: ['block_by_tx_id'],
    },
    Poa: {
      keyspace: 'gateway',
      tables: ['poa'],
    },
    PollStatus: {
      keyspace: 'gateway',
      tables: ['poll_status'],
    },
    Transaction: {
      keyspace: 'gateway',
      tables: ['transaction'],
    },
    TxTag: {
      keyspace: 'gateway',
      tables: ['tx_tag'],
    },
  },
});

export const blockHeightToHashMapper = mapper.forModel(
  'BlockHeightByBlockHash'
);

export const blockMapper = mapper.forModel('Block');

export const poaMapper = mapper.forModel('Poa');

export const pollStatusMapper = mapper.forModel('PollStatus');

export const transactionMapper = mapper.forModel('Transaction');

export const txIdToBlockMapper = mapper.forModel('BlockByTxId');

export const txTagMapper = mapper.forModel('TxTag');
