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
    BlockHash: {
      keyspace: 'gateway',
      tables: ['block_hash'],
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
    Poa: {
      keyspace: 'gateway',
      tables: ['poa'],
    },
  },
});

export const blockHeightToHashMapper = mapper.forModel('BlockHash');

export const blockMapper = mapper.forModel('Block');

export const poaMapper = mapper.forModel('Poa');
