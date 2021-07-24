// import * as R from 'rambda';
import { mapping } from 'cassandra-driver';
import { cassandraClient } from './cassandra.database';

const { Mapper } = mapping;

const mapper = new Mapper(cassandraClient, {
  models: {
    BlockHash: {
      keyspace: 'gateway',
      tables: ['block_hash'],
    },
    Block: {
      keyspace: 'gateway',
      tables: ['block'],
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
