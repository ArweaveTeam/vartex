import * as cassandra from 'cassandra-driver';
import { types as CassandraTypes } from 'cassandra-driver';
import { config } from 'dotenv';

config();

const isNumeric = (s: any) => !(isNaN as any)(s);

const toLong = (anyValue: any): CassandraTypes.Long =>
  !anyValue
    ? (cassandra as any).types.Long.fromNumber(0)
    : anyValue === 'string'
    ? (cassandra as any).types.Long.fromString(anyValue)
    : (cassandra as any).types.Long.fromNumber(anyValue);

const contactPoints = process.env.CASSANDRA_CONTACT_POINTS
  ? JSON.parse(process.env.CASSANDRA_CONTACT_POINTS)
  : ['localhost:9042'];

export const cassandraClient = new cassandra.Client({
  contactPoints,
  localDataCenter: 'datacenter1',
  encoding: {
    map: Map,
    set: Set,
  },
});

const poaKeys = ['option', 'tx_path', 'data_path', 'chunk', 'block_height'];

const importStatusKeys = ['last_block_height', 'session_uuid'];

// const blockStatusKeys = ['block_height', 'synced'];

const txTagKeys = ['tx_id', 'index', 'name', 'value'];

const transactionKeys = [
  'data',
  'data_root',
  'data_tree',
  'format',
  'id',
  'last_tx',
  'owner',
  'quantity',
  'reward',
  'signature',
  'tag_count',
];

const blockKeys = [
  'block_size',
  'cumulative_diff',
  'diff',
  'hash',
  'hash_list_merkle',
  'height',
  'indep_hash',
  'last_retarget',
  'nonce',
  // 'poa',
  'previous_block',
  'reward_addr',
  'reward_pool',
  'tags',
  'timestamp',
  'tx_root',
  'tx_tree',
  'txs',
  'wallet_list',
  'weave_size',
];

// note for optimization reasons
// we may store the data differently than we serve it (eg. bigint->string)
const transformBlockKey = (key: string, obj: any) => {
  switch (key) {
    case 'txs':
    case 'tx_tree': {
      const txs = obj[key] && Array.isArray(obj[key]) ? obj[key] : [];
      return txs;
    }
    case 'tags': {
      const tagSet = new Set();
      const tags = obj[key] && Array.isArray(obj[key]) ? obj[key] : [];
      tags.forEach((tag = {} as any) => {
        tagSet.add(
          new (cassandra as any).types.Tuple(
            tag['name'] || '',
            tag['value'] || ''
          )
        );
      });
      return tagSet;
    }
    case 'poa': {
      const poa = obj[key] && typeof obj[key] === 'object' ? obj[key] : {};
      const tuple = []; // new Map();
      tuple.push(poa['option'] || '');
      tuple.push(poa['tx_path'] || '');
      tuple.push(poa['data_path'] || '');
      tuple.push(poa['chunk'] || '');

      return new (cassandra as any).types.Tuple(
        tuple[0],
        tuple[1],
        tuple[2],
        tuple[3]
      );
    }
    case 'block_size':
    case 'diff':
    case 'height':
    case 'reward_pool':
    case 'weave_size': {
      if (isNumeric(obj[key])) {
        return typeof cassandra === 'string'
          ? (cassandra as any).types.Long.fromString(obj[key])
          : (cassandra as any).types.Long.fromNumber(obj[key]);
      } else {
        return (cassandra as any).types.Long.fromNumber(0);
      }
    }
    case 'cumulative_diff':
    case 'hash':
    case 'hash_list_merkle':
    case 'indep_hash':
    case 'nonce':
    case 'previous_block':
    case 'reward_addr':
    case 'tx_root':
    case 'wallet_list':
    case 'last_retarget':
    case 'timestamp': {
      if (obj[key] || isNumeric(obj[key])) {
        return typeof obj[key] === 'string' ? obj[key] : obj[key].toString();
      } else {
        return null;
      }
    }

    default: {
      console.error('Unknown key', key);
    }
  }
};

const blockInsertQuery = `INSERT INTO gateway.block (${blockKeys.join(
  ', '
)}) VALUES (${blockKeys.map(() => '?').join(', ')})`;

const blockStatusUpdateQuery = `
  UPDATE gateway.block_status
  SET synced = true
  WHERE block_height = ?`;

// these updates and inserts need to be atomic
export const makeBlockImportQuery = (input: any) =>
  cassandraClient.batch(
    [
      { query: blockStatusUpdateQuery, params: [toLong(input.height)] },
      {
        query: blockInsertQuery,
        params: blockKeys.reduce((paramz: Array<any>, key: string) => {
          paramz.push(transformBlockKey(key, input));
          // console.log(input, transformBlockKey(key, input));
          return paramz;
        }, []),
      },
    ],
    { prepare: true }
  );

// unsafe and slow!
export const getMaxHeightBlock = async (): Promise<CassandraTypes.Long> => {
  const response = await cassandraClient.execute(
    'SELECT MAX(height) FROM gateway.block;'
  );
  return (
    response.rows[0]['system.max(height)'] ||
    (cassandra as any).types.Long.fromNumber(-1)
  );
};
