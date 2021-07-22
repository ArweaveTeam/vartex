import * as cassandra from 'cassandra-driver';
import * as R from 'rambda';
import { types as CassandraTypes } from 'cassandra-driver';
import { config } from 'dotenv';

config();

const currentSessionId = CassandraTypes.TimeUuid.now();

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

export const newSession = async () => {
  let lastSessionHeight = 0;
  let lastSessionHash = '';

  try {
    const maybeLastSession = cassandraClient.execute(
      `SELECT height FROM gateway.sync_status limit 1`
    );
  } catch (error) {
    console.error('ERROR LOOK:', error);
  }
  process.exit(1);
  await cassandraClient.execute(
    'INSERT INTO gateway.sync_status IF NOT EXISTS ()'
  );
};

export const cassandraClient = new cassandra.Client({
  contactPoints,
  localDataCenter: 'datacenter1',
  encoding: {
    map: Map,
    set: Set,
  },
});

const poaKeys = [
  'option',
  'tx_path',
  'data_path',
  'chunk',
  'block_hash',
  'block_height',
];

const syncStatusKeys = ['last_block_hash', 'last_block_height', 'session_uuid'];

// const blockStatusKeys = ['block_height', 'synced'];

const txTagKeys = ['tx_id', 'tag_index', 'name', 'value'];

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

const transformPoaKeys = (key: string, obj: any) => {
  const poa = obj['poa'] && typeof obj['poa'] === 'object' ? obj['poa'] : {};
  const poaObj = {};
  poaObj[poa['option'] || ''];
  poaObj[poa['tx_path'] || ''];
  poaObj[poa['data_path'] || ''];
  poaObj[poa['chunk'] || ''];
  poaObj[poa['block_hash'] || ''];
  poaObj[poa['block_height'] || ''];
  return poaObj;
};

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

const poaInsertQuery = `INSERT INTO gateway.poa (${poaKeys.join(
  ', '
)}) VALUES (${poaKeys.map(() => '?').join(', ')})`;

const syncStatusInsertQuery = `INSERT INTO gateway.poa (${syncStatusKeys.join(
  ', '
)}) VALUES (${syncStatusKeys.map(() => '?').join(', ')})`;

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
      {
        query: poaInsertQuery,
        params: transformPoaKeys(input.poa),
      },
      { query: blockStatusUpdateQuery, params: [toLong(input.height)] },
      {
        query: blockInsertQuery,
        params: R.dissoc(blockKeys, 'poa').reduce(
          (paramz: Array<any>, key: string) => {
            paramz.push(transformBlockKey(key, input));
            // console.log(input, transformBlockKey(key, input));
            return paramz;
          },
          []
        ),
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
