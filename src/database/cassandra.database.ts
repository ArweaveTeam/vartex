import * as cassandra from 'cassandra-driver';
import * as R from 'rambda';
import { types as CassandraTypes } from 'cassandra-driver';
import { config } from 'dotenv';

config();

const currentSessionId = CassandraTypes.TimeUuid.now();

const isNumeric = (s: any) => !(isNaN as any)(s);

export const toLong = (anyValue: any): CassandraTypes.Long =>
  !anyValue && typeof anyValue !== 'string'
    ? (cassandra as any).types.Long.fromNumber(0)
    : typeof anyValue === 'string'
    ? (cassandra as any).types.Long.fromString(
        R.isEmpty(anyValue) ? '0' : anyValue
      )
    : (cassandra as any).types.Long.fromNumber(anyValue);

const contactPoints = process.env.CASSANDRA_CONTACT_POINTS
  ? JSON.parse(process.env.CASSANDRA_CONTACT_POINTS)
  : ['localhost:9042'];

export const newSession = async (): Promise<[string, number]> => {
  let lastSessionHeight = 0;
  let lastSessionHash = '';

  try {
    const maybeLastSession = await cassandraClient.execute(
      `SELECT height FROM gateway.sync_status limit 1`
    );
    lastSessionHeight = maybeLastSession?.rows[0]['last_block_height'];
    lastSessionHash = maybeLastSession?.rows[0]['last_block_hash'];
  } catch (error) {
    if (!error.toString().includes('Undefined column name height')) {
      process.exit(1);
    }
  }

  await cassandraClient.execute(
    'INSERT INTO gateway.sync_status (last_block, session_uuid)' +
      'VALUES (?, ?) IF NOT EXISTS',
    [
      new CassandraTypes.Tuple(lastSessionHash, toLong(lastSessionHeight)),
      currentSessionId,
    ],
    { prepare: true }
  );
  return [lastSessionHash, lastSessionHeight];
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

const syncStatusKeys = ['last_block', 'session_uuid'];

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

interface Poa {
  option: string;
  tx_path: string;
  data_path: string;
  chunk: string;
  block_hash: string;
  block_height: CassandraTypes.Long;
}

const transformPoaKeys = (obj: any): Poa => {
  const poa = obj['poa'] ? obj['poa'] : {};
  const poaObj = {} as Poa;
  poaObj['option'] = poa['option'] || '';
  poaObj['tx_path'] = poa['tx_path'] || '';
  poaObj['data_path'] = poa['data_path'] || '';
  poaObj['chunk'] = poa['chunk'] || '';
  poaObj['block_hash'] = obj['hash'] || '';
  poaObj['block_height'] = toLong(obj['height']);
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
      if (tags.length === 0) {
        return [];
      } else {
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
    }

    case 'block_size':
    case 'diff':
    case 'height':
    case 'last_retarget':
    case 'reward_pool':
    case 'timestamp':
    case 'weave_size': {
      return toLong(obj[key]);
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
    case 'last_retarget': {
      if (obj[key] || isNumeric(obj[key])) {
        return typeof obj[key] === 'string' ? obj[key] : obj[key].toString();
      } else {
        return '';
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

const blockInsertQuery = (nonNilBlockKeys: string[]) =>
  `INSERT INTO gateway.block (${nonNilBlockKeys.join(
    ', '
  )}) VALUES (${nonNilBlockKeys.map(() => '?').join(', ')})`;

const blockStatusUpdateQuery = `
  UPDATE gateway.block_status
  SET synced = true
  WHERE block_hash = ?`;

// Note the last synced block isn't
// nececcarily the latest one, than
// always needs verification on init
const syncStatusUpdateQuery = `
  UPDATE gateway.sync_status
  SET last_block = ?
  WHERE session_uuid = ?`;

// these updates and inserts need to be atomic
export const makeBlockImportQuery = (input: any) => () => {
  const nonNilBlockKeys: string[] = [];
  const blockInsertParams = blockKeys.reduce(
    (paramz: Array<any>, key: string) => {
      const nextVal = transformBlockKey(key, input);
      if (nextVal && !R.isEmpty(nextVal)) {
        paramz.push(nextVal);
        nonNilBlockKeys.push(key);
      }

      return paramz;
    },
    []
  );
  const batch = [
    {
      query: poaInsertQuery,
      params: transformPoaKeys(input),
    },
    { query: blockStatusUpdateQuery, params: [input.hash] },
    {
      query: blockInsertQuery(nonNilBlockKeys),
      params: blockInsertParams,
    },
    {
      query: syncStatusUpdateQuery,
      params: [
        new CassandraTypes.Tuple(input.hash, toLong(input.height)),
        currentSessionId,
      ],
    },
  ];
  return cassandraClient.batch(batch, { prepare: true });
};

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

// doesn't need to accurate this one
export const getPlaceholderCount = async (): Promise<CassandraTypes.Long> => {
  let cnt = (cassandra as any).types.Long.fromNumber(0);
  try {
    const response = await cassandraClient.execute(
      `SELECT * FROM gateway.block_status limit 100;`
    );
    for (const resp of response.rows) {
      if (resp.block_height.gt(cnt)) {
        cnt = resp.block_height;
      }
    }
  } catch (error) {}

  return cnt;
};
