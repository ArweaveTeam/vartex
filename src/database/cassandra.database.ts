import crypto from 'crypto';
import * as cassandra from 'cassandra-driver';
import * as R from 'rambda';
import { types as CassandraTypes } from 'cassandra-driver';
import { ImportQueue } from '../types/cassandra.types';
import { config } from 'dotenv';

config();

const currentSessionId = CassandraTypes.TimeUuid.now();
const currentSessionRandomId = crypto.randomBytes(16);

const isNumeric = (s: any) => !(isNaN as any)(s);

export const toLong = (anyValue: any): CassandraTypes.Long =>
  (cassandra as any).types.Long.isLong(anyValue)
    ? anyValue
    : !anyValue && typeof anyValue !== 'string'
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
  let lastSessionHeight = -1;
  let lastSessionHash = '';

  try {
    const maybeLastSession = await cassandraClient.execute(
      `SELECT last_block_height,last_block_hash FROM gateway.sync_status limit 1`
    );
    const tuple = maybeLastSession.rows[0];
    if (tuple) {
      lastSessionHash = tuple.last_block_hash;
      lastSessionHeight = tuple.last_block_height.toInt();
    }
  } catch (error) {
    if (!error.toString().includes('Undefined column name height')) {
      process.exit(1);
    }
  }

  if (lastSessionHeight > 0) {
    await cassandraClient.execute(
      'INSERT INTO gateway.sync_status (last_block_height,last_block_hash,session_uuid,random_uuid)' +
        'VALUES (?, ?, ?, ?) IF NOT EXISTS',
      [
        toLong(lastSessionHeight),
        lastSessionHash,
        currentSessionId,
        currentSessionRandomId,
      ],
      { prepare: true }
    );
  }
  return [lastSessionHash, lastSessionHeight];
};

export const cassandraClient = new cassandra.Client({
  contactPoints,
  localDataCenter: 'datacenter1',
  encoding: {
    map: Map,
    set: Set,
  },
  protocolOptions: {
    maxSchemaAgreementWaitSeconds: process.env['DB_TIMEOUT']
      ? parseInt(process.env['DB_TIMEOUT'])
      : 30,
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

const txOffsetKeys = ['tx_id', 'size', 'offset'];

const transactionKeys = [
  'block_height',
  'block_hash',
  'data_root',
  'data_size',
  'data_tree',
  'format',
  'id',
  'last_tx',
  'owner',
  'quantity',
  'reward',
  'signature',
  'tag_count',
  'tx_uuid',
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
  poaObj['block_hash'] = obj['indep_hash'] || '';
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

const transformTxKey = (key: string, txData: any, blockData: any) => {
  switch (key) {
    case 'tx_uuid': {
      return CassandraTypes.TimeUuid.now();
    }
    case 'block_height': {
      return toLong(blockData['height']);
    }

    case 'block_hash': {
      return blockData['indep_hash'];
    }

    case 'data_tree': {
      const txs = txData[key] && Array.isArray(txData[key]) ? txData[key] : [];
      return txs;
    }
    case 'tag_count': {
      return !txData.tags || R.isEmpty(txData.tags) ? 0 : R.length(txData.tags);
    }
    case 'tags': {
      const tagSet = new Set();
      const tags = txData[key] && Array.isArray(txData[key]) ? txData[key] : [];
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

    case 'data_root':
    case 'id':
    case 'last_tx':
    case 'owner':
    case 'signature': {
      if (txData[key]) {
        return typeof txData[key] === 'string'
          ? txData[key]
          : txData[key].toString();
      } else {
        return '';
      }
    }

    case 'data_size':
    case 'quantity':
    case 'reward': {
      return toLong(txData[key]);
    }

    case 'format': {
      return txData[key];
    }

    default: {
      console.error('Unknown key', key);
    }
  }
};

interface TxOffset {
  tx_id: string;
  size: CassandraTypes.Long;
  offset: CassandraTypes.Long;
}

const transformTxOffsetKeys = (txObj: any): TxOffset => {
  const txOffset = txObj['tx_offset'] ? txObj['tx_offset'] : {};
  const txOffsetObj = {} as TxOffset;
  txOffsetObj['tx_id'] = txObj['id'] || '';
  txOffsetObj['size'] = toLong(txOffset['size'] || 0);
  txOffsetObj['offset'] = toLong(txOffset['offset'] || -1);
  return txOffsetObj;
};

interface Tag {
  tag_index: number;
  tx_id: string;
  name: string;
  value: string;
}

type UpstreamTag = { name: string; value: string };

const transformTag = (tag: UpstreamTag, txObj: any, index: number): Tag => {
  const tagObj = {} as Tag;
  tagObj['tag_index'] = index;
  tagObj['tx_id'] = txObj['id'];
  tagObj['name'] = tag.name || '';
  tagObj['value'] = tag.value || '';
  return tagObj;
};

const poaInsertQuery = `INSERT INTO gateway.poa (${poaKeys.join(
  ', '
)}) VALUES (${poaKeys.map(() => '?').join(', ')})`;

const blockInsertQuery = (nonNilBlockKeys: string[]) =>
  `INSERT INTO gateway.block (${nonNilBlockKeys.join(
    ', '
  )}) VALUES (${nonNilBlockKeys.map(() => '?').join(', ')})`;

const transactionInsertQuery = (nonNilTxKeys: string[]) =>
  `INSERT INTO gateway.transaction (${nonNilTxKeys.join(
    ', '
  )}) VALUES (${nonNilTxKeys.map(() => '?').join(', ')})`;

const txOffsetInsertQuery = `INSERT INTO gateway.tx_offset (${txOffsetKeys.join(
  ', '
)}) VALUES (${txOffsetKeys.map(() => '?').join(', ')})`;

const txTagsInsertQuery = `INSERT INTO gateway.tx_tag (${txTagKeys.join(
  ', '
)}) VALUES (${txTagKeys.map(() => '?').join(', ')})`;

const blockStatusUpdateQuery = `
  UPDATE gateway.block_status
  SET synced = true
  WHERE block_height = ? and block_hash = ?`;

const blockHeightByHashInsertQuery = `INSERT INTO gateway.block_height_by_block_hash (block_height, block_hash) VALUES (?, ?) IF NOT EXISTS`;

const blockByTxIdInsertQuery = `INSERT INTO gateway.block_by_tx_id (tx_id, tx_uuid, block_height, block_hash) VALUES (?, ?, ?, ?) IF NOT EXISTS`;

// Note the last synced block isn't
// nececcarily the latest one, than
// always needs verification on init
const syncStatusUpdateQuery = `
  UPDATE gateway.sync_status
  SET last_block_height = ?, last_block_hash = ?
  WHERE session_uuid = ? and random_uuid = ?`;

export const makeTxImportQuery = (
  tx: { [k: string]: any },
  blockData: { [k: string]: any }
) => () => {
  let txUuid: any;
  const nonNilTxKeys: string[] = [];
  const txInsertParams: { [k: string]: any } = transactionKeys.reduce(
    (paramz: Array<any>, key: string) => {
      const nextVal = transformTxKey(key, tx, blockData);
      if (key === 'tx_uuid') {
        txUuid = nextVal;
      }
      if (nextVal && !R.isEmpty(nextVal)) {
        paramz.push(nextVal);
        nonNilTxKeys.push(key);
      }

      return paramz;
    },
    []
  );

  return [
    cassandraClient.execute(txOffsetInsertQuery, transformTxOffsetKeys(tx), {
      prepare: true,
    }),
    cassandraClient.execute(
      blockByTxIdInsertQuery,
      [tx.id, txUuid, blockData.height, blockData.indep_hash],
      {
        prepare: true,
      }
    ),
    cassandraClient.execute(
      transactionInsertQuery(nonNilTxKeys),
      txInsertParams,
      { prepare: true }
    ),
  ].concat(
    (tx.tags || []).map((tag: UpstreamTag, index: number) =>
      cassandraClient.execute(txTagsInsertQuery, transformTag(tag, tx, index), {
        prepare: true,
      })
    )
  );
};

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

  return [
    cassandraClient.execute(poaInsertQuery, transformPoaKeys(input), {
      prepare: true,
    }),
    cassandraClient.execute(
      blockStatusUpdateQuery,
      [input.height, input.indep_hash],
      { prepare: true }
    ),
    cassandraClient.execute(
      blockHeightByHashInsertQuery,
      [input.height, input.indep_hash],
      { prepare: true }
    ),
    cassandraClient.execute(
      blockInsertQuery(nonNilBlockKeys),
      blockInsertParams,
      { prepare: true }
    ),
    cassandraClient.execute(
      syncStatusUpdateQuery,
      [
        toLong(input.height),
        input.indep_hash,
        currentSessionId,
        currentSessionRandomId,
      ],
      { prepare: true }
    ),
  ];
};

export const getMaxHeightBlock = async (): Promise<
  [string, CassandraTypes.Long]
> => {
  // note that the block_hash table is sorted descendingly by block height
  const response = await cassandraClient.execute(
    'SELECT block_height,block_hash FROM gateway.block_by_tx_id limit 1;'
  );

  const row = response.rows[0];
  return [row['block_hash'], row['block_height']];
};

export const makeBlockPlaceholder = (
  blockHeight: CassandraTypes.Long | number,
  blockHash: string
): Promise<unknown> =>
  cassandraClient.execute(
    `INSERT INTO gateway.block_status (block_height, block_hash, synced, txs_synced)` +
      ` VALUES (?, ?, ?, ?) IF NOT EXISTS`,
    [toLong(blockHeight), blockHash, false, false]
  );

export const newPollStatus = (
  blockHeight: CassandraTypes.Long | number,
  blockHash: string
): Promise<unknown> =>
  cassandraClient.execute(
    `INSERT INTO gateway.poll_status (current_block_height, current_block_hash, random_uuid, txs_synced)` +
      ` VALUES (?, ?, ?, ?)`,
    [toLong(blockHeight), blockHash, false, false]
  );

export const getPlaceholderCount = async (): Promise<CassandraTypes.Long> => {
  let cnt = (cassandra as any).types.Long.fromNumber(0);
  try {
    const response = await cassandraClient.execute(
      `SELECT * FROM gateway.block_status limit 1;`
    );
    cnt = response.rows[0].block_height;
  } catch (error) {}

  return cnt;
};
