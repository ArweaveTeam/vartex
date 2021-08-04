import crypto from 'crypto';
import * as cassandra from 'cassandra-driver';
import * as R from 'rambda';
import { types as CassandraTypes } from 'cassandra-driver';
import { ImportQueue, Poa, TxOffset } from '../types/cassandra.types.js';
import { KEYSPACE } from '../constants.js';
import { config } from 'dotenv';
import {
  MAX_TX_PER_BLOCK,
  getGqlBlockHeightAscPartitionName,
  getGqlBlockHeightAscBucketName,
  getGqlBlockHeightDescPartitionName,
  getGqlBlockHeightDescBucketName,
  getGqlTxIdAscPartitionName,
  getGqlTxIdAscBucketName,
  getGqlTxIdDescPartitionName,
  getGqlTxIdDescBucketName,
  getGqlTxTagAscPartitionName,
  getGqlTxTagAscBucketName,
  getGqlTxTagDescPartitionName,
  getGqlTxTagDescBucketName,
  getTxTagPartitionName,
  getTxTagBucketName,
} from './constants.database';

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

export const cassandraClient = new cassandra.Client({
  contactPoints,
  localDataCenter: 'datacenter1',
  queryOptions: { isIdempotent: true },
  // encoding: {
  //   map: Map,
  //   set: Set,
  // },
  socketOptions: {
    connectTimeout: 5000,
    defunctReadTimeoutThreshold: 64,
    keepAlive: true,
    keepAliveDelay: 0,
    readTimeout: 30000,
    tcpNoDelay: true,
    coalescingThreshold: 65536,
  },
  protocolOptions: {
    maxSchemaAgreementWaitSeconds: process.env['DB_TIMEOUT']
      ? parseInt(process.env['DB_TIMEOUT'])
      : 30,
  },
  profiles: [
    new cassandra.ExecutionProfile('fast', {
      readTimeout: 5000,
      consistency: cassandra.types.consistencies.any,
      serialConsistency: cassandra.types.consistencies.any,
    }),
    new cassandra.ExecutionProfile('gql', {
      readTimeout: 5000,
      consistency: cassandra.types.consistencies.all,
      serialConsistency: cassandra.types.consistencies.serial,
    }),
    new cassandra.ExecutionProfile('full', {
      readTimeout: 15000,
      consistency: cassandra.types.consistencies.all,
      serialConsistency: cassandra.types.consistencies.serial,
      graphOptions: {
        writeConsistency: cassandra.types.consistencies.all,
      },
    }),
  ],
});

const poaKeys = [
  'option',
  'tx_path',
  'data_path',
  'chunk',
  'block_hash',
  'block_height',
];

const blockGqlKeys = ['height', 'indep_hash'];

const txTagKeys = [
  'partition_id',
  'bucket_id',
  'tx_id',
  'tx_index',
  'tag_index',
  'next_tag_index',
  'name',
  'value',
];

const txOffsetKeys = ['tx_id', 'size', 'offset'];

const transactionKeys = [
  'tx_index',
  'block_height',
  'block_hash',
  'data_root',
  'data_size',
  'data_tree',
  'format',
  'tx_id',
  'last_tx',
  'owner',
  'quantity',
  'reward',
  'signature',
  'target',
  'tags',
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
      return (
        !R.isEmpty(obj.tags) &&
        (obj.tags || []).map(({ name, value }) =>
          CassandraTypes.Tuple.fromArray([name, value])
        )
      );
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

const transformTxKey = (
  key: string,
  txIndex: CassandraTypes.Long,
  txData: any,
  blockData: any
) => {
  switch (key) {
    case 'tx_index': {
      return txIndex;
    }
    case 'block_timestamp': {
      return toLong(blockData['timestamp']);
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
    case 'tags': {
      return (txData.tags || []).map(({ name, value }) =>
        CassandraTypes.Tuple.fromArray([name, value])
      );
    }

    case 'tx_id': {
      return txData.id;
    }
    case 'data_root':
    case 'last_tx':
    case 'owner':
    case 'signature':
    case 'target': {
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

const transformTxOffsetKeys = (txObj: any): TxOffset => {
  const txOffset = txObj['tx_offset'] ? txObj['tx_offset'] : {};
  const txOffsetObj = {} as TxOffset;
  txOffsetObj['tx_id'] = txObj['id'] || '';
  txOffsetObj['size'] = toLong(txOffset['size'] || 0);
  txOffsetObj['offset'] = toLong(txOffset['offset'] || -1);
  return txOffsetObj;
};

interface Tag {
  partition_id: string;
  bucket_id: string;
  tag_index: number;
  tx_index: CassandraTypes.Long;
  tx_id: string;
  name: string;
  value: string;
}

type UpstreamTag = { name: string; value: string };

const transformTag = (
  tag: UpstreamTag,
  txObj: any,
  blockHeight: CassandraTypes.Long,
  txIndex: CassandraTypes.Long,
  index: number,
  nextIndex?: number
): Tag => {
  const tagObj = {} as Tag;
  tagObj['partition_id'] = getTxTagPartitionName(blockHeight);
  tagObj['bucket_id'] = getTxTagBucketName(blockHeight);
  tagObj['tag_index'] = index;
  tagObj['next_tag_index'] = nextIndex || undefined;
  tagObj['tx_index'] = txIndex;
  tagObj['tx_id'] = txObj['id'];
  tagObj['name'] = tag.name || '';
  tagObj['value'] = tag.value || '';
  return tagObj;
};

const poaInsertQuery = `INSERT INTO ${KEYSPACE}.poa (${poaKeys.join(
  ', '
)}) VALUES (${poaKeys.map(() => '?').join(', ')})`;

const blockInsertQuery = (nonNilBlockKeys: string[]) =>
  `INSERT INTO ${KEYSPACE}.block (${nonNilBlockKeys.join(
    ', '
  )}) VALUES (${nonNilBlockKeys.map(() => '?').join(', ')})`;

const transactionInsertQuery = (nonNilTxKeys: string[]) =>
  `INSERT INTO ${KEYSPACE}.transaction (${nonNilTxKeys.join(
    ', '
  )}) VALUES (${nonNilTxKeys.map(() => '?').join(', ')})`;

const txOffsetInsertQuery = `INSERT INTO ${KEYSPACE}.tx_offset (${txOffsetKeys.join(
  ', '
)}) VALUES (${txOffsetKeys.map(() => '?').join(', ')})`;

const txTagsInsertQuery = `INSERT INTO ${KEYSPACE}.tx_tag (${txTagKeys.join(
  ', '
)}) VALUES (${txTagKeys.map(() => '?').join(', ')})`;

const blockHeightByHashInsertQuery = `INSERT INTO ${KEYSPACE}.block_height_by_block_hash (block_height, block_hash) VALUES (?, ?) IF NOT EXISTS`;

// const blockByTxIdInsertQuery = `INSERT INTO ${KEYSPACE}.block_by_tx_id (tx_id, block_height, block_hash) VALUES (?, ?, ?) IF NOT EXISTS`;

const blockGqlInsertAscQuery = `INSERT INTO ${KEYSPACE}.block_gql_asc (partition_id, bucket_id, height, indep_hash, timestamp) VALUES (?, ?, ?, ?, ?)`;

const blockGqlInsertDescQuery = `INSERT INTO ${KEYSPACE}.block_gql_desc (partition_id, bucket_id, height, indep_hash, timestamp) VALUES (?, ?, ?, ?, ?)`;

const txIdGqlInsertAscQuery = `INSERT INTO ${KEYSPACE}.tx_id_gql_asc
                               (partition_id, bucket_id, tx_index, tags, tx_id, owner, target, bundle_id)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;

const txIdGqlInsertDescQuery = `INSERT INTO ${KEYSPACE}.tx_id_gql_desc
                                (partition_id, bucket_id, tx_index, tags, tx_id, owner, target, bundle_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;

const txTagGqlInsertAscQuery = `INSERT INTO ${KEYSPACE}.tx_tag_gql_by_name_asc
                                (partition_id, bucket_id, tx_index, tag_index, tag_value, tag_name, tx_id, owner, target, bundle_id)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

const txTagGqlInsertDescQuery = `INSERT INTO ${KEYSPACE}.tx_tag_gql_by_name_desc
                                 (partition_id, bucket_id, tx_index, tag_index, tag_value, tag_name, tx_id, owner, target, bundle_id)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

export const makeTxImportQuery = (
  height: CassandraTypes.Long,
  txIndex: CassandraTypes.Long,
  tx: { [k: string]: any },
  blockData: { [k: string]: any }
) => () => {
  let dataSize: CassandraTypes.Long | undefined;
  const nonNilTxKeys: string[] = [];
  const txInsertParams: { [k: string]: any } = transactionKeys.reduce(
    (paramz: Array<any>, key: string) => {
      const nextVal = transformTxKey(key, txIndex, tx, blockData);

      if (key === 'data_size') {
        dataSize = nextVal;
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
    cassandraClient.execute(
      transactionInsertQuery(nonNilTxKeys),
      txInsertParams,
      { prepare: true, executionProfile: 'full' }
    ),

    cassandraClient.execute(
      txIdGqlInsertAscQuery,
      [
        getGqlTxIdAscPartitionName(height),
        getGqlTxIdAscBucketName(height),
        txIndex,
        (tx.tags || []).map(({ name, value }) =>
          CassandraTypes.Tuple.fromArray([name, value])
        ),
        tx.id,
        tx.owner,
        tx.target,
        '', // FIXME ANS-102/ANS-104
      ],
      { prepare: true, executionProfile: 'full' }
    ),
    cassandraClient.execute(
      txIdGqlInsertDescQuery,
      [
        getGqlTxIdDescPartitionName(height),
        getGqlTxIdDescBucketName(height),
        txIndex,
        (tx.tags || []).map(({ name, value }) =>
          CassandraTypes.Tuple.fromArray([name, value])
        ),
        tx.id,
        tx.owner,
        tx.target,
        '', // FIXME ANS-102/ANS-104
      ],
      { prepare: true, executionProfile: 'full' }
    ),
  ]
    .concat(
      (tx.tags || []).map((tag: UpstreamTag, index: number) =>
        cassandraClient.execute(
          txTagGqlInsertAscQuery,
          [
            getGqlTxTagAscPartitionName(height),
            getGqlTxTagAscBucketName(height),
            txIndex,
            index,
            tag.value || '',
            tag.name || '',
            tx.id,
            tx.owner,
            tx.target,
            '', // FIXME ANS-102/ANS-104
          ],
          { prepare: true, executionProfile: 'full' }
        )
      )
    )
    .concat(
      (tx.tags || []).map((tag: UpstreamTag, index: number) =>
        cassandraClient.execute(
          txTagGqlInsertDescQuery,
          [
            getGqlTxTagDescPartitionName(height),
            getGqlTxTagDescBucketName(height),
            txIndex,
            index,
            tag.value || '',
            tag.name || '',
            tx.id,
            tx.owner,
            tx.target,
            '', // FIXME ANS-102/ANS-104
          ],
          { prepare: true, executionProfile: 'full' }
        )
      )
    )
    .concat(
      (tx.tags || []).map((tag: UpstreamTag, index: number) =>
        cassandraClient.execute(
          txTagsInsertQuery,
          transformTag(
            tag,
            tx,
            height,
            txIndex,
            index,
            index + 1 < tx.tags.length ? index + 1 : undefined
          ),
          {
            prepare: true,
            executionProfile: 'full',
          }
        )
      )
    )
    .concat(
      dataSize && dataSize.gt(0)
        ? [
            cassandraClient.execute(
              txOffsetInsertQuery,
              transformTxOffsetKeys(tx),
              {
                prepare: true,
                executionProfile: 'full',
              }
            ),
          ]
        : []
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

  const height = toLong(input.height);
  const blockTimeUuid = CassandraTypes.TimeUuid.fromDate(
    new Date(input.timestamp * 1000)
  );

  return [
    cassandraClient.execute(poaInsertQuery, transformPoaKeys(input), {
      prepare: true,
      executionProfile: 'full',
    }),
    cassandraClient.execute(
      blockGqlInsertAscQuery,
      [
        getGqlBlockHeightAscPartitionName(height),
        getGqlBlockHeightAscBucketName(height),
        height,
        input.indep_hash,
        blockTimeUuid,
      ],
      { prepare: true, executionProfile: 'full' }
    ),
    cassandraClient.execute(
      blockGqlInsertDescQuery,
      [
        getGqlBlockHeightDescPartitionName(height),
        getGqlBlockHeightDescBucketName(height),
        height,
        input.indep_hash,
        blockTimeUuid,
      ],
      { prepare: true, executionProfile: 'full' }
    ),
    cassandraClient.execute(
      blockHeightByHashInsertQuery,
      [height, input.indep_hash],
      { prepare: true, executionProfile: 'full' }
    ),
    cassandraClient.execute(
      blockInsertQuery(nonNilBlockKeys),
      blockInsertParams,
      { prepare: true, executionProfile: 'full' }
    ),
  ];
};

export const getMaxHeightBlock = async (): Promise<
  [string, CassandraTypes.Long]
> => {
  // note that the block_hash table is sorted descendingly by block height
  const response = await cassandraClient.execute(
    `SELECT height,indep_hash FROM ${KEYSPACE}.block_gql limit 1;`
  );

  const row = response.rows[0];
  if (row) {
    return [row['indep_hash'], row['height']];
  } else {
    return ['', toLong(-1)];
  }
};
