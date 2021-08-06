import * as R from 'rambda';
import { types as CassandraTypes } from 'cassandra-driver';
import * as Constants from '../database/constants.database';
import { config } from 'dotenv';
import { KEYSPACE } from '../constants';
import { indices } from '../utility/order.utility';
import { ISO8601DateTimeString } from '../utility/encoding.utility';
import { TagFilter } from './types';
import { toB64url } from '../query/transaction.query';
import { default as cqlBuilder } from '@ridi/cql-builder';

const { Insert, Select, Update, Delete, CqlBuilderError } = cqlBuilder;

export type TxSortOrder = 'HEIGHT_ASC' | 'HEIGHT_DESC';

config();

export interface QueryParams {
  to?: string[];
  from?: string[];
  id?: string;
  ids?: string[];
  limit?: number;
  offset?: number;
  select?: any;
  blocks?: boolean;
  since?: string;
  before?: string;
  sortOrder?: TxSortOrder;
  status?: 'any' | 'confirmed' | 'pending';
  tags?: TagFilter[];
  pendingMinutes?: number;
  minHeight?: CassandraTypes.Long;
  maxHeight?: CassandraTypes.Long;
}

export function generateTransactionQuery(params: QueryParams): any {
  let table = 'tx_id_gql_desc';

  table =
    params.sortOrder === 'HEIGHT_ASC' ? 'tx_id_gql_asc' : 'tx_id_gql_desc';

  const cql = Select().table(table, KEYSPACE).field(params.select).filtering();

  if (params.id !== undefined) {
    cql.where('tx_id = ?', params.id);
    return cql.build();
  } else if (params.ids && Array.isArray(params.ids)) {
    cql.where.apply(
      cql,
      R.concat(
        [
          `tx_id IN ( ${R.range(0, params.ids.length)
            .map(() => '?')
            .join(', ')} )`,
        ],
        params.ids
      )
    );
  }

  if (Array.isArray(params.tags) && !R.isEmpty(params.tags)) {
    for (const { name, values = '' } of params.tags) {
      if (Array.isArray(values)) {
        for (const value of values) {
          cql.where(
            `tags CONTAINS (?, ?)`,
            toB64url(name || ''),
            toB64url(value || '')
          );
        }
      } else {
        cql.where(
          `tags CONTAINS (?, ?)`,
          toB64url(name || ''),
          toB64url(values || '')
        );
      }
    }
  }

  if (params.since) {
    cql.where(
      'block_timestamp < ?',
      CassandraTypes.Long.fromNumber(
        Math.floor(
          CassandraTypes.TimeUuid.fromString(params.since).getDate().valueOf() /
            1000
        )
      )
    );
  }

  // if (params.status === 'confirmed') {
  //   cql.where('block_height >= ?', CassandraTypes.Long.fromNumber(0));
  // }

  if (params.to) {
    cql.where('target = ?', params.to);
  }

  // if (params.before) {
  //   cql.where('timestamp < ?', params.before);
  // }

  cql.where(
    'tx_index >= ?',
    params.sortOrder === 'HEIGHT_ASC'
      ? params.minHeight.add(params.offset).toString()
      : params.minHeight.toString()
  );

  cql.where(
    'tx_index <= ?',
    params.sortOrder === 'HEIGHT_DESC'
      ? params.maxHeight.sub(params.offset).toString()
      : params.maxHeight.toString()
  );

  cql.limit(params.limit);

  return cql.build();
}

export interface BlockQueryParams {
  id?: string;
  ids?: string[];
  select?: any;
  before?: string;
  offset: number;
  fetchSize: number;
  minHeight?: CassandraTypes.Long;
  maxHeight?: CassandraTypes.Long;
  sortOrder?: TxSortOrder;
}

export function generateBlockQuery(params: BlockQueryParams): any {
  const {
    id,
    ids,
    select,
    before,
    offset = 0,
    fetchSize,
    minHeight,
    maxHeight,
  } = params;

  const cql = Select()
    .table(
      params.sortOrder === 'HEIGHT_ASC' ? 'block_gql_asc' : 'block_gql_desc',
      KEYSPACE
    )
    .field(
      select.includes('indep_hash') ? select : R.append('indep_hash', select)
    )
    .filtering();

  // const query = connection.queryBuilder().select(select).from('blocks');
  if (id) {
    cql.where('indep_hash = ?', id);
  } else if (ids && Array.isArray(ids) && !R.isEmpty(ids)) {
    cql.where.apply(
      cql,
      R.concat(
        [
          `indep_hash IN ( ${R.range(0, params.ids.length)
            .map(() => '?')
            .join(', ')} )`,
        ],
        params.ids
      )
    );
  }

  if (before) {
    cql.where('timestamp < ?', before);
  }

  cql.where(
    'height >= ?',
    params.sortOrder === 'HEIGHT_ASC'
      ? minHeight.add(offset).toString()
      : minHeight.toString()
  );

  cql.where(
    'height <= ?',
    params.sortOrder === 'HEIGHT_DESC'
      ? (maxHeight as any).sub(offset).toString()
      : maxHeight.toString()
  );

  cql.limit(fetchSize);

  return cql.build();
}

export interface DeferedBlockQueryParams {
  indep_hash: string;
  deferedSelect: string[];
}

export function generateDeferedBlockQuery(
  params: DeferedBlockQueryParams
): any {
  return Select()
    .table('block', KEYSPACE)
    .where('indep_hash = ?', params.indep_hash)
    .field(params.deferedSelect)
    .build();
}

export function generateDeferedTxQuery(params: any): any {
  return Select()
    .table('transaction', KEYSPACE)
    .where('tx_id = ?', params.tx_id)
    .field(params.deferedSelect)
    .build();
}

export function generateDeferedTxBlockQuery(
  height: CassandraTypes.Long,
  fieldSelect: any
): any {
  return Select()
    .table('block_gql_asc', KEYSPACE)
    .field(fieldSelect)
    .where('height = ?', height)
    .where(
      'partition_id = ?',
      Constants.getGqlBlockHeightAscPartitionName(height)
    )
    .where('bucket_id = ?', Constants.getGqlBlockHeightAscBucketName(height))
    .build();
}

export function generateTagQuery(tags: TagFilter[]) {
  const cql = Select().table('tx_tag', KEYSPACE).field('tx_id').filtering();
  tags.forEach((tag) => {
    cql.where('name = ?', tag.name.toString());
    if (Array.isArray(tag.values)) {
      cql.where.apply(
        cql,
        R.concat(
          [
            `value IN ( ${R.range(0, tag.values.length)
              .map(() => '?')
              .join(', ')} )`,
          ],
          tag.values
        )
      );
    } else {
      cql.where('value = ?', (tag.values as any).toString());
    }
  });
  return cql.build();
}
