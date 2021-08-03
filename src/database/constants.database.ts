import { types as CassandraTypes } from 'cassandra-driver';

export const MAX_TX_PER_BLOCK = 1000;

export const GQL_BLOCK_HEIGHT_PARTITION_SIZE = 500000; // Should be well under 100MB
export const GQL_BLOCK_HEIGHT_BUCKET_SIZE = 100000; // Bit skinny bucket for greater distribution

export const GQL_TX_ID_PARTITION_SIZE = 100000; // Low partition for greater distribution
export const GQL_TX_ID_BUCKET_SIZE = 50000; // Very hard to guestimate 100MB

export const GQL_TX_TAG_PARTITION_SIZE = 100000; // same guestimation
export const GQL_TX_TAG_BUCKET_SIZE = 50000; // same guestimation

// converters to help locating buckets

export const getGqlBlockHeightAscPartitionName = (
  height: CassandraTypes.Long
): string =>
  `gql_partition_block_height_asc_${height
    .div(GQL_BLOCK_HEIGHT_PARTITION_SIZE)
    .toString()}`;

export const getGqlBlockHeightAscBucketName = (
  height: CassandraTypes.Long
): string =>
  `gql_bucket_block_height_asc_${height
    .div(GQL_BLOCK_HEIGHT_BUCKET_SIZE)
    .toString()}`;

export const getGqlBlockHeightDescPartitionName = (
  height: CassandraTypes.Long
): string =>
  `gql_partition_block_height_desc_${height
    .div(GQL_BLOCK_HEIGHT_PARTITION_SIZE)
    .toString()}`;

export const getGqlBlockHeightDescBucketName = (
  height: CassandraTypes.Long
): string =>
  `gql_bucket_block_height_desc_${height
    .div(GQL_BLOCK_HEIGHT_BUCKET_SIZE)
    .toString()}`;

export const getGqlTxIdAscPartitionName = (
  txIndex: CassandraTypes.Long
): string =>
  `gql_partition_tx_id_asc_${txIndex.div(GQL_TX_ID_PARTITION_SIZE).toString()}`;

export const getGqlTxIdAscBucketName = (txIndex: CassandraTypes.Long): string =>
  `gql_bucket_tx_id_asc_${txIndex.div(GQL_TX_ID_BUCKET_SIZE).toString()}`;

export const getGqlTxIdDescPartitionName = (
  txIndex: CassandraTypes.Long
): string =>
  `gql_partition_tx_id_desc_${txIndex
    .div(GQL_TX_ID_PARTITION_SIZE)
    .toString()}`;

export const getGqlTxIdDescBucketName = (
  txIndex: CassandraTypes.Long
): string =>
  `gql_bucket_tx_id_desc_${txIndex.div(GQL_TX_ID_BUCKET_SIZE).toString()}`;

export const getGqlTxTagAscPartitionName = (
  txIndex: CassandraTypes.Long
): string =>
  `gql_partition_tx_tag_asc_${txIndex
    .div(GQL_TX_TAG_PARTITION_SIZE)
    .toString()}`;

export const getGqlTxTagAscBucketName = (
  txIndex: CassandraTypes.Long
): string =>
  `gql_bucket_tx_tag_asc_${txIndex.div(GQL_TX_TAG_BUCKET_SIZE).toString()}`;

export const getGqlTxTagDescPartitionName = (
  txIndex: CassandraTypes.Long
): string =>
  `gql_partition_tx_tag_desc_${txIndex
    .div(GQL_TX_TAG_PARTITION_SIZE)
    .toString()}`;

export const getGqlTxTagDescBucketName = (
  txIndex: CassandraTypes.Long
): string =>
  `gql_bucket_tx_tag_desc_${txIndex.div(GQL_TX_TAG_BUCKET_SIZE).toString()}`;
