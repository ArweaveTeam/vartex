import { types as CassandraTypes } from "cassandra-driver";

export const MAX_TX_PER_BLOCK = 1000;

export const GQL_BLOCK_HEIGHT_PARTITION_SIZE = 500_000; // Should be well under 100MB
export const GQL_BLOCK_HEIGHT_BUCKET_SIZE = 100_000; // Bit skinny bucket for greater distribution

export const GQL_TX_ID_PARTITION_SIZE = 100_000; // Low partition for greater distribution
export const GQL_TX_ID_BUCKET_SIZE = 50_000; // Very hard to guestimate 100MB

export const GQL_TX_TAG_PARTITION_SIZE = 100_000; // same guestimation
export const GQL_TX_TAG_BUCKET_SIZE = 50_000; // same guestimation

// converters to help locating buckets

export const getGqlBlockHeightAscPartitionName = (
    height: CassandraTypes.Long,
): string =>
  `gql_partition_block_height_asc_${height
      .divide(GQL_BLOCK_HEIGHT_PARTITION_SIZE)
      .toString()}`;

export const getGqlBlockHeightAscBucketName = (
    height: CassandraTypes.Long,
): string =>
  `gql_bucket_block_height_asc_${height
      .divide(GQL_BLOCK_HEIGHT_BUCKET_SIZE)
      .toString()}`;

export const getGqlBlockHeightDescPartitionName = (
    height: CassandraTypes.Long,
): string =>
  `gql_partition_block_height_desc_${height
      .divide(GQL_BLOCK_HEIGHT_PARTITION_SIZE)
      .toString()}`;

export const getGqlBlockHeightDescBucketName = (
    height: CassandraTypes.Long,
): string =>
  `gql_bucket_block_height_desc_${height
      .divide(GQL_BLOCK_HEIGHT_BUCKET_SIZE)
      .toString()}`;

export const getGqlTxIdAscPartitionName = (
    height: CassandraTypes.Long,
): string =>
  `gql_partition_tx_id_asc_${height
      .divide(GQL_TX_ID_PARTITION_SIZE)
      .toString()}`;

export const getGqlTxIdAscBucketName = (height: CassandraTypes.Long): string =>
  `gql_bucket_tx_id_asc_${height.divide(GQL_TX_ID_BUCKET_SIZE).toString()}`;

export const getGqlTxIdDescPartitionName = (
    height: CassandraTypes.Long,
): string =>
  `gql_partition_tx_id_desc_${height
      .divide(GQL_TX_ID_PARTITION_SIZE)
      .toString()}`;

export const getGqlTxIdDescBucketName = (height: CassandraTypes.Long): string =>
  `gql_bucket_tx_id_desc_${height.divide(GQL_TX_ID_BUCKET_SIZE).toString()}`;

export const getGqlTxTagAscPartitionName = (
    height: CassandraTypes.Long,
): string =>
  `gql_partition_tx_tag_asc_${height
      .divide(GQL_TX_TAG_PARTITION_SIZE)
      .toString()}`;

export const getGqlTxTagAscBucketName = (height: CassandraTypes.Long): string =>
  `gql_bucket_tx_tag_asc_${height.divide(GQL_TX_TAG_BUCKET_SIZE).toString()}`;

export const getGqlTxTagDescPartitionName = (
    height: CassandraTypes.Long,
): string =>
  `gql_partition_tx_tag_desc_${height
      .divide(GQL_TX_TAG_PARTITION_SIZE)
      .toString()}`;

export const getGqlTxTagDescBucketName = (
    height: CassandraTypes.Long,
): string =>
  `gql_bucket_tx_tag_desc_${height.divide(GQL_TX_TAG_BUCKET_SIZE).toString()}`;

export const getTxTagPartitionName = (height: CassandraTypes.Long): string =>
  `tx_tag_partition_${height.divide(GQL_TX_TAG_PARTITION_SIZE).toString()}`;

export const getTxTagBucketName = (height: CassandraTypes.Long): string =>
  `tx_tag_bucket_${height.divide(GQL_TX_TAG_BUCKET_SIZE).toString()}`;
