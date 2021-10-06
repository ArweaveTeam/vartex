import { types as CassandraTypes } from "cassandra-driver";
import { toLong } from "./cassandra";

interface CqlPrimaryKeys {
  partition_id: string;
  bucket_id: string;
  bucket_number: string;
}

export const MAX_TX_PER_BLOCK = 1000;

export const GQL_BLOCK_HEIGHT_PARTITION_SIZE = 500_000; // Should be well under 100MB
export const GQL_BLOCK_HEIGHT_BUCKET_SIZE = 100_000; // Bit skinny bucket for greater distribution

export const GQL_TX_ID_PARTITION_SIZE = 100_000; // Low partition for greater distribution
export const GQL_TX_ID_BUCKET_SIZE = 50_000; // Very hard to guestimate 100MB

export const GQL_TX_TAG_PARTITION_SIZE = 100_000; // same guestimation
export const GQL_TX_TAG_BUCKET_SIZE = 50_000; // same guestimation

// converters to help locating buckets

export const getGqlBlockHeightAscPartitionName = (
  height: CassandraTypes.Long
): string =>
  `gql_partition_block_height_asc_${height
    .divide(GQL_BLOCK_HEIGHT_PARTITION_SIZE)
    .toString()}`;

export const getGqlBlockHeightAscBucketName = (
  height: CassandraTypes.Long
): string =>
  `gql_bucket_block_height_asc_${height
    .divide(GQL_BLOCK_HEIGHT_BUCKET_SIZE)
    .toString()}`;

export const getGqlBlockHeightAscBucketNumber = (
  height: CassandraTypes.Long
): number => height.divide(GQL_BLOCK_HEIGHT_BUCKET_SIZE).toInt();

export const convertGqlBlockHeightAscBucketNumberToPrimaryKeys = (
  bucketNumber: number
): CqlPrimaryKeys => {
  const height = toLong(bucketNumber * GQL_BLOCK_HEIGHT_BUCKET_SIZE).add(1);
  return {
    partition_id: getGqlBlockHeightAscPartitionName(height),
    bucket_id: getGqlBlockHeightAscBucketName(height),
    bucket_number: `${bucketNumber}`,
  };
};

export const getGqlBlockHeightDescPartitionName = (
  height: CassandraTypes.Long
): string =>
  `gql_partition_block_height_desc_${height
    .divide(GQL_BLOCK_HEIGHT_PARTITION_SIZE)
    .toString()}`;

export const getGqlBlockHeightDescBucketName = (
  height: CassandraTypes.Long
): string =>
  `gql_bucket_block_height_desc_${height
    .divide(GQL_BLOCK_HEIGHT_BUCKET_SIZE)
    .toString()}`;

export const getGqlBlockHeightDescBucketNumber = (
  height: CassandraTypes.Long
): number => height.divide(GQL_BLOCK_HEIGHT_BUCKET_SIZE).toInt();

export const convertGqlBlockHeightDescBucketNumberToPrimaryKeys = (
  bucketNumber: number
): CqlPrimaryKeys => {
  const height = toLong(bucketNumber * GQL_BLOCK_HEIGHT_BUCKET_SIZE).add(1);
  return {
    partition_id: getGqlBlockHeightDescPartitionName(height),
    bucket_id: getGqlBlockHeightDescBucketName(height),
    bucket_number: `${bucketNumber}`,
  };
};

export const getGqlTxIdAscPartitionName = (
  height: CassandraTypes.Long
): string =>
  `gql_partition_tx_id_asc_${height
    .divide(GQL_TX_ID_PARTITION_SIZE)
    .toString()}`;

export const getGqlTxIdAscBucketName = (height: CassandraTypes.Long): string =>
  `gql_bucket_tx_id_asc_${height.divide(GQL_TX_ID_BUCKET_SIZE).toString()}`;

export const getGqlTxIdAscBucketNumber = (
  height: CassandraTypes.Long
): number => height.divide(GQL_TX_ID_BUCKET_SIZE).toInt();

export const convertGqlTxIdAscBucketNumberToPrimaryKeys = (
  bucketNumber: number
): CqlPrimaryKeys => {
  const height = toLong(bucketNumber * GQL_TX_ID_BUCKET_SIZE).add(1);
  return {
    partition_id: getGqlTxIdAscPartitionName(height),
    bucket_id: getGqlTxIdAscBucketName(height),
    bucket_number: `${bucketNumber}`,
  };
};

export const getGqlTxIdDescPartitionName = (
  height: CassandraTypes.Long
): string =>
  `gql_partition_tx_id_desc_${height
    .divide(GQL_TX_ID_PARTITION_SIZE)
    .toString()}`;

export const getGqlTxIdDescBucketName = (height: CassandraTypes.Long): string =>
  `gql_bucket_tx_id_desc_${height.divide(GQL_TX_ID_BUCKET_SIZE).toString()}`;

export const getGqlTxIdDescBucketNumber = (
  height: CassandraTypes.Long
): number => height.divide(GQL_TX_ID_BUCKET_SIZE).toInt();

export const convertGqlTxIdDescBucketNumberToPrimaryKeys = (
  bucketNumber: number
): CqlPrimaryKeys => {
  const height = toLong(bucketNumber * GQL_TX_ID_BUCKET_SIZE).add(1);
  return {
    partition_id: getGqlTxIdDescPartitionName(height),
    bucket_id: getGqlTxIdDescBucketName(height),
    bucket_number: `${bucketNumber}`,
  };
};

export const getGqlTxTagAscPartitionName = (
  height: CassandraTypes.Long
): string =>
  `gql_partition_tx_tag_asc_${height
    .divide(GQL_TX_TAG_PARTITION_SIZE)
    .toString()}`;

export const getGqlTxTagAscBucketName = (height: CassandraTypes.Long): string =>
  `gql_bucket_tx_tag_asc_${height.divide(GQL_TX_TAG_BUCKET_SIZE).toString()}`;

export const getGqlTxTagAscBucketNumber = (
  height: CassandraTypes.Long
): number => height.divide(GQL_TX_TAG_BUCKET_SIZE).toInt();

export const convertGqlTxTagAscBucketNumberToPrimaryKeys = (
  bucketNumber: number
): CqlPrimaryKeys => {
  const height = toLong(bucketNumber * GQL_TX_TAG_BUCKET_SIZE).add(1);
  return {
    partition_id: getGqlTxTagAscPartitionName(height),
    bucket_id: getGqlTxTagAscBucketName(height),
    bucket_number: `${bucketNumber}`,
  };
};

export const getGqlTxTagDescPartitionName = (
  height: CassandraTypes.Long
): string =>
  `gql_partition_tx_tag_desc_${height
    .divide(GQL_TX_TAG_PARTITION_SIZE)
    .toString()}`;

export const getGqlTxTagDescBucketName = (
  height: CassandraTypes.Long
): string =>
  `gql_bucket_tx_tag_desc_${height.divide(GQL_TX_TAG_BUCKET_SIZE).toString()}`;

export const getGqlTxTagDescBucketNumber = (
  height: CassandraTypes.Long
): number => height.divide(GQL_TX_TAG_BUCKET_SIZE).toInt();

export const convertGqlTxTagDescBucketNumberToPrimaryKeys = (
  bucketNumber: number
): CqlPrimaryKeys => {
  const height = toLong(bucketNumber * GQL_TX_TAG_BUCKET_SIZE).add(1);
  return {
    partition_id: getGqlTxTagDescPartitionName(height),
    bucket_id: getGqlTxTagDescBucketName(height),
    bucket_number: `${bucketNumber}`,
  };
};

export const getTxTagPartitionName = (height: CassandraTypes.Long): string =>
  `tx_tag_partition_${height.divide(GQL_TX_TAG_PARTITION_SIZE).toString()}`;

export const getTxTagBucketName = (height: CassandraTypes.Long): string =>
  `tx_tag_bucket_${height.divide(GQL_TX_TAG_BUCKET_SIZE).toString()}`;

export const getTxTagBucketNumber = (height: CassandraTypes.Long): number =>
  height.divide(GQL_TX_TAG_BUCKET_SIZE).toInt();
