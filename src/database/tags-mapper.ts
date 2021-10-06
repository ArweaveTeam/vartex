import * as R from "rambda";
import { mapping, types as CassandraTypes } from "cassandra-driver";
import { cassandraClient } from "./cassandra";
import { KEYSPACE } from "../constants";

const { Mapper } = mapping;

const tagMappings = [
  "Tag",
  "TagAndTxId",
  "TagAndOwner",
  "TagAndTarget",
  "TagAndBundleId",
  "TagAndDataRoot",
  "TagAndTxIdAndDataRoot",
  "TagAndTxIdAndTarget",
  "TagAndTxIdAndBundleId",
  "TagAndTxIdAndDataRoot",
  "TagAndOwnerAndTarget",
  "TagAndOwnerAndBundleId",
  "TagAndOwnerAndDataRoot",
  "TagAndTargetAndBundleId",
  "TagAndTargetAndDataRoot",
  "TagAndBundleIDAndDataRoot",
  "TagAndTxIdAndOwnerAndTarget",
  "TagAndTxIdAndOwnerAndBundleId",
  "TagAndTxIdAndOwnerAndDataRoot",
  "TagAndTxIdAndTargetAndBundleId",
  "TagAndTxIdAndTargetAndDataRoot",
  "TagAndTxIdAndBundleIdAndDataRoot",
  "TagAndOwnerAndTargetAndBundleId",
  "TagAndOwnerAndTargetAndDataRoot",
  "TagAndOwnerAndBundleIdAndDataRoot",
  "TagAndTargetAndBundleIdAndDataRoot",
  "TagAndTxIdAndOwnerAndTargetAndBundleId",
  "TagAndTxIdAndOwnerAndTargetAndDataRoot",
  "TagAndTxIdAndOwnerAndBundleIdAndDataRoot",
  "TagAndTxIdAndTargetAndBundleIdAndDataRoot",
  "TagAndOwnerAndTargetAndBundleIdAndDataRoot",
  "TagAndTxIdAndOwnerAndTargetAndBundleIdAndDataRoot",
];

const tagsMapper = new Mapper(cassandraClient, {
  mapper: {
    Tag: {
      keyspace: KEYSPACE,
      tables: [
        "tx_tag_gql_by_tagsum_asc_migration_0",
        "tx_tag_gql_by_tagsum_desc_migration_0",
      ],
    },
    TagAndTxId: {
      keyspace: KEYSPACE,
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_desc_migration_0",
      ],
    },
    TagAndOwner: {
      tables: [
        "tx_tag_gql_by_tagsum_and_owner_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_owner_desc_migration_0",
      ],
    },
    TagAndTarget: {
      tables: [
        "tx_tag_gql_by_tagsum_and_target_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_target_desc_migration_0",
      ],
    },
    TagAndBundleId: {
      tables: [
        "tx_tag_gql_by_tagsum_and_bundle_id_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_bundle_id_desc_migration_0",
      ],
    },
    TagAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_data_root_desc_migration_0",
      ],
    },
    TagAndTxIdAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_desc_migration_0",
      ],
    },
    TagAndTxIdAndTarget: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_target_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_target_desc_migration_0",
      ],
    },
    TagAndTxIdAndBundleId: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_bundle_id_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_bundle_id_desc_migration_0",
      ],
    },
    TagAndTxIdAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_data_root_desc_migration_0",
      ],
    },

    TagAndOwnerAndTarget: {
      tables: [
        "tx_tag_gql_by_tagsum_and_owner_and_target_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_owner_and_target_desc_migration_0",
      ],
    },
    TagAndOwnerAndBundleId: {
      tables: [
        "tx_tag_gql_by_tagsum_and_owner_and_bundle_id_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_owner_and_bundle_id_desc_migration_0",
      ],
    },
    TagAndOwnerAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_owner_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_owner_and_data_root_desc_migration_0",
      ],
    },
    TagAndTargetAndBundleId: {
      tables: [
        "tx_tag_gql_by_tagsum_and_target_and_bundle_id_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_target_and_bundle_id_desc_migration_0",
      ],
    },
    TagAndTargetAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_target_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_target_and_data_root_desc_migration_0",
      ],
    },
    TagAndBundleIDAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_bundle_id_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_bundle_id_and_data_root_desc_migration_0",
      ],
    },
    TagAndTxIdAndOwnerAndTarget: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_target_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_target_desc_migration_0",
      ],
    },
    TagAndTxIdAndOwnerAndBundleId: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_bundle_id_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_bundle_id_desc_migration_0",
      ],
    },
    TagAndTxIdAndOwnerAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_data_root_desc_migration_0",
      ],
    },
    TagAndTxIdAndTargetAndBundleId: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_target_and_bundle_id_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_target_and_bundle_id_desc_migration_0",
      ],
    },
    TagAndTxIdAndTargetAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_target_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_target_and_data_root_desc_migration_0",
      ],
    },
    TagAndTxIdAndBundleIdAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_bundle_id_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_bundle_id_and_data_root_desc_migration_0",
      ],
    },
    TagAndOwnerAndTargetAndBundleId: {
      tables: [
        "tx_tag_gql_by_tagsum_and_owner_and_target_and_bundle_id_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_owner_and_target_and_bundle_id_desc_migration_0",
      ],
    },
    TagAndOwnerAndTargetAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_owner_and_target_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_owner_and_target_and_data_root_desc_migration_0",
      ],
    },
    TagAndOwnerAndBundleIdAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_owner_and_bundle_id_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_owner_and_bundle_id_and_data_root_desc_migration_0",
      ],
    },
    TagAndTargetAndBundleIdAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_target_and_bundle_id_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_target_and_bundle_id_and_data_root_desc_migration_0",
      ],
    },
    TagAndTxIdAndOwnerAndTargetAndBundleId: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_target_and_bundle_id_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_target_and_bundle_id_desc_migration_0",
      ],
    },
    TagAndTxIdAndOwnerAndTargetAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_target_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_target_and_data_root_desc_migration_0",
      ],
    },
    TagAndTxIdAndOwnerAndBundleIdAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_bundle_id_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_bundle_id_and_data_root_desc_migration_0",
      ],
    },
    TagAndTxIdAndTargetAndBundleIdAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_target_and_bundle_id_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_target_and_bundle_id_and_data_root_desc_migration_0",
      ],
    },

    TagAndOwnerAndTargetAndBundleIdAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_owner_and_target_and_bundle_id_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_owner_and_target_and_bundle_id_and_data_root_desc_migration_0",
      ],
    },

    TagAndTxIdAndOwnerAndTargetAndBundleIdAndDataRoot: {
      tables: [
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_target_and_bundle_id_and_data_root_asc_migration_0",
        "tx_tag_gql_by_tagsum_and_tx_id_and_owner_and_target_and_bundle_id_and_data_root_desc_migration_0",
      ],
    },
  },
});
