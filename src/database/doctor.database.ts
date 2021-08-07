import * as R from "rambda";
import { types as CassandraTypes } from "cassandra-driver";
import { KEYSPACE } from "../constants";
import { getBlock as queryGetBlock } from "../query/block.query";
import {
  cassandraClient,
  getMaxHeightBlock,
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from "./cassandra.database";
import * as C from "./constants.database";

export const checkForBlockGaps = async (): Promise<boolean> => {
  const expectedBlockHeightResult = await cassandraClient.execute(
      `SELECT height FROM ${KEYSPACE}.block_gql_desc LIMIT 1`,
  );
  const expectedBlockHeight = expectedBlockHeightResult.rows[0].height;

  const eachBucket = R.range(
      0,
      expectedBlockHeight.divide(C.GQL_BLOCK_HEIGHT_BUCKET_SIZE).add(1).toInt(),
  );

  let totalBlocksCount = toLong(0);

  for (const bucket of eachBucket) {
    const currentPartition = toLong(bucket)
        .mul(C.GQL_BLOCK_HEIGHT_BUCKET_SIZE)
        .divide(C.GQL_BLOCK_HEIGHT_PARTITION_SIZE)
        .toString();
    const currentBucketCountResult = await cassandraClient.execute(
        `SELECT COUNT(*) from ${KEYSPACE}.block_gql_asc WHERE bucket_id='gql_bucket_block_height_asc_${bucket}' AND partition_id='gql_partition_block_height_asc_${currentPartition}'`,
    );
    totalBlocksCount = totalBlocksCount.add(
        currentBucketCountResult.rows[0].count,
    );
  }
  return !expectedBlockHeight.add(1).equals(totalBlocksCount);
};

export const findBlockGaps = async (): Promise<number[]> => {
  const topHeightQ = await cassandraClient.execute(
      `SELECT height FROM ${KEYSPACE}.block_gql_desc LIMIT 1`,
  );
  const topHeight = topHeightQ.rows[0].height;

  const queryHeightGroups = R.splitEvery(
      1000,
      R.range(0, topHeight.add(1).toInt()),
  );

  const missingHeights: number[] = [];

  for (const heightGroup of queryHeightGroups) {
    const blockQ = await cassandraClient.execute(
        `SELECT height FROM ${KEYSPACE}.block WHERE height >= ${R.head(
            heightGroup,
        )} AND height <= ${R.last(heightGroup)} ALLOW FILTERING`,
    );
    for (const height of R.range(
        R.head(heightGroup),
        R.last(heightGroup) + 1,
    )) {
      const findResult = R.findIndex((row: any) => row.height.equals(height))(
          blockQ.rows,
      );
      if (findResult < 0) {
        missingHeights.push(height);
      }
    }
    return missingHeights;
  }
};

export const findTxGaps = async (): Promise<void> => {
  const topHeightQ = await cassandraClient.execute(
      `SELECT height FROM ${KEYSPACE}.block_gql_desc LIMIT 1`,
  );
  const topHeight = topHeightQ.rows[0].height;

  const queryHeightGroups = R.splitEvery(
      C.MAX_TX_PER_BLOCK,
      R.range(0, topHeight.add(1).toInt()),
  );

  const missingTxs: string[] = [];

  for (const heightGroup of queryHeightGroups) {
    const blockQ = await cassandraClient.execute(
        `SELECT height,txs,txs_count FROM ${KEYSPACE}.block WHERE height >= ${R.head(
            heightGroup,
        )} AND height <= ${R.last(heightGroup)} ALLOW FILTERING`,
    );

    const txCounts = blockQ.rows.filter(
        (row) => row.txs_count && row.txs_count !== 0,
    );

    for (const { txs_count, height, txs } of txCounts) {
      const txCntQ = await cassandraClient.execute(
          `SELECT COUNT(*) FROM gateway.transaction WHERE block_height>=${height.divide(
              C.MAX_TX_PER_BLOCK,
          )} AND block_height<${height
              .add(1)
              .divide(C.MAX_TX_PER_BLOCK)} ALLOW FILTERING`,
      );

      if (txCntQ.rowLength !== txs_count) {
        const txDataQ = await cassandraClient.execute(
            `SELECT tx_id FROM gateway.transaction WHERE block_height>=${height.divide(
                C.MAX_TX_PER_BLOCK,
            )} AND block_height<${height
                .add(1)
                .divide(C.MAX_TX_PER_BLOCK)} ALLOW FILTERING`,
        );
        for (const { tx_id } of txDataQ.rows) {
          if (!txs.includes(tx_id)) {
            missingTxs.push(tx_id);
          }
        }
      }
    }
  }

  if (!R.isEmpty(missingTxs)) {
    console.error(
        "Very bad situation it seems these txs are missing",
        missingTxs,
    );
  }
};

// const verifyBlock = async (height: number): Promise<boolean> => {
//   const queryResponse = await cassandraClient.execute(
//     `SELECT height FROM ${KEYSPACE}.block LIMIT 1`
//   );
//   if (queryResponse && queryResponse.rowLength > 0) {
//     return false;
//   } else {
//     return true;
//   }
// };

// export const checkfixMissingTxs = async (): Promise<void> => {
//   const queryResponse = await cassandraClient.execute(
//     `SELECT height FROM ${KEYSPACE}.block_gql_desc LIMIT 1`
//   );
//   let tallestBlock = queryResponse.rows[0].height.toInt();

//   const blockGroups = R.range(0, tallestBlock).reduce(
//     ([buffer, groups], height) => {
//       if (height % 1000 === 0 && height !== 0) {
//         return [[height], R.concat(groups, buffer)];
//       } else {
//         return [R.concat(buffer, [height]), groups];
//       }
//     },
//     [[], []]
//   );
// };

// export const fixNonLinearBlockOrder = async (): Promise<void> => {
//   const queryResponse = await cassandraClient.execute(
//     `SELECT height FROM ${KEYSPACE}.block_gql_desc LIMIT 1`
//   );
//   let tallestBlock = queryResponse.rows[0].height.toInt();
//   let expectedBlocks = R.range(0, tallestBlock);

//   return await new Promise(
//     async (resolve: (val?: any) => void, reject: (err: string) => void) => {
//       cassandraClient.eachRow(
//         `SELECT height FROM ${KEYSPACE}.block`,
//         [],
//         {
//           autoPage: true,
//           prepare: false,
//           executionProfile: 'fast',
//         },
//         function (n, row) {
//           expectedBlocks = R.reject(R.equals(row.height.toInt()))(
//             expectedBlocks
//           );
//         },
//         async function (err, res) {
//           // leftover of expectedBlocks would be missing!
//           for (const missingHeight of expectedBlocks) {
//             const missingBlock = await (queryGetBlock as any)({
//               height: missingHeight,
//             });
//             await makeBlockImportQuery(missingHeight)();
//           }
//           resolve();
//         }
//       );
//     }
//   );
// };
