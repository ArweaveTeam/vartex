import * as R from 'rambda';
import { types as CassandraTypes } from 'cassandra-driver';
import { KEYSPACE } from '../constants';
import { getBlock as queryGetBlock } from '../query/block.query';
import {
  cassandraClient,
  getMaxHeightBlock,
  makeBlockImportQuery,
  makeTxImportQuery,
  toLong,
} from './cassandra.database';

const verifyBlock = async (height: number): Promise<boolean> => {
  const queryResponse = await cassandraClient.execute(
    `SELECT height FROM ${KEYSPACE}.block LIMIT 1`
  );
  if (queryResponse && queryResponse.rowLength > 0) {
    return false;
  } else {
    return true;
  }
};

export const fixNonLinearBlockOrder = async (): Promise<void> => {
  const queryResponse = await cassandraClient.execute(
    `SELECT height FROM ${KEYSPACE}.block_gql_desc LIMIT 1`
  );
  let tallestBlock = queryResponse.rows[0].height.toInt();
  let expectedBlocks = R.range(0, tallestBlock);

  return await new Promise(
    async (resolve: (val?: any) => void, reject: (err: string) => void) => {
      cassandraClient.eachRow(
        `SELECT height FROM ${KEYSPACE}.block`,
        [],
        {
          autoPage: true,
          prepare: false,
          executionProfile: 'fast',
        },
        function (n, row) {
          expectedBlocks = R.reject(R.equals(row.height.toInt()))(
            expectedBlocks
          );
        },
        async function (err, res) {
          // leftover of expectedBlocks would be missing!
          for (const missingHeight of expectedBlocks) {
            const missingBlock = await (queryGetBlock as any)({
              height: missingHeight,
            });
            await makeBlockImportQuery(missingHeight)();
          }
          resolve();
        }
      );
    }
  );
};
