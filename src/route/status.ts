import { Request, Response } from "express";
import { types as CassandraTypes } from "cassandra-driver";
import {
  topHeight,
  gatewayHeight,
  currentHeight,
  getTxsInFlight,
} from "../database/sync";
import { statusMapper } from "../database/mapper";
import { toLong } from "../database/cassandra";
import { getNodeInfo } from "../query/node";
import { KEYSPACE } from "../constants";
import gitRev from "git-rev-sync";

export const sessionUuid: CassandraTypes.TimeUuid =
  CassandraTypes.TimeUuid.fromDate(new Date());

export const initializeStatusSession = async (cassandraClient) => {
  // const maybeLastSession = await cassandraClient.execute(
  //   `SELECT * FROM ${KEYSPACE}.status session LIMIT 1`
  // );
  // console.log("maybeLastSession", maybeLastSession);
  // statusMapper.insert({ date: sessionUuid, status: "BOOTING" });
};

let gitRevision = "unknown";

try {
  gitRevision = gitRev.long([process.cwd()]);
} catch {}

export async function statusRoute(
  request: Request,
  response: Response
): Promise<void> {
  try {
    const info = await getNodeInfo({ maxRetry: 100 });
    const gwHeightLong = gatewayHeight.gt(currentHeight)
      ? gatewayHeight
      : currentHeight;
    const arweaveHeight = Math.max(topHeight, info.height);
    const delta = toLong(info.height).sub(gatewayHeight).toString();
    let txsInFlight = 0;
    try {
      txsInFlight = getTxsInFlight();
    } catch {}
    response.status(200).send({
      status: gatewayHeight.toString() === "0" ? "BOOTING" : "OK",
      gatewayHeight: gwHeightLong.toString(),
      arweaveHeight,
      txsInFlight,
      delta,
      vartex_git_revision: gitRevision,
    });
  } catch (error) {
    response.status(503).send(error);
  }
}
