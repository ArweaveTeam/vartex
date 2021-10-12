import * as R from "rambda";
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
import { KEYSPACE, isGatewayNodeModeEnabled } from "../constants";
import gitRev from "git-rev-sync";

let lastKnownSessionUuid: CassandraTypes.TimeUuid;

export const initializeStatusSession = async (
  cassandraClient,
  sessionUuid: CassandraTypes.TimeUuid
) => {
  const maybeLastSession = await cassandraClient.execute(
    `SELECT * FROM ${KEYSPACE}.status`
  );

  if (isGatewayNodeModeEnabled && !R.isEmpty(maybeLastSession.rows)) {
    lastKnownSessionUuid = maybeLastSession.rows[0].session;
    return maybeLastSession.rows[0].session;
  } else if (isGatewayNodeModeEnabled) {
    lastKnownSessionUuid = sessionUuid;
    return sessionUuid;
  }

  let lastSession = {
    status: "BOOTING",
    arweave_height: "-1",
    gateway_height: "-1",
    vartex_git_revision: "",
    current_imports: [],
    current_migrations: {},
  };

  if (!R.isEmpty(maybeLastSession.rows)) {
    for (const { session } of maybeLastSession.rows) {
      await cassandraClient.execute(
        `DELETE FROM ${KEYSPACE}.status WHERE session = ${session} IF EXISTS`
      );
    }
    lastSession = maybeLastSession.rows[0];
  }
  // console.log("maybeLastSession", maybeLastSession);
  await statusMapper.insert(
    R.mergeAll([lastSession, { session: sessionUuid, status: "BOOTING" }])
  );
  lastKnownSessionUuid = sessionUuid;
  return sessionUuid;
};

let gitRevision = "unknown";

try {
  gitRevision = gitRev.long([process.cwd()]);
} catch {}

export async function statusRoute(
  request: Request,
  response: Response,
  next: any
): Promise<void> {
  if (!lastKnownSessionUuid) {
    return next();
  }
  try {
    const currentStatus = await statusMapper.get({
      session: lastKnownSessionUuid,
    });
    response.status(200).send({
      status: currentStatus.status,
      gatewayHeight: currentStatus.gateway_height,
      arweaveHeight: currentStatus.arweave_height,
      // txsInFlight,
      // delta,
      vartex_git_revision: currentStatus.vartex_git_revision,
    });
    // console.log({ currentStatus });
    // const info = await getNodeInfo({ maxRetry: 100 });
    // const gwHeightLong = gatewayHeight.gt(currentHeight)
    //   ? gatewayHeight
    //   : currentHeight;
    // const arweaveHeight = Math.max(topHeight, info.height);
    // const delta = toLong(info.height).sub(gatewayHeight).toString();
    // let txsInFlight = 0;
    // try {
    //   txsInFlight = getTxsInFlight();
    // } catch {}
    // response.status(200).send({
    //   status: gatewayHeight.toString() === "0" ? "BOOTING" : "OK",
    //   gatewayHeight: gwHeightLong.toString(),
    //   arweaveHeight,
    //   txsInFlight,
    //   delta,
    //   vartex_git_revision: gitRevision,
    // });
  } catch (error) {
    response.status(503).send(error);
  }
}
