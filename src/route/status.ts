import * as R from "rambda";
import { Request, Response } from "express";
import {
  Client as CassandraClient,
  types as CassandraTypes,
} from "cassandra-driver";
import { statusMapper } from "../database/mapper";
import { KEYSPACE, isGatewayNodeModeEnabled } from "../constants";
import gitRev from "git-rev-sync";

let gitRevision = "unknown";

try {
  gitRevision = gitRev.long([process.cwd()]);
} catch {}

let lastKnownSessionUuid: CassandraTypes.TimeUuid;

export const initializeStatusSession = async (
  cassandraClient: CassandraClient,
  sessionUuid: CassandraTypes.TimeUuid
): Promise<CassandraTypes.TimeUuid> => {
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
    vartex_git_revision: gitRevision,
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

  await statusMapper.insert(
    R.mergeAll([lastSession, { session: sessionUuid, status: "BOOTING" }])
  );
  lastKnownSessionUuid = sessionUuid;
  return sessionUuid;
};

export async function statusRoute(
  request: Request,
  response: Response,
  next: (error?: string) => void
): Promise<void> {
  if (!lastKnownSessionUuid) {
    return next("not ready");
  }
  try {
    const currentStatus = await statusMapper.get({
      session: lastKnownSessionUuid,
    });
    const delta =
      Number.parseInt(currentStatus.arweave_height) -
      Number.parseInt(currentStatus.gateway_height);
    response.status(200).send({
      delta,
      ...currentStatus,
      vartex_git_revision: gitRevision,
    });
  } catch (error) {
    response.status(503).send(error);
  }
}
