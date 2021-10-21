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

let ready = false;

let lastKnownSessionUuid: CassandraTypes.TimeUuid;

interface StatusSchema {
  session?: CassandraTypes.TimeUuid;
  status: string;
  arweave_height: string;
  gateway_height: string;
  vartex_git_revision: string;
  current_imports: string[];
  current_migrations: Record<string, string>;
}

function signalReady(): void {
  setTimeout(() => {
    if (!ready) {
      try {
        gitRevision = gitRev.long([process.cwd()]);
      } catch {}

      ready = true;
    }
  }, 2000);
}

export const initializeStatusSession = async (
  cassandraClient: CassandraClient,
  sessionUuid: CassandraTypes.TimeUuid
): Promise<CassandraTypes.TimeUuid> => {
  const maybeLastSession = await cassandraClient.execute(
    `SELECT * FROM ${KEYSPACE}.status`
  );

  if (isGatewayNodeModeEnabled && !R.isEmpty(maybeLastSession.rows)) {
    signalReady();
    lastKnownSessionUuid = maybeLastSession.rows[0].session;
    return maybeLastSession.rows[0].session;
  } else if (isGatewayNodeModeEnabled) {
    lastKnownSessionUuid = sessionUuid;
    signalReady();
    return sessionUuid;
  }

  let lastSession: StatusSchema = {
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
    lastSession = (maybeLastSession as any).rows[0] as StatusSchema;
  }

  await statusMapper.insert(
    R.mergeAll([lastSession, { session: sessionUuid, status: "BOOTING" }])
  );
  signalReady();
  lastKnownSessionUuid = sessionUuid;
  return sessionUuid;
};

export async function statusRoute(
  request: Request,
  response: Response,
  next: (error?: string) => void
): Promise<void> {
  if (!ready) {
    response.sendStatus(503);
  } else {
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
      response.sendStatus(503);
    }
  }
}
