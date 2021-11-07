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
  let maybeLastSession;

  let lastSession: StatusSchema = {
    status: "BOOTING",
    arweave_height: "0",
    gateway_height: "0",
    vartex_git_revision: gitRevision,
    current_imports: [],
    current_migrations: {},
  };

  try {
    maybeLastSession = await cassandraClient.execute(
      `SELECT * FROM ${KEYSPACE}.status`
    );
  } catch {}

  if (!maybeLastSession) {
    await new Promise<void>((resolve) => setTimeout(resolve, 1000));
    return initializeStatusSession(cassandraClient, sessionUuid);
  } else if (isGatewayNodeModeEnabled && !R.isEmpty(maybeLastSession.rows)) {
    signalReady();
    lastKnownSessionUuid = maybeLastSession.rows[0].session;
    return lastKnownSessionUuid;
  } else if (isGatewayNodeModeEnabled && R.isEmpty(maybeLastSession.rows)) {
    lastKnownSessionUuid = sessionUuid;
    signalReady();
    return lastKnownSessionUuid as CassandraTypes.TimeUuid;
  }

  if (!R.isEmpty(maybeLastSession.rows)) {
    for (const { session } of maybeLastSession.rows) {
      await cassandraClient.execute(
        `DELETE FROM ${KEYSPACE}.status WHERE session = ${session} IF EXISTS`
      );
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    lastSession = (maybeLastSession as any).rows[0] as StatusSchema;
  }

  const newSession = R.mergeAll([
    lastSession,
    { session: sessionUuid, status: "BOOTING" },
  ]);
  try {
    await statusMapper.insert(newSession);
  } catch {
    await new Promise<void>((resolve) => setTimeout(resolve, 1000));
    return initializeStatusSession(cassandraClient, sessionUuid);
  }

  signalReady();
  lastKnownSessionUuid = sessionUuid;
  return lastKnownSessionUuid;
};

export async function statusRoute(
  request: Request,
  response: Response
): Promise<void> {
  if (!ready) {
    response.send("not ready");
  } else {
    let currentStatus: StatusSchema;
    try {
      currentStatus = await statusMapper.get({
        session: lastKnownSessionUuid,
      });
    } catch {}

    try {
      const delta =
        Number.parseInt(currentStatus ? currentStatus.arweave_height : "0") -
        Number.parseInt(currentStatus ? currentStatus.gateway_height : "0");
      response.status(200).send({
        delta,
        ...currentStatus,
        vartex_git_revision: gitRevision,
      });
    } catch (error) {
      response.send(`${error}`);
    }
  }
}
