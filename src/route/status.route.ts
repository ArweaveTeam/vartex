import { Request, Response } from "express";
import { topHeight } from "../database/sync.database.js";
import { toLong } from "../database/cassandra.database.js";
import { getNodeInfo } from "../query/node.query.js";
import gitRev from "git-rev-sync";

export const start = Date.now();

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
    const delta = toLong(info.height).sub(topHeight).toString();

    response.status(200).send({
      status: "OK",
      gatewayHeight: topHeight.toString(),
      arweaveHeight: info.height,
      delta,
      vartex_git_revision: gitRevision,
    });
  } catch (error) {
    response.status(503).send(error);
  }
}
