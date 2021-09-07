import { Request, Response } from "express";
import { topHeight, gatewayHeight, getTxsInFlight } from "../database/sync";
import { toLong } from "../database/cassandra";
import { getNodeInfo } from "../query/node";
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
    const delta = toLong(info.height).sub(gatewayHeight).toString();
    let txsInFlight = 0;
    try {
      txsInFlight = getTxsInFlight();
    } catch {}
    response.status(200).send({
      status: gatewayHeight.toString() === "0" ? "BOOTING" : "OK",
      gatewayHeight: gatewayHeight.toString(),
      arweaveHeight: Math.max(topHeight, info.height),
      txsInFlight,
      delta,
      vartex_git_revision: gitRevision,
    });
  } catch (error) {
    response.status(503).send(error);
  }
}
