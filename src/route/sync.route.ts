import { Request, Response } from "express";
import { syncHeight, topHeight } from "../database/sync.database";
import { toLong } from "../database/cassandra.database";
import { getNodeInfo } from "../query/node.query";

export const start = Date.now();

export async function syncRoute(request: Request, response: Response) {
  const info = await getNodeInfo({ maxRetry: 1, keepAlive: true });

  if (info) {
    const delta = toLong(info.height).sub(syncHeight);
    const status = delta.lt(3) ? 200 : 400;

    return response.status(status).send({
      status: "OK",
      gatewayHeight: topHeight.sub(syncHeight).toString(),
      arweaveHeight: info.height,
      delta: delta.toString(),
    });
  } else {
    return response.status(404).send();
  }
}
