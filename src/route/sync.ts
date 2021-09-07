import { Request, Response } from "express";
import { syncHeight, topHeight } from "../database/sync";
import { toLong } from "../database/cassandra";
import { getNodeInfo } from "../query/node";

export const start = Date.now();

export async function syncRoute(request: Request, response: Response): void {
  const info = await getNodeInfo({ maxRetry: 1, keepAlive: true });

  if (info) {
    const delta = toLong(info.height).sub(syncHeight);
    const status = delta.lt(3) ? 200 : 400;
    response.status(status).send({
      status: "OK",
      gatewayHeight: topHeight.sub(syncHeight).toString(),
      arweaveHeight: info.height,
      delta: delta.toString(),
    });
  } else {
    response.status(404).send();
  }
}
