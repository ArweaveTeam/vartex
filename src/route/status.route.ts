import { Request, Response } from "express";
import { topHeight } from "../database/sync.database.js";
import { toLong } from "../database/cassandra.database.js";
import { getNodeInfo } from "../query/node.query.js";

export const start = Date.now();

export async function statusRoute(request: Request, response: Response) {
  try {
    const info = await getNodeInfo({ maxRetry: 1 });
    const delta = toLong(info.height).sub(topHeight).toString();

    return response.status(200).send({
      status: "OK",
      gatewayHeight: topHeight.toString(),
      arweaveHeight: info.height,
      delta,
    });
  } catch (error) {
    return response.status(503).send(error);
  }
}
