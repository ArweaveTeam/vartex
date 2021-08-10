import { Request, Response } from "express";
import { grabNode } from "../query/node.query.js";
import got from "got";

export function proxyGetRoute(request: Request, res: Response) {
  const uri = `${grabNode()}${request.originalUrl}`;
  const stream = got.stream.get(uri);
  stream.on("error", (error) => {
    res.status(404).json({
      status: 404,
      error: "Not Found",
    });

    console.log(`[GET] Failed to get: ${uri}`);
  });
  stream.on("end", () => res.end());
  stream.pipe(res);
}

export function proxyPostRoute(request: Request, res: Response) {
  const uri = `${grabNode()}${request.originalUrl}`;
  const stream = got.stream.post(uri, {
    body: JSON.stringify(request.body),
  });
  stream.on("error", (error) => {
    console.log(error);

    res.status(503).send();
    console.log(`[POST] Failed to post: ${uri}`);
  });
  stream.on("end", () => res.end());
  stream.pipe(res);
}
