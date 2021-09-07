import { Request, Response } from "express";
import { grabNode } from "../query/node";
import got from "got";

export function proxyGetRoute(request: Request, response: Response): void {
  const uri = `${grabNode()}${request.originalUrl}`;
  const stream = got.stream.get(uri);
  stream.on("error", (error) => {
    response.status(404).json({
      status: 404,
      error: "Not Found: " + error,
    });

    console.log(`[GET] Failed to get: ${uri}`);
  });
  stream.on("end", () => response.end());
  stream.pipe(response);
}

export function proxyPostRoute(request: Request, response: Response): void {
  const uri = `${grabNode()}${request.originalUrl}`;
  const stream = got.stream.post(uri, {
    body: JSON.stringify(request.body),
  });
  stream.on("error", (error) => {
    console.log(error);

    response.status(503).send();
    console.log(`[POST] Failed to post: ${uri}`);
  });
  stream.on("end", () => response.end());
  stream.pipe(response);
}
