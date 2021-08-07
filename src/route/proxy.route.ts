import { Request, Response } from 'express';
import { grabNode } from '../query/node.query.js';
import got from 'got';

export function proxyGetRoute(req: Request, res: Response) {
  const uri = `${grabNode()}${req.originalUrl}`;
  const stream = got.stream.get(uri);
  stream.on('error', (err) => {
    console.log(err);
    res.status(503).send();
    console.log(`[GET] Failed to get: ${uri}`);
  });
  stream.on('end', () => res.end());
  stream.pipe(res);
}

export function proxyPostRoute(req: Request, res: Response) {
  const uri = `${grabNode()}${req.originalUrl}`;
  const stream = got.stream.post(uri, {
    body: JSON.stringify(req.body),
  });
  stream.on('error', (err) => {
    console.log(err);
    res.status(503).send();
    console.log(`[POST] Failed to post: ${uri}`);
  });
  stream.on('end', () => res.end());
  stream.pipe(res);
}
