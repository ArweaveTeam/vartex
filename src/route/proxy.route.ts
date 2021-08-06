import { Request, Response } from 'express';
import { grabNode } from '../query/node.query.js';
import got from 'got';

export function proxyGetRoute(req: Request, res: Response) {
  const stream = got.stream.get(`${grabNode()}${req.originalUrl}`);
  stream.on('error', (err) => {
    res.status(404).json({
      status: 404,
      error: 'Not Found',
    });
  });
  stream.on('end', () => res.end());
  stream.pipe(res);
}

export function proxyPostRoute(req: Request, res: Response) {
  const stream = got.stream.post(`${grabNode()}${req.originalUrl}`, {
    body: JSON.stringify(req.body),
  });

  stream.on('end', () => res.end());
  stream.pipe(res);
}
