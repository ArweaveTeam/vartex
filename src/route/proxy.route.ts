import { Request, Response } from 'express';
import got from 'got';

export function proxyGetRoute(req: Request, res: Response) {
  const stream = got.stream.get(`${req.session.node}${req.originalUrl}`);

  stream.on('end', () => res.end());
  stream.pipe(res);
}

export function proxyPostRoute(req: Request, res: Response) {
  const stream = got.stream.post(`${req.session.node}${req.originalUrl}`, {
    body: JSON.stringify(req.body),
  });

  stream.on('end', () => res.end());
  stream.pipe(res);
}
