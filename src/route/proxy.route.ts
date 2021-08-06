import { Request, Response } from 'express';
import got from 'got';

export function proxyRoute(req: Request, res: Response) {
  const stream = got.stream.get(`${req.session.node}${req.originalUrl}`);

  stream.on('end', () => res.end());
  stream.pipe(res);
}
