import { Request, Response } from 'express';
import proxy from 'express-http-proxy';

export function proxyRoute(req: Request, res: Response) {
  console.log(req, `${req.session.node}${req.originalUrl}`, {
    https: true,
    preserveHostHdr: true,
  });
  return proxy(`${req.session.node}${req.originalUrl}`, {
    // https: true,
    // preserveHostHdr: true,
    reqAsBuffer: true,
  });
}
