import * as R from 'rambda';
import {Request, Response} from 'express';
import {concurrent, types as CassandraTypes} from 'cassandra-driver';
import {KEYSPACE} from '../constants.js';
import {cassandraClient} from '../database/cassandra.database.js';

export async function hashListRoute(
    req: Request,
    res: Response,
    next: (err?: string) => void,
) {
  res.writeHead(200, {
    'Content-Type': 'application/json',
    'Transfer-Encoding': 'chunked',
  });
  res.write('[');
  const stream = cassandraClient.stream(
      `SELECT indep_hash FROM ${KEYSPACE}.block`,
  );
  stream.on('end', function streamEnd() {
    res.write(']');
    res.end();
  });
  stream.on('readable', function streamReadable() {
    let item;
    let head = true;
    while ((item = (stream as any).read())) {
      res.write((!head ? ',' : '') + JSON.stringify(item.indep_hash));
      head = false;
    }
  });
  stream.on('error', next);
}
