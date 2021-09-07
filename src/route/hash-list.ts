import { Request, Response } from "express";
import { EventEmitter } from "node:events";
import { KEYSPACE } from "../constants.js";
import { cassandraClient } from "../database/cassandra.js";

type CassandraStream = Partial<EventEmitter & { read: () => number }>;

export async function hashListRoute(
  request: Request,
  response: Response,
  next: (error?: string) => void
): Promise<void> {
  response.writeHead(200, {
    "Content-Type": "application/json",
    "Transfer-Encoding": "chunked",
  });
  response.write("[");
  const stream: CassandraStream = cassandraClient.stream(
    `SELECT indep_hash FROM ${KEYSPACE}.block`
  ) as CassandraStream;
  stream.on("end", function streamEnd() {
    response.write("]");
    response.end();
  });
  stream.on("readable", function streamReadable() {
    let item;
    let head = true;
    while ((item = stream.read())) {
      response.write((!head ? "," : "") + JSON.stringify(item.indep_hash));
      head = false;
    }
  });
  stream.on("error", next);
}
