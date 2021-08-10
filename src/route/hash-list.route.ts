import * as R from "rambda";
import { Request, Response } from "express";
import { concurrent, types as CassandraTypes } from "cassandra-driver";
import { KEYSPACE } from "../constants.js";
import { cassandraClient } from "../database/cassandra.database.js";

export async function hashListRoute(
  request: Request,
  response: Response,
  next: (error?: string) => void
) {
  response.writeHead(200, {
    "Content-Type": "application/json",
    "Transfer-Encoding": "chunked",
  });
  response.write("[");
  const stream = cassandraClient.stream(
    `SELECT indep_hash FROM ${KEYSPACE}.block`
  );
  stream.on("end", function streamEnd() {
    response.write("]");
    response.end();
  });
  stream.on("readable", function streamReadable() {
    let item;
    let head = true;
    while ((item = (stream as any).read())) {
      response.write((!head ? "," : "") + JSON.stringify(item.indep_hash));
      head = false;
    }
  });
  stream.on("error", next);
}
