import got from "got";
import { lookup as mimeLookup } from "mime-types";
import { head, last, prop } from "rambda";
import { Request, Response } from "express";
import { Duplex, PassThrough, Transform } from "stream";
import StreamChain from "stream-chain";
import StreamJson from "stream-json";
import StreamJsonPick from "stream-json/filters/Pick";
import StreamJsonValues from "stream-json/streamers/StreamValues";
import { transactionMapper, txOffsetMapper } from "../database/mapper";
import {
  TransactionType,
  getTransaction,
  getTxOffset,
} from "../query/transaction";
import { grabNode } from "../query/node";
import { utf8DecodeTag, utf8DecodeTupleTag } from "../utility/encoding";

class B64Transform extends Transform {
  protected extra: string;
  protected iterLength: number;

  constructor(startOffset: number) {
    super();
    this.extra = "";
    this.iterLength = startOffset;
  }

  _transform(chunk: string, encoding: string, cb: any) {
    // ensure string
    chunk = "" + chunk;

    // Add previous extra and remove any newline characters
    chunk = this.extra + chunk.replace(/(\r\n|\n|\r)/gm, "");

    // 4 characters represent 3 bytes, so we can only decode in groups of 4 chars
    const remaining = chunk.length % 4;

    // Store the extra chars for later
    this.extra = chunk.slice(chunk.length - remaining);
    chunk = chunk.slice(0, chunk.length - remaining);

    // Create the new buffer and push
    const buf = Buffer.from(chunk, "base64");
    this.iterLength += buf.length;
    this.push(buf);
    cb();
  }

  _flush(cb: any) {
    if (this.extra.length) {
      this.push(Buffer.from(this.extra, "base64"));
    }

    cb();
  }
}

function recurNextChunk(
  response: Response,
  pipeline: any,
  endOffset: number,
  nextOffset: number,
  retry = 0
) {
  const passThru = new PassThrough();

  const chunkStream = got.stream(`${grabNode()}/chunk/${nextOffset}`, {
    followRedirect: true,
  });

  let hasError = false;
  let pipeStarted = false;

  chunkStream.on("error", () => {
    hasError = true;
    if (retry < 4) {
      return recurNextChunk(
        response,
        pipeline,
        endOffset,
        nextOffset,
        retry + 1
      );
    } else {
      response.end();
      passThru.unpipe(pipeline);
      chunkStream.unpipe(passThru);
      chunkStream.destroy();
    }
  });

  chunkStream.on("downloadProgress", () => {
    if (!pipeStarted && !hasError) {
      pipeStarted = true;
      chunkStream.pipe(passThru).pipe(pipeline);
      // pipeline.resume();
    }
  });

  chunkStream.on("end", () => {
    if (nextOffset < endOffset) {
      chunkStream.unpipe(passThru);
      passThru.unpipe(pipeline);

      // maybe a bug in the library itself, but it stays otherwise
      // stuck in "done" state, here we restart the json parser
      head<any>(pipeline.streams)._expect = "value";

      return recurNextChunk(
        response,
        pipeline,
        endOffset,
        last<any>(pipeline.streams).iterLength,
        0
      );
    } else {
      head<any>(pipeline.streams)._expect = "done";
      pipeline.on("end", response.end.bind(response));
      pipeline.end();
    }
  });
}

export async function dataRoute(
  request: Request,
  response: Response,
  next: (error?: string) => void
): Promise<void> {
  const path = request.params["0"];
  if (!path) {
    response.sendStatus(404);
    return;
  }

  const [txId, ...subpaths] = path.split("/");
  const txDb = await transactionMapper.get({ tx_id: txId });
  let txUpstream: TransactionType | undefined;

  if (!txDb) {
    try {
      txUpstream = await getTransaction({ txId, retry: 2 });
    } catch {
      console.error(`tx ${txId} wasn't found`);
      response.sendStatus(404);
      return;
    }
  }

  let offset = await txOffsetMapper.get({ tx_id: txId });

  if (!offset) {
    offset = await getTxOffset({ txId });
  }

  if (offset) {
    const tags = txUpstream
      ? txUpstream.tags.map(utf8DecodeTag)
      : txDb.tags.map(utf8DecodeTupleTag);

    let contentType: string;
    let filename: string;

    for (const tag of tags as { name: string; value: string }[]) {
      if (tag.name.toLowerCase() === "content-type") {
        contentType = tag.value;
      }
      if (!contentType && tag.name.toLowerCase() === "filename") {
        contentType = mimeLookup(tag.value) || undefined;
      }
    }

    const size = parseInt(offset.size);
    const endOffset = parseInt(offset.offset);
    const startOffset = endOffset - size + 1;

    response.set({
      "Content-Type": contentType || "text/plain",
      "Content-Length": size,
    });

    const b64Transform = new B64Transform(startOffset);
    const streamJsonParser = StreamJson.parser();

    const pipeline = StreamChain.chain([
      streamJsonParser,
      StreamJsonPick.pick({ filter: "chunk" }),
      StreamJsonValues.streamValues(),
      prop("value"),
      b64Transform,
    ]);

    pipeline.pipe(response);
    response.on("error", () => {
      response.end();
    });
    pipeline.on("error", () => {
      response.end();
    });

    recurNextChunk(response, pipeline, endOffset, startOffset);
  } else {
    console.error(`offset for ${txId} wasn't found`);
    response.sendStatus(404);
  }
}
