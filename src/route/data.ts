import got from "got";
import { Request, Response } from "express";
import { PassThrough, Transform } from "stream";
import StreamChain from "stream-chain";
import StreamJson from "stream-json";
import StreamJsonPick from "stream-json/filters/Pick";
import StreamJsonValues from "stream-json/streamers/StreamValues";
import { txOffsetMapper } from "../database/mapper";
import { getTxOffset } from "../query/transaction";
import { grabNode } from "../query/node";

class b64Transform extends Transform {
  protected extra: string;

  constructor() {
    super();
    this.extra = "";
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

export async function dataRoute(
  request: Request,
  response: Response,
  next: (error?: string) => void
): Promise<void> {
  const path = request.url.replace(/^\//, "").replace(/\/$/, "");
  const [txId, ...subpaths] = path.split("/");
  let offset = await txOffsetMapper.get({ tx_id: txId });

  if (!offset) {
    offset = await getTxOffset({ txId });
  }
  const size = parseInt(offset.size);
  const startOffset = parseInt(offset.offset);

  let byteCnt = 0;
  while (byteCnt < size) {
    console.log("trying ", `${grabNode()}/chunk/${startOffset + byteCnt}`);
    const chunkStream = got.stream(
      `${grabNode()}/chunk/${startOffset + byteCnt}`
    );
    console.log(Object.keys(chunkStream));
    const passThru = new PassThrough();
    const pipeline = StreamChain.chain([
      StreamJson.parser(),
      StreamJsonPick.pick({ filter: "chunk" }),
      StreamJsonValues.streamValues(),
      // response.write,
    ]);
    // response.pipe(chunkStream.pipe(pipeline));
    // pipeline.on("data", () => response.write);
    pipeline.on("end", () => {
      chunkStream.end();
      response.end();
    });
    // response.pipe(pipeline);
    // pipeline.pipe(response);
    chunkStream.pipe(passThru).pipe(pipeline).pipe(response);

    break;
  }
  // console.log("REQ", Object.keys(request), request.url);
  // console.log(txId, subpaths);
}
