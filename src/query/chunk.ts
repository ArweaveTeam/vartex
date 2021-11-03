import got, { Response } from "got";
import { b64UrlToBuffer } from "../utility/encoding";
import { forEachNode } from "./node";

export interface ChunkType {
  tx_path: string;
  data_path: string;
  chunk: Uint8Array;
  chunkSize: number;
}

interface ChunkResponse extends Response {
  body: {
    tx_path: string;
    packing: string;
    data_path: string;
    chunk: string;
  };
  statusCode: number;
}

export async function getChunk({
  offset,
  retryCount = 5,
}: {
  offset: string;
  retryCount?: number;
}): Promise<ChunkType | undefined> {
  const nodeGrab = forEachNode(retryCount);
  const mayebeMissingProtocol = nodeGrab.startsWith("http") ? "" : "http://";

  const chunkResponse: ChunkResponse | void = (await got
    .get(`${mayebeMissingProtocol}${nodeGrab}/chunk/${offset}`, {
      responseType: "json",
    })
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    .catch((error: any) => {
      console.error(
        `${mayebeMissingProtocol}${nodeGrab}/chunk/${offset}`,
        error.message
      );
    })) as ChunkResponse;

  if (
    chunkResponse &&
    typeof chunkResponse === "object" &&
    chunkResponse.statusCode >= 200 &&
    chunkResponse.statusCode < 300
  ) {
    const { body } = chunkResponse;
    const chunkBuffer = Buffer.from(b64UrlToBuffer(body.chunk));
    return {
      tx_path: body.tx_path,
      data_path: body.data_path,
      chunkSize: chunkBuffer.length,
      chunk: chunkBuffer,
    };
  } else {
    return retryCount > 0
      ? await getChunk({
          offset,
          retryCount: retryCount - 1,
        })
      : undefined;
  }
}
