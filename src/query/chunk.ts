import got, { Response } from "got";
import * as B64js from "base64-js";
// import { TextDecoder } from "node:util";
import { b64UrlToBuffer } from "../utility/encoding";
import { forEachNode, grabNode } from "./node";
import { HTTP_TIMEOUT_SECONDS } from "../constants";

// export interface TransactionOffsetType {
//   size: number;
//   endOffset: number;
//   startOffset: number;
// }

export interface ChunkType {
  tx_path: string;
  data_path: string;
  chunk: Uint8Array;
  chunkSize: number;
}

// export const decoder = new TextDecoder();

// export async function getTransactionOffset(id: string): Promise<TransactionOffsetType> {
//   const payload = await get(`${grabNode()}/tx/${id}/offset`);
//   const body = JSON.parse(payload.text);

//   const size = parseInt(body.size);
//   const endOffset = parseInt(body.offset);
//   const startOffset = endOffset - size + 1;

//   return {
//     size,
//     endOffset,
//     startOffset,
//   };
// }

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
  // let body: any;

  const chunkResponse: ChunkResponse | void = (await got
    .get(`${mayebeMissingProtocol}${nodeGrab}/chunk/${offset}`, {
      responseType: "json",
    })
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
    const { body, statusCode } = chunkResponse;
    const chunkBuffer = Buffer.from(b64UrlToBuffer(body.chunk));
    return {
      tx_path: body.tx_path,
      data_path: body.data_path,
      chunkSize: chunkBuffer.length,
      chunk: chunkBuffer,
    };
  } else {
    return retryCount > 0 ? (await getChunk({
        offset,
        retryCount: retryCount - 1,
      })) : undefined;
  }
}

// catch {
//    return retry && retryCount > 0
//      ? getChunk({ offset, retry: true, retryCount: retryCount - 1 })
//      : undefined;
//  }
