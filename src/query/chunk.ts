import got from "got";
import * as B64js from "base64-js";
// import { TextDecoder } from "node:util";
// import { b64UrlToBuffer } from "../utility/encoding";
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

export async function getChunk({
  offset,
  retryCount = 5,
}: {
  offset: string;
  retryCount?: number;
}): Promise<ChunkType | undefined> {
  const nodeGrab = forEachNode(retryCount);
  const mayebeMissingProtocol = nodeGrab.startsWith("http") ? "" : "http://";
  let body: any;

  body = await got
    .get(`${mayebeMissingProtocol}${nodeGrab}/chunk/${offset}`, {
      responseType: "json",
    })
    .catch((error: any) => {
      console.error(
        `${mayebeMissingProtocol}${nodeGrab}/chunk/${offset}`,
        error.message
      );
      body = undefined;
    });

  if (body && typeof body === "object") {
    return {
      tx_path: body.tx_path,
      data_path: body.data_path,
      chunk: B64js.toByteArray(body.chunk),
    };
  } else {
    if (retryCount > 0) {
      return await getChunk({
        offset,
        retryCount: retryCount - 1,
      });
    } else {
      return undefined;
    }
  }
}

// catch {
//    return retry && retryCount > 0
//      ? getChunk({ offset, retry: true, retryCount: retryCount - 1 })
//      : undefined;
//  }
