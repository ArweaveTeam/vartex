import got from "got";
import { b64UrlToBuffer } from "../utility/encoding";
import { grabNode } from "./node";
import { HTTP_TIMEOUT_SECONDS } from "../constants";

// export interface TransactionOffsetType {
//   size: number;
//   endOffset: number;
//   startOffset: number;
// }

export interface ChunkType {
  tx_path: string;
  data_path: string;
  chunk: string;
  parsed_chunk: Uint8Array;
  response_chunk: Buffer;
}

export const decoder = new TextDecoder();

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
  retry = true,
  retryCount = 5,
}: {
  offset: string;
  retry?: boolean;
  retryCount?: number;
}): Promise<ChunkType> {
  try {
    const body: ChunkType = await got.get(`${grabNode()}/chunk/${offset}`, {
      responseType: "json",
      resolveBodyOnly: true,
      timeout: HTTP_TIMEOUT_SECONDS * 1000,
      followRedirect: true,
    });

    const parsed_chunk = b64UrlToBuffer(body.chunk);
    const response_chunk = Buffer.from(parsed_chunk);

    return {
      tx_path: body.tx_path,
      data_path: body.data_path,
      chunk: body.chunk,
      parsed_chunk,
      response_chunk,
    };
  } catch (error) {
    if (retry && retryCount > 0) {
      return getChunk({ offset, retry: true, retryCount: retryCount - 1 });
    } else {
      throw error;
    }
  }
}
