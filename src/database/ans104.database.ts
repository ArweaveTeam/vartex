import { getTxOffset, TransactionType } from "../query/transaction.query";
import base64url from "base64url";
import { getChunk } from "../query/chunk.query";
import { pipeline } from "stream/promises";
import { tmpName } from "tmp-promise";
import { getHeaders, getOwner, getTags, numberOfItems } from "ans104/file";
import * as fs from "fs";
import { DataItem } from "ans104/lib";
import { promisify } from "util";

export function isAns104(tx: TransactionType): boolean {
    for (const tag of tx.tags) {
        if (tag.name === "Bundle-Type" && tag.value === "binary") {
            return true;
        }
    }
    return false;
}

export async function processAns104Transaction(txId: string): Promise<void> {
    const txOffset = await getTxOffset({ txId, retry: 10 });

    const filename = await tmpName();
    const stream = fs.createWriteStream(filename);
    await pipeline(
        getTxData(txId, txOffset),
        stream
    );
    stream.close();

    await indexAns104File(filename);
}

const read = promisify(fs.read);
export async function indexAns104File(filename: string): Promise<void> {
    const count = await numberOfItems(filename);
    let cumulativeOffset = 32 + (64 * count);
    const fd = await fs.promises.open(filename, "r").then(r => r.fd);
    for await (const { offset, id } of getHeaders(filename)) {
        if (await alreadyIndexed(id)) continue;

        const item = {
            owner: getOwner(filename, { offset: cumulativeOffset }),
            tags: getTags(filename)
        }
        cumulativeOffset += offset;
    }
}

async function* getTxData(txId: string, offsets: { offset: number, size: number }): AsyncGenerator<Buffer> {
    if (!offsets) {
        offsets = await getTxOffset({ txId });
    }
    const { offset, size } = offsets;

    let bytesReceived = 0;
    let cumulativeOffset = offset;
    while (bytesReceived < size) {
        const chunk = await getChunk({ offset: cumulativeOffset.toString(), retryCount: 10 })
            .then(r => Buffer.from(base64url.decode(r.chunk, "hex"), "hex"));
        cumulativeOffset += chunk.byteLength;
        bytesReceived += chunk.byteLength;
        yield chunk;
    }
}

async function alreadyIndexed(txId: string): Promise<boolean> {
    return false;
}
