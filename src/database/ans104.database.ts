import { getTxOffset, TransactionType } from "../query/transaction.query";
import got from "got";
import { grabNode } from "../query/node.query";
import { byteArrayToLong } from "ans104/lib/utils";

export function isAns104(tx: TransactionType): boolean {
    for (const tag of tx.tags) {
        if (tag.name === "Bundle-Type" && tag.value === "binary") {
            return true;
        }
    }
    return false;
}

export async function processAns104Transaction(tx: Omit<TransactionType, "data">): Promise<void> {
    const itemCount = await getAns104DataItemCount(tx.id);
    const { size, offset } = await getTxOffset({ txId: tx.id, retry: 10 });

    for await (const header of getAns104Headers(itemCount)) {

    }
}

export async function getAns104DataItemCount(txId: string):Promise<number> {
    const response = await got.get(`${grabNode()}/data/${txId}/chunk`);
    return byteArrayToLong(response.rawBody.slice(0, 32));
}

export async function* getAns104Headers(itemCount: number): AsyncGenerator<Buffer> {

    yield
}
