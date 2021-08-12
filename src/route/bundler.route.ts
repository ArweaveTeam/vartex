import { Request, Response } from "express";
import { deepHash } from "ans104";
import { stringToBuffer } from "arweave/node/lib/utils";
import base64url from "base64url";
import Arweave from "arweave";
import { tmpName } from "tmp-promise";
import * as fs from "node:fs";
import { pipeline } from "node:stream/promises";

/**
 * Uploads the data item to the gateways cache
 * @param request
 * @param response
 */
export async function newDataItem(
    request: Request,
    response: Response
): Promise<void> {
    const stream = fs.createWriteStream("SOME_CACHE/" + request.header("bnd.data.id"));
    await pipeline(
        request,
        stream
    );

    response
        .status(200);
}

/**
 * Verifies the sender is a bundler
 *
 * @param request
 * @param response
 * @param next
 */
export async function verifySender(
    request: Request,
    response: Response,
    next: (error?: string) => Promise<void>
): Promise<void> {
    // Make this unique each time?
    const id = request.header("bnd.data.id");
    const block = +request.header("bnd.data.blockBy");
    const signature = Buffer.from(base64url.decode(request.header("bnd.account.signature"), "hex"), "hex");
    const expected = await deepHash([
        stringToBuffer("1"),
        stringToBuffer("bundler"),
        stringToBuffer(id),
        stringToBuffer(block.toString())
    ]);
    const n = await getPublic();
    const valid = await Arweave.crypto.verify(n, expected, signature);
    if (!valid) {
        response
            .status(400)
            .send({
                error: "Invalid signature"
            })
            .end();
    }

    await next()
}


async function getPublic(): Promise<string> {
    return "";
}
