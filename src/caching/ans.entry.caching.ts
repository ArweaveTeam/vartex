import fs from "fs-jetpack";
import { DataItemJson } from "arweave-bundles";
import { cacheFolder } from "./file.caching";
import { b64UrlToBuffer } from "../utility/encoding.utility";

export async function cacheANSEntries(entries: Array<DataItemJson>) {
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    const id = entry.id;
    const data = entry.data;

    const bufferData = Buffer.from(b64UrlToBuffer(data));

    if (fs.exists(`${cacheFolder}/${id}`) === false) {
      fs.write(`${cacheFolder}/${id}`, bufferData.toString("utf-8"));
    }
  }
}
