import { createHash } from "node:crypto";
import { entropyToMnemonic } from "bip39";

export function stringToBip39(input: string): string {
  const hash = createHash("sha256").update(input).digest("hex");

  return entropyToMnemonic(hash).replace(new RegExp(" ", "g"), ".");
}

export function stringToHash(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}
