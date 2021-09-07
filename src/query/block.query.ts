import got from "got";
import { log } from "../utility/log.utility";
import { grabNode, warmNode, coolNode } from "./node.query";
import { HTTP_TIMEOUT_SECONDS } from "../constants";

export interface BlockType {
  nonce: string;
  previous_block: string;
  timestamp: number;
  last_retarget: number;
  diff: string;
  height: number;
  hash: string;
  indep_hash: string;
  txs: Array<string>;
  tx_root: string;
  tx_tree: Array<string>;
  wallet_list: string;
  reward_addr: string;
  tags: Array<string>;
  reward_pool: number;
  weave_size: number;
  block_size: number;
  cumulative_diff: string;
  hash_list_merkle: string;
  poa: {
    option: string;
    tx_path: string;
    chunk: string;
  };
}

// get block by hash is optional (needs proper decoupling)
export async function getBlock({
  hash,
  height,
}: {
  hash?: string | undefined;
  height: number;
}): Promise<BlockType | undefined> {
  const tryNode = grabNode();
  const url = hash
    ? `${tryNode}/block/hash/${hash}`
    : `${tryNode}/block/height/${height}`;
  let body;
  try {
    body = (await got.get(url, {
      responseType: "json",
      resolveBodyOnly: true,
      timeout: HTTP_TIMEOUT_SECONDS * 1000,
      followRedirect: true,
    })) as BlockType;
  } catch (error) {
    coolNode(tryNode);
    if (error instanceof got.TimeoutError) {
      log.error(`fetching block timed out: ${url}, retry...`);
    } else if (error instanceof got.HTTPError) {
      log.error(`error while fetching block: ${url}, retry...`);
    }
  }

  if (!body) {
    return getBlock({ hash, height });
  }

  if (hash && height !== body.height) {
    console.error(height, typeof height, body.height, typeof body.height);
    log.error(
      "fatal inconsistency: hash and height dont match for hash." +
        "wanted: " +
        hash +
        " got: " +
        body.indep_hash +
        "\nwanted: " +
        height +
        " got: " +
        body.height +
        " while requesting " +
        url
    );
    // REVIEW: does assuming re-forking condition work better than fatal error?
    process.exit(1);
  }
  warmNode(tryNode);
  return body;
}

export async function fetchBlockByHash(
  hash: string
): Promise<BlockType | undefined> {
  const tryNode = grabNode();
  const url = `${tryNode}/block/hash/${hash}`;

  let body;
  try {
    body = (await got.get(url, {
      responseType: "json",
      resolveBodyOnly: true,
      timeout: HTTP_TIMEOUT_SECONDS * 1000,
      followRedirect: true,
    })) as BlockType;
  } catch {
    coolNode(tryNode);
  }

  if (!body) {
    return fetchBlockByHash(hash);
  }

  warmNode(tryNode);
  return body;
}

export async function currentBlock(): Promise<BlockType | undefined> {
  const tryNode = grabNode();
  let jsonPayload;
  try {
    jsonPayload = await got.get(`${tryNode}/block/current`, {
      responseType: "json",
      resolveBodyOnly: true,
      timeout: 15 * 1000,
    });
  } catch {
    coolNode(tryNode);
    return undefined;
  }

  warmNode(tryNode);

  return jsonPayload;
}
