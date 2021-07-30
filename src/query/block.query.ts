import got from 'got';
import fs from 'fs/promises';
import path from 'path';
import { log } from '../utility/log.utility.js';
import { grabNode, warmNode, coolNode } from './node.query.js';
import { HTTP_TIMEOUT_SECONDS } from '../constants.js';

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
  gauge,
  getProgress,
}: {
  hash: string | undefined;
  height: number;
  gauge?: any;
  getProgress?: () => string;
}): Promise<BlockType | undefined> {
  const tryNode = grabNode();
  const url = hash
    ? `${tryNode}/block/hash/${hash}`
    : `${tryNode}/block/height/${height}`;
  gauge && gauge.show(`${getProgress ? getProgress() || '' : ''} ${url}`);
  // const

  let body;
  try {
    body = (await got.get(url, {
      responseType: 'json',
      resolveBodyOnly: true,
      timeout: HTTP_TIMEOUT_SECONDS * 1000,
      followRedirect: true,
    })) as BlockType;
  } catch (error) {
    coolNode(tryNode);
    if (error instanceof got.TimeoutError) {
      gauge.show(`timeout: ${url}`);
    } else if (error instanceof got.HTTPError) {
      gauge.show(`error'd: ${url}`);
    }
  }

  if (!body) {
    return getBlock({ hash, height, gauge, getProgress });
  }

  if (hash && height !== body.height) {
    log.error(
      'fatal inconsistency: hash and height dont match for hash:' +
        'wanted: ' +
        hash +
        ' got: ' +
        body.indep_hash +
        '\nwanted: ' +
        height +
        ' got: ' +
        body.height
    );
    // REVIEW: does assuming re-forking condition work better than fatal error?
    process.exit(1);
  }
  warmNode(tryNode);
  return body;
}

export async function currentBlock(): Promise<BlockType | undefined> {
  const tryNode = grabNode();
  let jsonPayload;
  try {
    jsonPayload = await got.get(`${tryNode}/block/current`, {
      responseType: 'json',
      resolveBodyOnly: true,
      timeout: 15 * 1000,
    });
  } catch (error) {
    coolNode(tryNode);
    return undefined;
  }

  warmNode(tryNode);

  return jsonPayload;
}
