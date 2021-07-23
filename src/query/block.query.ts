import { get } from 'superagent';
import { grabNode, warmNode, coolNode } from './node.query';

const HTTP_TIMEOUT_SECONDS = process.env['HTTP_TIMEOUT_SECONDS']
  ? parseInt(process.env['HTTP_TIMEOUT_SECONDS'])
  : 15;

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
export function getBlock({
  hash,
  height,
  gauge,
  completed,
}: {
  hash: string | undefined;
  height: number;
  gauge?: any;
  completed?: string;
}): Promise<BlockType | void> {
  const tryNode = grabNode();
  const url = hash
    ? `${tryNode}/block/hash/${hash}`
    : `${tryNode}/block/height/${height}`;
  gauge && gauge.show(`${completed || ''} ${url}`);
  return get(url)
    .timeout(HTTP_TIMEOUT_SECONDS * 1000)
    .then((payload) => {
      const body = JSON.parse(payload.text);
      if (hash && height !== body.height) {
        // REVIEW: does assuming re-forking condition work better than fatal error?
        console.error(
          "FATAL, hash height from hash_list doesn't match up:",
          height,
          '!=',
          body.height,
          'from',
          `${tryNode}/block/hash/${hash}`
        );
        process.exit(1);
      }
      warmNode(tryNode);
      return body;
    })
    .catch(() => {
      coolNode(tryNode);
    });
}

export async function currentBlock(): Promise<BlockType | void> {
  const tryNode = grabNode();
  return get(`${tryNode}/block/current`)
    .then((payload) => {
      const body = JSON.parse(payload.text);
      return body;
    })
    .catch(() => {
      coolNode(tryNode);
    });
}
