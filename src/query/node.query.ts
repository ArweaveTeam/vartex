import {existsSync} from 'fs';
import fs from 'fs/promises';
import {types as CassandraTypes} from 'cassandra-driver';
import * as R from 'rambda';
import rwc from 'random-weighted-choice';
import got from 'got';
import {log} from '../utility/log.utility';
import {getChunk} from './chunk.query';
import {HTTP_TIMEOUT_SECONDS} from '../constants';

export const NODES = process.env.ARWEAVE_NODES ?
  JSON.parse(process.env.ARWEAVE_NODES) :
  ['http://lon-2.eu-west-1.arweave.net:1984'];

type WeightedNode = { id: string; weight: number };

let nodeTemperatures: WeightedNode[] =
  process.env.NODE_ENV === 'test' ?
    [] :
    [
      {id: 'https://arweave.net', weight: 2},
      {id: 'http://lon-2.eu-west-1.arweave.net:1984', weight: 2},
    ];

const syncNodeTemperatures = () => {
  nodeTemperatures = NODES.map((url: string) => {
    const previousWeight = nodeTemperatures.find(
        (i: WeightedNode) => i.id === url,
    );
    return {
      id: url,
      weight: previousWeight ? previousWeight.weight : 1,
    };
  });
};

export const findPeers = async () => {
  // await superagent.get('https://www.arweave.net/peers').then((payload) => {
  //   const rootPeers = JSON.parse(payload.text);
  //   rootPeers.forEach(
  //     (peer: string) =>
  //       peer &&
  //       !peer.startsWith('127.0') &&
  //       !NODES.includes(peer) &&
  //       NODES.push(peer)
  //   );
  //   syncNodeTemperatures();
  // });
};

export function grabNode() {
  R.isEmpty(nodeTemperatures) && syncNodeTemperatures();
  const randomWeightedNode = rwc(nodeTemperatures);
  if (!randomWeightedNode) {
    if (process.env.NODE_ENV === 'test') {
      return nodeTemperatures[0].id;
    }
    throw new Error('No more peers were found');
  }
  return randomWeightedNode.startsWith('http') ?
    randomWeightedNode :
    `http://${randomWeightedNode}`;
}

export function warmNode(url: string) {
  const item = nodeTemperatures.find((i: WeightedNode) => i.id === url);
  if (item) {
    item['weight'] = Math.max(item['weight'] + 1, 99);
  }
}

export function coolNode(url: string, kickIfLow: boolean = false) {
  const item = nodeTemperatures.find((i: WeightedNode) => i.id === url);
  if (item) {
    if (kickIfLow && item['weight'] < 2) {
      log.info(`[network] peer ${url} kicked out because of unresponsiveness`);
      nodeTemperatures = R.reject((temp: WeightedNode) =>
        R.equals(R.prop('id', temp), url),
      )(nodeTemperatures) as WeightedNode[];
    }
    item['weight'] = Math.min(item['weight'] - 1, 1);
  }
}

export interface InfoType {
  network: string;
  version: number;
  release: number;
  height: number;
  current: string;
  blocks: number;
  peers: number;
  queue_length: number;
  node_state_latency: number;
}

export async function getNodeInfo({
  retry = 0,
  maxRetry = 100,
  keepAlive = false,
}): Promise<InfoType | undefined> {
  const tryNode = grabNode();

  try {
    const body: any = await got.get(`${tryNode}/info`, {
      responseType: 'json',
      resolveBodyOnly: true,
      timeout: HTTP_TIMEOUT_SECONDS * 1000,
      followRedirect: true,
    });

    warmNode(tryNode);

    return {
      network: body.network,
      version: body.version,
      release: body.release,
      height: body.height,
      current: body.current,
      blocks: body.blocks,
      peers: body.peers,
      queue_length: body.queue_length,
      node_state_latency: body.node_state_latency,
    };
  } catch (error) {
    coolNode(tryNode, true);
    return new Promise((res) => setTimeout(res, 10 + 2 * retry)).then(
        async () => {
          if (retry < maxRetry) {
            return await getNodeInfo({retry: retry + 1, maxRetry});
          } else {
            console.trace(
                '\n' +
              'Failed to establish connection to any specified node after 100 retries with these nodes: ' +
              nodeTemperatures.map(R.prop('id')).join(', ') +
              '\n',
            );

            if (keepAlive) {
              console.error(
                  '\n' +
                'Check the network status, trying again to reach some of these nodes, but it is unlikely to make a differnece:' +
                nodeTemperatures.map(R.prop('id')).join(', ') +
                '\n',
              );
              return await getNodeInfo({retry: 0, maxRetry});
            } else {
              return undefined;
            }
          }
        },
    );
  }
}

export async function getHashList({
  retry = 0,
}): Promise<string[] | undefined> {
  const hashListCachePath =
    process.env.NODE_ENV === 'test' ?
      'cache/hash_list_test.json' :
      'cache/hash_list.json';
  const cacheExists = existsSync(hashListCachePath);

  if (cacheExists) {
    log.info('[database] using hash_list from cache');
    return fs.readFile(hashListCachePath).then((hashListBuf) => {
      return JSON.parse(hashListBuf.toString());
    });
  } else {
    const tryNode = grabNode();
    const url = `${tryNode}/hash_list`;
    log.info('[database] fetching the hash_list, this may take a while...');

    try {
      const body = await got.get(url, {
        responseType: 'json',
        resolveBodyOnly: true,
        followRedirect: true,
      });

      const linearHashList = R.reverse(body as any);
      return fs
          .writeFile(
              hashListCachePath,
              JSON.stringify(linearHashList, undefined, 2),
          )
          .then(() => linearHashList as string[]);
    } catch (error) {
      process.env.NODE_ENV === 'test' && console.error(error);
      coolNode(tryNode);
      return new Promise((res) => setTimeout(res, 10 + 2 * retry)).then(
          async () => {
            if (retry < 100) {
              return await getHashList({retry: retry + 1});
            } else {
              console.trace(
                  'Failed to establish connection to any specified node after 100 retries',
              );
              process.exit(1);
            }
          },
      );
    }
  }
}

export async function getData(id: string): Promise<any> {
  return await got.get(`${grabNode()}/${id}`);
}

export async function getDataFromChunks({
  id,
  startOffset,
  endOffset,
  retry,
}: {
  id: string;
  retry?: boolean;
  startOffset: CassandraTypes.Long;
  endOffset: CassandraTypes.Long;
}): Promise<Buffer> {
  try {
    let byte = 0;
    let chunks = Buffer.from('');

    while (startOffset.add(byte).lt(endOffset)) {
      const chunk = await getChunk({
        offset: startOffset.add(byte).toString(),
      });
      byte += chunk.parsed_chunk.length;
      chunks = Buffer.concat([chunks, chunk.response_chunk]);
    }

    return chunks;
  } catch (error) {
    if (retry) {
      console.error(
          `error retrieving data from ${id}, please note that this may be a cancelled transaction`
              .red.bold,
      );
      return await getDataFromChunks({
        id,
        retry: false,
        startOffset,
        endOffset,
      });
    } else {
      throw error;
    }
  }
}
