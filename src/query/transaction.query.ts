import superagent from 'superagent';
import { TagFilter } from '../graphql/types.js';
import {
  Base64UrlEncodedString,
  WinstonString,
  fromB64Url,
} from '../utility/encoding.utility.js';
import { grabNode, coolNode, warmNode } from './node.query.js';
import { HTTP_TIMEOUT_SECONDS } from '../constants.js';

export interface Tag {
  name: Base64UrlEncodedString;
  value: Base64UrlEncodedString;
}

export interface TransactionType {
  format: number;
  id: string;
  height?: number;
  last_tx: string;
  owner: string;
  tags: Array<Tag>;
  target: string;
  quantity: WinstonString;
  data: Base64UrlEncodedString;
  data_size: string;
  data_tree: Array<string>;
  data_root: string;
  reward: string;
  signature: string;
}

export function getTransaction({
  txId,
  retry = 0,
}: {
  txId: string;
  retry?: number;
}): Promise<TransactionType | undefined> {
  const tryNode = grabNode();

  return superagent
    .get(`${tryNode}/tx/${txId}`)
    .timeout(HTTP_TIMEOUT_SECONDS * 4 * 1000)
    .then((payload) => {
      const body = JSON.parse(payload.text);
      warmNode(tryNode);
      return body;
    })
    .catch((error) => {
      coolNode(tryNode);
      return new Promise((res) => setTimeout(res, 10 + 2 * retry)).then(() => {
        if (retry < 100) {
          return getTransaction({ txId, retry: retry + 1 });
        } else {
          console.error(
            'Failed to establish connection to any specified node after 100 retries'
          );
          process.exit(1);
        }
      });
    });
}

export function getTxOffset({
  txId,
  retry = 0,
}: {
  txId: string;
  retry?: number;
}): Promise<TransactionType | undefined> {
  const tryNode = grabNode();

  return superagent
    .get(`${tryNode}/tx/${txId}/offset`)
    .timeout(HTTP_TIMEOUT_SECONDS * 4 * 1000)
    .then((offsetPayload) => {
      const body = JSON.parse(offsetPayload.text);
      warmNode(tryNode);
      return body;
    })
    .catch((error) => {
      coolNode(tryNode);
      return new Promise((res) => setTimeout(res, 10 + 2 * retry)).then(() => {
        if (retry < 100) {
          return getTransaction({ txId, retry: retry + 1 });
        } else {
          console.error(
            'Failed to establish connection to any specified node after 100 retries'
          );
          process.exit(1);
        }
      });
    });
}

export function toB64url(input: string): Base64UrlEncodedString {
  return Buffer.from(input)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');
}

export function tagValue(tags: Array<Tag>, name: string): string {
  for (let i = 0; i < tags.length; i++) {
    const tag = tags[i];
    if (fromB64Url(tag.name).toString().toLowerCase() === name.toLowerCase()) {
      return fromB64Url(tag.value).toString();
    }
  }

  return '';
}

export function tagToUTF8(tags: Array<Tag>): Array<Tag> {
  const conversion: Array<Tag> = [];

  for (let i = 0; i < tags.length; i++) {
    const tag = tags[i];
    conversion.push({
      name: fromB64Url(tag.name).toString(),
      value: fromB64Url(tag.value).toString(),
    });
  }

  return conversion;
}

export function tagToB64(tags: Array<TagFilter>): Array<TagFilter> {
  const conversion: Array<TagFilter> = [];

  for (let i = 0; i < tags.length; i++) {
    const tag = tags[i];
    conversion.push({
      name: toB64url(tag.name),
      values: tag.values.map((v) => toB64url(v)),
    });
  }

  return conversion;
}
