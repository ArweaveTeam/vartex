import got from 'got';
import {TagFilter} from '../graphql/types';
import {
  Base64UrlEncodedString,
  WinstonString,
  fromB64Url,
} from '../utility/encoding.utility';
import {grabNode, coolNode, warmNode} from './node.query';
import {HTTP_TIMEOUT_SECONDS} from '../constants';

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

export async function getTransaction({
  txId,
  retry = 0,
}: {
  txId: string;
  retry?: number;
}): Promise<TransactionType | undefined> {
  const tryNode = grabNode();
  let jsonPayload;
  try {
    jsonPayload = await got.get(`${tryNode}/tx/${txId}`, {
      responseType: 'json',
      resolveBodyOnly: true,
    });
  } catch (error) {
    coolNode(tryNode);
    return new Promise((res) => setTimeout(res, 10 + 2 * retry)).then(() => {
      if (retry < 100) {
        return getTransaction({txId, retry: retry + 1});
      } else {
        console.error(
            'Failed to establish connection to any specified node after 100 retries',
        );
        process.exit(1);
      }
    });
  }
  warmNode(tryNode);
  return jsonPayload;
}

export async function getTxOffset({
  txId,
  retry = 0,
}: {
  txId: string;
  retry?: number;
}): Promise<TransactionType | undefined> {
  const tryNode = grabNode();
  let jsonPayload;
  try {
    jsonPayload = await got.get(`${tryNode}/tx/${txId}/offset`, {
      responseType: 'json',
      resolveBodyOnly: true,
    });
  } catch (error) {
    coolNode(tryNode);
    return new Promise((res) => setTimeout(res, 10 + 2 * retry)).then(() => {
      if (retry < 100) {
        return getTransaction({txId, retry: retry + 1});
      } else {
        console.error(
            'Failed to establish connection to any specified node after 100 retries',
        );
        process.exit(1);
      }
    });
    warmNode(tryNode);
    return jsonPayload;
  }
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
