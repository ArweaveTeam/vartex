import { Base64UrlEncodedString, fromB64Url, WinstonString } from "./encoding";
import AbortController from "abort-controller";
import fetch, {
  Headers as FetchHeaders,
  RequestInit as FetchRequestInit,
} from "node-fetch";
import { shuffle } from "lodash";
import log from "../lib/log";
import { NotFound } from "http-errors";

export type TransactionHeader = Omit<Transaction, "data">;

export type TransactionData = {
  data: Buffer;
  contentType: string | undefined;
};

export interface Transaction {
  format: number;
  id: string;
  signature: string;
  owner: string;
  target: string;
  data: Base64UrlEncodedString;
  reward: WinstonString;
  last_tx: string;
  tags: Tag[];
  quantity: WinstonString;
  data_size: number;
  data_root: string;
  data_tree: string[];
}

export interface Chunk {
  data_root: string;
  data_size: number;
  data_path: string;
  chunk: string;
  offset: number;
}

export type ChunkHeader = Omit<Chunk, "chunk">;

export interface Tag {
  name: Base64UrlEncodedString;
  value: Base64UrlEncodedString;
}

export interface Block {
  nonce: string;
  previous_block: string;
  timestamp: number;
  last_retarget: number;
  diff: string;
  height: number;
  hash: string;
  indep_hash: string;
  txs: string[];
  tx_root: string;
  wallet_list: string;
  reward_addr: string;
  reward_pool: number;
  weave_size: number;
  block_size: number;
  cumulative_diff: string;
  hash_list_merkle: string;
}

export interface DataResponse {
  data: Buffer;
  contentType: string | undefined;
}

export const origins = JSON.parse(
  process.env.ARWEAVE_NODES || "null"
) as string[];

export const fetchBlock = async (id: string): Promise<Block> => {
  const endpoints = origins.map((host) => `${host}/block/hash/${id}`);

  const { body } = await getFirstResponse(
    endpoints,
    ({ status }) => status == 200
  );

  if (body) {
    const block = JSON.parse(body.toString());

    //For now we don't care about the poa and it's takes up too much
    // space when logged, so just remove it for now.
    //@ts-ignore
    delete block.poa;

    return block as Block;
  }

  throw new Error(`Failed to fetch block: ${id}`);
};

export const fetchBlockByHeight = async (height: string): Promise<Block> => {
  log.info(`[arweave] fetching block by height`, { height });

  const endpoints = origins.map((host) => `${host}/block/height/${height}`);

  const { body } = await getFirstResponse(
    endpoints,
    ({ status }) => status == 200
  );

  if (body) {
    const block = JSON.parse(body.toString());

    //For now we don't care about the poa and it's takes up too much
    // space when logged, so just remove it for now.
    //@ts-ignore
    delete block.poa;

    return block as Block;
  }

  throw new Error(`Failed to fetch block: ${height}`);
};

export const fetchTransactionHeader = async (
  txid: string
): Promise<TransactionHeader> => {
  log.info(`[arweave] fetching transaction header`, { txid });
  const endpoints = origins.map((host) => `${host}/tx/${txid}`);

  const { body } = await getFirstResponse(
    endpoints,
    ({ status }) => status == 200
  );

  if (body) {
    return JSON.parse(body.toString()) as TransactionHeader;
  }

  throw new NotFound();
};

export const fetchTransactionData = async (
  txid: string
): Promise<DataResponse> => {
  log.info(`[arweave] fetching data and tags`, { txid });

  try {
    const [data, contentType] = await Promise.all([
      fetchRequest(`/tx/${txid}/data`, ({ status }) => status == 200).then(
        (response) => {
          return response && response.body
            ? fromB64Url(response.body!.toString("utf8"))
            : undefined;
        }
      ),
      fetchRequest(`/tx/${txid}/tags`, ({ status }) => status == 200).then(
        (response) => {
          const tags =
            response && response.body
              ? (JSON.parse(response.body!.toString("utf8")) as Tag[])
              : [];
          return getTagValue(tags, "content-type");
        }
      ),
    ]);
    if (data) {
      log.info(`[arweave] found tx`, { txid, type: contentType });
      return {
        contentType,
        data,
      };
    } else {
      log.info(`[arweave] failed to find tx`, { txid });
    }
  } catch (error) {
    log.error(`[arweave] error finding tx`, { txid, error: error.message });
  }

  throw new NotFound();
};

export const fetchRequest = async (
  endpoint: string,
  filter?: FilterFunction
): Promise<RequestResponse> => {
  const endpoints = origins.map((host) => `${host}/${endpoint}`);

  return await getFirstResponse(endpoints, filter);
};

export const getTagValue = (tags: Tag[], name: string): string | undefined => {
  const contentTypeTag = tags.find((tag) => {
    try {
      return (
        fromB64Url(tag.name).toString().toLowerCase() == name.toLowerCase()
      );
    } catch (error) {
      return undefined;
    }
  });
  try {
    return contentTypeTag
      ? fromB64Url(contentTypeTag.value).toString()
      : undefined;
  } catch (error) {
    return undefined;
  }
};

function isValidUTF8(buffer: Buffer) {
  return Buffer.compare(Buffer.from(buffer.toString(), "utf8"), buffer) === 0;
}

export const utf8DecodeTag = (
  tag: Tag
): { name: string | undefined; value: string | undefined } => {
  let name = undefined;
  let value = undefined;
  try {
    const nameBuffer = fromB64Url(tag.name);
    if (isValidUTF8(nameBuffer)) {
      name = nameBuffer.toString("utf8");
    }
    const valueBuffer = fromB64Url(tag.value);
    if (isValidUTF8(valueBuffer)) {
      value = valueBuffer.toString("utf8");
    }
  } catch (error) {}
  return {
    name,
    value,
  };
};

interface RequestResponse {
  status?: number;
  headers?: FetchHeaders;
  body?: Buffer;
}

type FilterFunction = (options: {
  status: number;
  headers: FetchHeaders;
}) => boolean;

const getFirstResponse = async <T = any>(
  urls: string[],
  filter?: FilterFunction,
  options?: FetchRequestInit
): Promise<RequestResponse> => {
  const controllers: AbortController[] = [];

  const defaultFilter: FilterFunction = ({ status }) =>
    [200, 201, 202, 208].includes(status);

  return new Promise(async (resolve, reject) => {
    let isResolved = false;
    await Promise.all(
      shuffle(urls).map(async (url, index) => {
        await new Promise((resolve) => setTimeout(resolve, index * 500));

        if (isResolved) {
          return;
        }

        const controller = new AbortController();
        controllers.push(controller);

        try {
          const response = await fetch(url, {
            ...(options || {}),
            signal: controller.signal,
          });

          if (
            filter
              ? filter({ status: response.status, headers: response.headers })
              : defaultFilter({
                  status: response.status,
                  headers: response.headers,
                })
          ) {
            isResolved = true;
            controllers.forEach((requestController) => {
              if (requestController != controller) {
                requestController.abort();
              }
            });
            resolve({
              body: await response.buffer(),
              status: response.status,
              headers: response.headers,
            });
          }
        } catch (error) {
          if (error.type != "aborted") {
            log.warn(`[arweave] request error`, {
              message: error.message,
              url,
            });
          }
        }
      })
    );
    resolve({});
  });
};
