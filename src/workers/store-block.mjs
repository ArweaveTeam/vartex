import { ThreadWorker } from 'poolifier';
import got from "got";
import { log } from "../utility/log.utility";
import { grabNode, warmNode, coolNode } from "./node.query";
import { HTTP_TIMEOUT_SECONDS } from "../constants";

// get block by hash is optional (needs proper decoupling)
export async function getBlock({
  hash,
  height,
  gauge,
  getProgress,
}){
  if(gauge) {
    gauge = new Function(gauge);
  }
  if(getProgress) {
    getProgress = new Function(getProgress);
  }

  const tryNode = grabNode();
  const url = hash
    ? `${tryNode}/block/hash/${hash}`
    : `${tryNode}/block/height/${height}`;
  gauge && gauge.show(`${getProgress ? getProgress() || "" : ""} ${url}`);
  // const

  let body;
  try {
    body = (await got.get(url, {
      responseType: "json",
      resolveBodyOnly: true,
      timeout: HTTP_TIMEOUT_SECONDS * 1000,
      followRedirect: true,
    }));
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
  hash
) {
  const tryNode = grabNode();
  const url = `${tryNode}/block/hash/${hash}`;

  let body;
  try {
    body = (await got.get(url, {
      responseType: "json",
      resolveBodyOnly: true,
      timeout: HTTP_TIMEOUT_SECONDS * 1000,
      followRedirect: true,
    }));
  } catch {
    coolNode(tryNode);
  }

  if (!body) {
    return fetchBlockByHash(hash);
  }

  warmNode(tryNode);
  return body;
}

export async function currentBlock(){
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


export default new ThreadWorker(getBlock, {
  maxInactiveTime: 60000,
  async: false
});