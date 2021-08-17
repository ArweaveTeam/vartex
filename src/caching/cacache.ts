import * as R from "rambda";
import cacache from "cacache";
import path from "node:path";
import mkdirp from "mkdirp";
import rimraf from "rimraf";

const importCacheDirectory = process.env.CACHE_IMPORT_PATH
  ? process.env.CACHE_IMPORT_PATH
  : path.resolve(process.cwd(), "cache/imports");

mkdirp.sync(importCacheDirectory);

export const putCache = async (
  key: string,
  value: any
): Promise<string | undefined> => {
  let integrity;
  try {
    integrity = await cacache.put(importCacheDirectory, key, value);
  } catch {
    console.error(
      "FATAL: unable to put in new cache, tried putting the following into importCache",
      key,
      value
    );
  }
  return integrity ? integrity : undefined;
};

export const getCache = async (integrity: string): Promise<any> => {
  let resp;
  try {
    resp = cacache.get.byDigest(importCacheDirectory, integrity);
  } catch {
    console.error(
      "went looking for entry by digest in import cache but didn't find integrity:",
      integrity
    );
  }
  return resp ? resp : undefined;
};

export const getCacheByKey = async (key: string): Promise<any> => {
  let resp;
  try {
    resp = await cacache.get(importCacheDirectory, key);
  } catch {}
  return resp ? resp : undefined;
};

export const rmCache = async (key: string): Promise<void> => {
  try {
    await cacache.rm.entry(importCacheDirectory, key);
  } catch {}
};

export const gcImportCache = async (): Promise<void> => {
  try {
    await cacache.verify(importCacheDirectory, {
      filter: ({ time }) => {
        const now = new Date();
        const nowSeconds = Math.floor(now.getTime() / 1000);
        // for safety, keep all 5 minute old cache, just in case of slowness causing purge before read
        return nowSeconds - time > 60 * 5;
      },
    });
  } catch {}
};

export const lastGcImportCacheRun = async (): Promise<number> => {
  let neverRan = true;
  let lastRunDate: Date;
  try {
    lastRunDate = await cacache.verify.lastRun(importCacheDirectory);
    neverRan = false;
  } catch {}

  if (neverRan) {
    return 9_999_999;
  }
  const now = new Date();
  const secondsAgo = Math.floor((now.getTime() - lastRunDate.getTime()) / 1000);
  return secondsAgo;
};

export const purgeCache = (): Promise<void> =>
  new Promise((resolve) =>
    rimraf(importCacheDirectory, {}, () => {
      mkdirp.sync(importCacheDirectory);
      resolve();
    })
  );

export const recollectIncomingTxs = async (): Promise<any> => {
  let entireCache;
  try {
    entireCache = await cacache.ls(importCacheDirectory);
  } catch {}
  return !entireCache
    ? []
    : R.filter(
        (R.pipe as any)(
          R.both(
            R.over(R.lensProp("key"), R.is(String)),
            R.over(R.lensProp("key"), R.startsWith("incoming:"))
          ),
          R.prop("key")
        )
      )(R.values(entireCache));
};

export const recollectImportableTxs = async (): Promise<any> => {
  let entireCache;
  try {
    entireCache = await cacache.ls(importCacheDirectory);
  } catch {}

  return !entireCache
    ? []
    : R.filter(
        (R.pipe as any)(
          R.both(
            R.over(R.lensProp("key"), R.is(String)),
            R.over(R.lensProp("key"), R.startsWith("tx:"))
          ),
          R.prop("key")
        )
      )(R.values(entireCache));
};
