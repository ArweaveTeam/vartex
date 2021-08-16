import * as R from "rambda";
import cacache from "cacache";
import path from "node:path";
import mkdirp from "mkdirp";
import rimraf from "rimraf";

const importCacheDirectory = process.env.CACHE_IMPORT_PATH
  ? process.env.CACHE_IMPORT_PATH
  : path.resolve(process.cwd(), "cache/imports");

mkdirp.sync(importCacheDirectory);

export const putCache = (key: string, value: any): Promise<any> =>
  cacache.put(importCacheDirectory, key, value);

export const getCache = async (integrity: string): Promise<any> => {
  let resp;
  try {
    resp = cacache.get.byDigest(importCacheDirectory, integrity);
  } catch (error) {
    console.error(
      "went looking for entry by digest in import cache but didn't find integrity:",
      integrity
    );
  }
  return resp ? resp : undefined;
};

export const getCacheByKey = async (integrity: string): Promise<any> => {
  let resp;
  try {
    resp = await cacache.get(importCacheDirectory, integrity);
  } catch (error) {
    console.error(
      "went looking for entry in import cache but didn't find integrity:",
      integrity
    );
  }
  return resp ? resp : undefined;
};

export const rmCache = async (key: string): Promise<void> => {
  try {
    await cacache.rm.entry(importCacheDirectory, key);
  } catch {}
};

export const gcImportCache = async (): Promise<void> => {
  try {
    await cacache.verify(importCacheDirectory);
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
    return 9999999;
  }
  const now = new Date();
  const secondsAgo = Math.floor((now.getTime() - lastRunDate.getTime()) / 1000);
  return secondsAgo;
};

export const purgeCache = (): Promise<void> =>
  new Promise((resolve, reject) =>
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
  if (!entireCache) {
    return [];
  } else {
    return R.filter(
      (R.pipe as any)(
        R.both(
          R.over(R.lensProp("key"), R.is(String)),
          R.over(R.lensProp("key"), R.startsWith("incoming:"))
        ),
        R.prop("key")
      )
    )(R.values(entireCache));
  }
};

export const recollectImportableTxs = async (): Promise<any> => {
  let entireCache;
  try {
    entireCache = await cacache.ls(importCacheDirectory);
  } catch {}

  if (!entireCache) {
    return [];
  } else {
    return R.filter(
      (R.pipe as any)(
        R.both(
          R.over(R.lensProp("key"), R.is(String)),
          R.over(R.lensProp("key"), R.startsWith("tx:"))
        ),
        R.prop("key")
      )
    )(R.values(entireCache));
  }
};
