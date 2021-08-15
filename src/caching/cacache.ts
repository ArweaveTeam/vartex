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

export const getCache = (integrity: string): Promise<any> =>
  cacache.get.byDigest(importCacheDirectory, integrity);

export const getCacheByKey = (integrity: string): Promise<any> =>
  cacache.get(importCacheDirectory, integrity);

export const rmCache = async (key: string): Promise<void> => {
  try {
    await cacache.rm.entry(importCacheDirectory, key, {
      removeFully: true,
    });
  } catch {}
};

export const gcImportCache = (): Promise<any> =>
  cacache.verify(importCacheDirectory);

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
