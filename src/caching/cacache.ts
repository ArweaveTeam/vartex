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

export const rmCache = (integrity: string): Promise<any> =>
  cacache.rm.entry(importCacheDirectory, integrity);

export const purgeCache = (): Promise<void> =>
  new Promise((resolve, reject) =>
    rimraf(importCacheDirectory, {}, () => {
      mkdirp.sync(importCacheDirectory);
      resolve();
    })
  );

export const recollectIncomingTxs = async (): Promise<any> => {
  const entireCache = await cacache.ls(importCacheDirectory);
  return R.filter(
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
  const entireCache = await cacache.ls(importCacheDirectory);
  return R.filter(
    (R.pipe as any)(
      R.both(
        R.over(R.lensProp("key"), R.is(String)),
        R.over(R.lensProp("key"), R.startsWith("tx:"))
      ),
      R.prop("key")
    )
  )(R.values(entireCache));
};
