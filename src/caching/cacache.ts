import cacache from "cacache";
import path from "path";
import mkdirp from "mkdirp";

const cacheDirectory = path.resolve(process.cwd(), "cache/cacache");

mkdirp.sync(cacheDirectory);

export const putCache = (key: string, value: any): Promise<any> =>
  cacache.put(cacheDirectory, key, value);

export const getCache = (integrity: string): Promise<any> =>
  cacache.get.byDigest(cacheDirectory, integrity);

export const rmCache = (integrity: string): Promise<any> =>
  cacache.rm.entry(cacheDirectory, integrity);
