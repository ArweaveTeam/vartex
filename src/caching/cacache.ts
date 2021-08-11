import cacache from "cacache";
import path from "path";
import mkdirp from "mkdirp";

const cacheDir = path.resolve(process.cwd(), "cache/cacache");

mkdirp.sync(cacheDir);

export const putCache = (key: string, value: any): Promise<any> =>
  cacache.put(cacheDir, key, value);

export const getCache = (integrity: string): Promise<any> =>
  cacache.get.byDigest(cacheDir, integrity);
