import Cache from "async-disk-cache";
import pWaitFor from "p-wait-for";
import path from "node:path";
import mkdirp from "mkdirp";

// const startupTimeSeconds = Math.floor(Date.now() / 1000);

const cacheDirectory = process.env.CACHE_PATH
  ? process.env.CACHE_PATH
  : path.resolve(process.cwd(), "cache");

mkdirp.sync(cacheDirectory);

const cache = new Cache("imports", {
  location: cacheDirectory,
  compression: false,
  key: "sync",
  root: ".",
});

export const putCache = async (key: string, value: unknown): Promise<void> => {
  try {
    await cache.set(key, value);
    await pWaitFor(() => cache.has(key), { timeout: 60 * 1000 });
  } catch (error) {
    console.error(error);
  }
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const getCache = async (key: string): Promise<any> => {
  let resp;
  try {
    resp = await cache.get(key);
  } catch {}
  return resp ? resp.value : undefined;
};

export const rmCache = async (key: string): Promise<void> => {
  try {
    await cache.remove(key);
  } catch (error) {
    console.error(error);
  }
};

export const purgeCache = async (): Promise<void> => await cache.clear();

// export const gcImportCache = async (): Promise<void> => {
//   const now = Date.now();
//   const nowSeconds = Math.floor(now / 1000);

//   if (nowSeconds - startupTimeSeconds < 60 * 5) {
//     return;
//   }
//   try {
//     await cacache.verify(importCacheDirectory, {
//       filter: ({ time, path }) => {
//         const pathExists = fs.existsSync(path);
//         const hourPassed = nowSeconds - time / 1000 > 60 * 60;

//         return !pathExists ? true : !hourPassed;
//       },
//     });
//   } catch {}
// };

// export const lastGcImportCacheRun = async (): Promise<number> => {
//   let neverRan = true;
//   let lastRunDate: Date;
//   try {
//     lastRunDate = await cacache.verify.lastRun(importCacheDirectory);
//     neverRan = false;
//   } catch {}

//   if (neverRan) {
//     return 9_999_999;
//   }
//   const now = new Date();
//   const secondsAgo = Math.floor((now.getTime() - lastRunDate.getTime()) / 1000);
//   return secondsAgo;
// };

// export const recollectIncomingTxs = async (): Promise<any> => {
//   let entireCache;
//   try {
//     entireCache = await cacache.ls(importCacheDirectory);
//   } catch {}
//   return !entireCache
//     ? []
//     : R.filter(
//         (R.pipe as any)(
//           R.both(
//             R.over(R.lensProp("key"), R.is(String)),
//             R.over(R.lensProp("key"), R.startsWith("incoming:"))
//           ),
//           R.prop("key")
//         )
//       )(R.values(entireCache));
// };

// export const recollectImportableTxs = async (): Promise<any> => {
//   let entireCache;
//   try {
//     entireCache = await cacache.ls(importCacheDirectory);
//   } catch {}

//   return !entireCache
//     ? []
//     : R.filter(
//         (R.pipe as any)(
//           R.both(
//             R.over(R.lensProp("key"), R.is(String)),
//             R.over(R.lensProp("key"), R.startsWith("tx:"))
//           ),
//           R.prop("key")
//         )
//       )(R.values(entireCache));
// };
