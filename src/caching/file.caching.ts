import { config } from 'dotenv';
import fs from 'fs-jetpack';
import { streamAndCacheTx } from './stream.caching.js';
import { streamAndCacheAns } from './ans.caching.js';

config();

export const cacheFolder = process.env.CACHE_FOLDER;

// export async function cacheFile(id: string) {
//   if (exists(`${cacheFolder}/${id}`) === false) {
//     await streamAndCacheTx(id);
//   }
// }

export async function cacheAnsFile(id: string) {
  if (fs.exists(`${cacheFolder}/${id}`) === false) {
    await streamAndCacheAns(id);
  }
}
