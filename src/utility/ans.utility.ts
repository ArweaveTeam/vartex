import Arweave from 'arweave';
import deepHash from 'arweave/node/lib/deepHash.js';
import arweaveBundles from 'arweave-bundles';

export const ansDeps = {
  utils: Arweave.utils,
  crypto: Arweave.crypto,
  deepHash: deepHash,
};

export const ansBundles = (arweaveBundles as any).default(ansDeps);
