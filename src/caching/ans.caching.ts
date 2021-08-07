import {types as CassandraTypes} from 'cassandra-driver';
import fs from 'fs-jetpack';
import {DataItemJson} from 'arweave-bundles';
import {cacheFolder} from './file.caching';
import {ansBundles} from '../utility/ans.utility';
import {getDataFromChunks} from '../query/node.query';
import {tagToUTF8} from '../query/transaction.query';
import {cacheANSEntries} from './ans.entry.caching';

export async function streamAndCacheAns(id: string): Promise<boolean> {
  try {
    fs.dir(`${cacheFolder}`);

    const rawData = await getDataFromChunks({
      id,
      startOffset: CassandraTypes.Long.fromNumber(0), // FIXEME
      endOffset: CassandraTypes.Long.fromNumber(0), // FIXME
    });
    const ansTxs = await ansBundles.unbundleData(rawData.toString('utf-8'));

    const ansTxsConverted: Array<DataItemJson> = [];

    for (let i = 0; i < ansTxs.length; i++) {
      const ansTx = ansTxs[i];
      const newAnsTx: DataItemJson = {
        id: ansTx.id,
        owner: ansTx.owner,
        target: ansTx.target,
        nonce: ansTx.nonce,
        data: ansTx.data,
        signature: ansTx.signature,
        tags: tagToUTF8(ansTx.tags),
      };

      ansTxsConverted.push(newAnsTx);
    }

    fs.write(`${cacheFolder}/${id}`, JSON.stringify(ansTxsConverted, null, 2));

    await cacheANSEntries(ansTxs);

    return true;
  } catch (error) {
    fs.remove(`${cacheFolder}/${id}`);
    console.error(
        `error caching data from ${id}, please note that this may be a cancelled transaction`
            .red.bold,
    );
    throw error;
  }
}
