import {createWriteStream} from 'fs';
import {dir, remove} from 'fs-jetpack';
import {types as CassandraTypes} from 'cassandra-driver';
import {cacheFolder} from './file.caching';
import {getChunk} from '../query/chunk.query';

export async function streamAndCacheTx(
    id: string,
    startOffset: CassandraTypes.Long,
    endOffset: CassandraTypes.Long,
): Promise<boolean> {
  try {
    dir(`${cacheFolder}`);

    const fileStream = createWriteStream(`${cacheFolder}/${id}`, {
      flags: 'w',
    });

    let byte = 0;

    while (startOffset.add(byte).lt(endOffset)) {
      const chunk = await getChunk({
        offset: startOffset.add(byte).toString(),
      });
      byte += chunk.parsed_chunk.length;

      fileStream.write(Buffer.from(chunk.parsed_chunk));
    }

    fileStream.end();

    return true;
  } catch (error) {
    remove(`${cacheFolder}/${id}`);
    console.error(
        `error caching data from ${id}, please note that this may be a cancelled transaction`
            .red.bold,
    );
    throw error;
  }
}
