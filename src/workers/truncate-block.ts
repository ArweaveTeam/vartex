import {
  blockMapper,
  blockHeightToHashMapper,
  manifestMapper,
  manifestQueueMapper,
  permawebPathMapper,
  statusMapper,
  transactionMapper,
  txGqlAscMapper,
  txGqlDescMapper,
  txOffsetMapper,
} from "./mapper";
for await (const block of result) {
  log.info(
    `[fork recovery] removing data from abandoned block: ${block.block_hash} at height ${block.block_height}`
  );
  const abandonedBlock = await blockMapper.get({
    indep_hash: block.block_hash,
  });
  await blockMapper.remove({ indep_hash: block.block_hash });
  await blockHeightToHashMapper.remove({
    block_height: block.block_height,
  });

  // BlockGqlAsc
  const txGqlBlockAscPartitionId = CONST.getGqlBlockHeightAscPartitionName(
    block.block_height
  );
  const txGqlBlockAscBucketName = CONST.getGqlBlockHeightAscBucketName(
    block.block_height
  );
  const txGqlBlockAscBucketNumber = CONST.getGqlBlockHeightAscBucketNumber(
    block.block_height
  );

  try {
    await blockGqlAscMapper.remove({
      partition_id: txGqlBlockAscPartitionId,
      bucket_id: txGqlBlockAscBucketName,
      bucket_number: txGqlBlockAscBucketNumber,
      height: block.block_height,
    });
  } catch {}

  // BlockGqlDesc
  const txGqlBlockDescPartitionId = CONST.getGqlBlockHeightDescPartitionName(
    block.block_height
  );
  const txGqlBlockDescBucketName = CONST.getGqlBlockHeightDescBucketName(
    block.block_height
  );
  const txGqlBlockDescBucketNumber = CONST.getGqlBlockHeightDescBucketNumber(
    block.block_height
  );

  try {
    await blockGqlDescMapper.remove({
      partition_id: txGqlBlockDescPartitionId,
      bucket_id: txGqlBlockDescBucketName,
      bucket_number: txGqlBlockDescBucketNumber,
      height: block.block_height,
    });
  } catch {}

  if (!R.isEmpty(abandonedBlock.txs)) {
    for (const abandonedTx of abandonedBlock.txs) {
      const txGqlAscPart = CONST.getGqlTxIdAscPartitionName(block.block_height);
      const txGqlAscBucketId = CONST.getGqlTxIdAscBucketName(
        block.block_height
      );
      const txGqlAscBucketNumber = CONST.getGqlTxIdAscBucketNumber(
        block.block_height
      );
      try {
        await txGqlAscMapper.remove({
          tx_index: abandonedTx.tx_index,
          partition_id: txGqlAscPart,
          bucket_id: txGqlAscBucketId,
          bucket_number: txGqlAscBucketNumber,
        });
      } catch {}

      const txGqlDescPart = CONST.getGqlTxIdDescPartitionName(
        block.block_height
      );
      const txGqlDescBucketId = CONST.getGqlTxIdDescBucketName(
        block.block_height
      );
      const txGqlDescBucketNumber = CONST.getGqlTxIdDescBucketNumber(
        block.block_height
      );

      try {
        await txGqlDescMapper.remove({
          tx_index: abandonedTx.tx_index,
          partition_id: txGqlDescPart,
          bucket_id: txGqlDescBucketId,
          bucket_number: txGqlDescBucketNumber,
        });
      } catch {}

      try {
        await txOffsetMapper.remove({ tx_id: abandonedTx.tx_id });
      } catch {}

      if (
        abandonedTx.tags &&
        Array.isArray(abandonedTx.tags) &&
        !R.isEmpty(abandonedTx.tags)
      ) {
        const abandonedTxTags = abandonedTx.tag.map(
          (t: CassandraTypes.Tuple) => t.values
        );
        const isManifest = hasManifestContentType(abandonedTxTags);
        let index = 0;

        for (const abandonedTag of abandonedTxTags) {
          const [tagName, tagValue] = abandonedTag;

          const owner = ownerToAddress(abandonedTx.owner);
          const tagDropParameters: DropTagQueryParameters = {
            tagName,
            tagValue,
            owner,
            bundledIn: abandonedTx.bundled_in,
            dataItemIndex: "0",
            dataRoot: abandonedTx.data_root,
            tagIndex: `${index}`,
            target: abandonedTx.target,
            txId: abandonedTx.tx_id,
            txIndex: abandonedTx.tx_id,
          };
          await dropTagQuery(tagDropParameters);
          index += 1;
        }
        if (isManifest) {
          const maybeManifest = await manifestMapper.get({
            tx_id: abandonedTx.tx_id,
          });

          if (maybeManifest) {
            const manifestPaths = JSON.parse(maybeManifest.manifest_paths);
            const manifestFiles = Object.keys(manifestPaths);

            if (manifestFiles.includes(maybeManifest.manifest_index)) {
              try {
                await permawebPathMapper.remove({
                  domain_id: abandonedTx.tx_id,
                  uri_path: "",
                });
              } catch {}
            }
            for (const manifestFile of manifestFiles) {
              try {
                await permawebPathMapper.remove({
                  domain_id: abandonedTx.tx_id,
                  uri_path: escape(manifestFile),
                });
              } catch {}
            }
          }
          try {
            await manifestUnimportedMapper.remove(
              { tx_id: abandonedTx.tx_id },
              { ifExists: true }
            );
          } catch {}
        }
      }
    }
  }
}
