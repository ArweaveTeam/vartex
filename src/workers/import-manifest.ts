import * as R from "rambda";
import { types as CassandraTypes } from "cassandra-driver";
import { lookup as mimeLookup } from "mime-types";
import { KEYSPACE } from "../constants";
import { getMessenger } from "../gatsby-worker/child";
import { mkWorkerLog } from "../utility/log";
import { utf8DecodeTupleTag } from "../utility/encoding";
import { MessagesFromParent, MessagesFromWorker } from "./message-types";
import { getTxOffset } from "../query/transaction";
import { getDataFromChunks } from "../query/node";
import {
  cassandraClient,
  manifestMapper,
  manifestUnimportedMapper,
  permawebPathMapper,
  transactionMapper,
} from "../database/mapper";
import { ManifestV010 } from "../utility/manifest";

const messenger = getMessenger<MessagesFromParent, MessagesFromWorker>();

if (messenger) {
  messenger.sendMessage({
    type: "worker:ready",
  });
}

const log = mkWorkerLog(messenger);

export async function importManifests(): Promise<void> {
  log("doing manifest import");

  const unimportedManifests = await cassandraClient.execute(
    `SELECT * FROM ${KEYSPACE}.manifest_unimported`,
    [],
    { prepare: true }
  );

  for await (const unimportedManifest of unimportedManifests) {
    let manifest;

    try {
      let buffer;
      const offsetData = await getTxOffset({ txId: unimportedManifest.tx_id });

      if (offsetData) {
        const offset = CassandraTypes.Long.fromString(offsetData.offset);
        buffer = await getDataFromChunks({
          startOffset: offset
            .subtract(CassandraTypes.Long.fromString(offsetData.size))
            .add(1),
          endOffset: offset,
          id: unimportedManifest.tx_id,
          retry: true,
        });
      }
      if (buffer) {
        const unparsed = buffer.toString("utf8");
        manifest = JSON.parse(unparsed);
      }
    } catch (error) {
      messenger.sendMessage({
        type: "log:warn",
        message: "error while downloading manifest from chunks",
        payload: error,
      });
    }
    if (manifest) {
      // validate
      const validResult = ManifestV010.safeParse(manifest);
      if (
        validResult.success &&
        validResult.data.manifest === "arweave/paths" &&
        ["0.0.0", "0.1.0"].includes(validResult.data.version)
      ) {
        const manifestIndex: string = R.pathOr(
          "",
          "index.path",
          validResult.data
        );

        await manifestMapper.insert({
          tx_id: unimportedManifest.tx_id,
          manifest_type: validResult.data.manifest,
          manifest_version: validResult.data.version,
          manifest_index: manifestIndex,
          manifest_paths: JSON.stringify(validResult.data.paths),
        });

        let manifestIndexMatched = false;

        for (const pathUnescaped of Object.keys(validResult.data.paths)) {
          const { id: pathId, ext } = validResult.data.paths[pathUnescaped];
          const tx =
            pathId.length === 43
              ? await transactionMapper.get({ tx_id: pathId })
              : undefined;
          if (tx) {
            let contentType: string | undefined;

            const safePath = escape(pathUnescaped.toLowerCase());

            if (ext) {
              contentType = mimeLookup(ext) || undefined;
            }

            if (!contentType && Array.isArray(tx.tags) && tx.tags.length > 0) {
              const tags = tx.tags.map(utf8DecodeTupleTag);
              for (const tag of tags as { name: string; value: string }[]) {
                if (tag.name.toLowerCase() === "content-type") {
                  contentType = tag.value;
                }
              }
            }

            if (!contentType) {
              contentType =
                mimeLookup(pathUnescaped) || "application/octet-stream";
            }

            if (!manifestIndexMatched && manifestIndex === pathUnescaped) {
              manifestIndexMatched = true;
              permawebPathMapper.insert({
                domain_id: unimportedManifest.tx_id,
                target_id: tx.tx_id,
                uri_path: "",
                content_length: tx.data_size.toString(),
                content_type: contentType,
                blacklisted: false,
                customElements: [], // maybe later?
              });
            }

            permawebPathMapper.insert({
              domain_id: unimportedManifest.tx_id,
              target_id: tx.tx_id,
              uri_path: safePath,
              content_length: tx.data_size.toString(),
              content_type: contentType,
              blacklisted: false,
              customElements: [], // maybe later?
            });
          }
        }
        try {
          await manifestUnimportedMapper.remove({
            tx_id: unimportedManifest.tx_id,
          });
        } catch (error) {
          messenger.sendMessage({
            type: "log:warn",
            message: "error while removing unimported manifest from db",
            payload: error,
          });
        }
      } else {
        messenger.sendMessage({
          type: "log:warn",
          message: `Invalid manifest detected ${unimportedManifest.tx_id} ${
            (validResult as any).error
          }`,
        });
        await manifestMapper.insert({
          tx_id: unimportedManifest.tx_id,
          manifest_type: "error",
        });

        try {
          await manifestUnimportedMapper.remove({
            tx_id: unimportedManifest.tx_id,
          });
        } catch (error) {
          messenger.sendMessage({
            type: "log:warn",
            message: "error while removing unimported manifest from db (2)",
            payload: error,
          });
        }
      }
    } else {
      // 10000 retries, that should settle the deal
      await (unimportedManifest.import_attempt_cnt || 0 < 10000
        ? manifestUnimportedMapper.update({
            tx_id: unimportedManifest.tx_id,
            import_attempt_cnt:
              (unimportedManifest.import_attempt_cnt || 0) + 1,
          })
        : manifestUnimportedMapper.remove({
            tx_id: unimportedManifest.tx_id,
          }));
    }
  }
  return;
}

// importManifests().then(() => process.exit(0));
