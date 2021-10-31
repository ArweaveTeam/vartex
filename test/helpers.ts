import * as R from "rambda";
import express from "express";
import net from "net";
import path from "path";
import killPort from "kill-port";
import child_process, { fork } from "child_process";
import { testEnvVars } from "./setup";
import { purgeCache } from "../src/caching/cacache";

const PORT = parseInt(process.env.PORT);

export function waitForCassandra(): Promise<void> {
  return new Promise((resolve, reject) => {
    const maxRetry = 100;
    let rtry = 0;
    // Wait until cassandra is reachable
    const retry = () => {
      const client = net
        .createConnection(9042, "127.0.0.1")
        .on("error", function (error: string) {
          rtry += 1;
          if (rtry < maxRetry) {
            new Promise((resolveRetry) => setTimeout(resolveRetry, 1000)).then(
              retry
            );
          } else {
            throw new Error(
              "Couldn't find cassandra running after 100 retries: " + error
            );
          }
        })
        .on("connect", function () {
          try {
            client.destroy();
            // eslint-disable-next-line no-empty
          } catch (error) {}
          resolve();
        });
    };
    retry();
  });
}

const retryPort = async (port: number, retry = 0): Promise<void> => {
  const maxRetry = 100;
  return await new Promise((resolve) => {
    const client = net
      .createConnection(port, "127.0.0.1", () => {
        if (retry < maxRetry) {
          new Promise((resolveRetry) => setTimeout(resolveRetry, 1)).then(
            () => {
              return retryPort(port, retry + 1);
              try {
                client.destroy();
                // eslint-disable-next-line no-empty
              } catch (error) {}
            }
          );
        } else {
          // throw new Error(`Couldn't kill port ${port}`);
          resolve();
        }
      })
      .on("error", function (error: string) {
        try {
          client.destroy();
          // eslint-disable-next-line no-empty
        } catch (error) {}
        resolve();
      });
  });
};

export function killPortAndWait(port: number): Promise<void> {
  return new Promise(async (resolve, reject) => {
    await killPort(port);
    await retryPort(port);
    resolve();
  });
}

export function initDb(): Promise<string> {
  return new Promise((resolve, reject) => {
    // let invoked = false;
    const forkps = fork(path.resolve("./", "cassandra/init.cjs"), {
      env: process.env,
    });

    // listen for errors as they may prevent the exit event from firing
    forkps.on("error", function (err) {
      // if (invoked) return;
      // invoked = true;
      reject((err || "").toString());
    });

    // execute the callback once the forkps has finished running
    forkps.on("exit", function (code) {
      // if (invoked) return;
      // invoked = true;
      const err = code === 0 ? null : new Error("exit code " + code);
      resolve((err || "").toString());
    });
  });
}

export function nuke(): Promise<string> {
  return new Promise((resolve, reject) => {
    purgeCache().then(() => {
      let invoked = false;
      const forkps = fork(path.resolve("./", "cassandra/nuke.cjs"), {
        env: process.env,
      });

      // listen for errors as they may prevent the exit event from firing
      forkps.on("error", function (err) {
        if (invoked) return;
        invoked = true;
        reject((err || "").toString());
      });

      // execute the callback once the forkps has finished running
      forkps.on("exit", function (code) {
        if (invoked) return;
        invoked = true;
        const err = code === 0 ? null : new Error("exit code " + code);
        resolve((err || "").toString());
      });
    });
  });
}

function randomString(length: number) {
  let result = "";
  const characters =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  const charactersLength = characters.length;
  for (let i = 0; i < length; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
}

export function generateRandomMockTxs() {
  const template = {
    data: "",
    id: "",
    last_tx: "",
    owner: "",
    quantity: 0,
    reward: 0,
    signature: "",
    tags: [] as string[],
    target: "",
  };

  const txsRange = R.range(0, Math.floor(Math.random() * 10));

  const txs = txsRange.reduce(
    ({ lastId, acc }) => {
      const nextId = randomString(43);
      const tagsRange = R.range(0, Math.floor(Math.random() * 10));
      const nextTx = R.pipe(
        R.assoc("id", nextId),
        R.assoc("last_tx", lastId),
        R.assoc("reward", Math.floor(Math.random() * 1000)),
        R.assoc("quantity", Math.floor(Math.random() * 1000)),
        R.assoc(
          "tags",
          tagsRange.map(() => ({
            name: randomString(20),
            value: randomString(20),
          }))
        )
      )(template);
      return { lastId: nextId, acc: R.append(nextTx, acc) };
    },
    { lastId: "", acc: [] }
  );

  return txs.acc;
}

export function generateMockBlocks({
  totalBlocks,
  offset = 0,
  hashPrefix = "x",
}: {
  totalBlocks: number;
  offset?: number;
  hashPrefix?: string;
}): { blocks: any[]; txs: any[] } {
  const template = {
    nonce: "n1",
    previous_block: hashPrefix,
    timestamp: 1,
    last_retarget: 1,
    diff: "1111",
    height: 0,
    hash: "_____x",
    indep_hash: hashPrefix,
    txs: [] as any[],
    tx_root: "root1",
    wallet_list: "wl1",
    reward_addr: "xyz1",
    tags: [] as string[],
    reward_pool: "123",
    weave_size: "123",
    block_size: "123",
    cumulative_diff: "123",
    hash_list_merkle: "xxx",
  };

  const blockHeights = R.range(offset, offset + totalBlocks);
  let txs: any[] = [];
  const blocks = blockHeights.map((height) => {
    const thisTxs = generateRandomMockTxs();
    txs = R.concat(txs, thisTxs);

    return R.pipe(
      R.assoc("height", height),
      R.assoc("indep_hash", `${hashPrefix}${height}`),
      R.assoc("previous_block", `${hashPrefix}${height - 1}`),
      R.assoc(
        "txs",
        thisTxs.map((tx) => tx.id)
      )
    )(template);
  });
  return { blocks, txs };
}

export function startGateway(): any {
  return child_process.spawn(
    "node",
    [
      "--experimental-specifier-resolution=node",
      "--max-old-space-size=4096",
      "--loader=ts-node/esm.mjs",
      "src/gateway.ts",
    ],
    {
      env: testEnvVars,
    }
  );
}

export async function runGatewayOnce({
  onLog,
  stopCondition,
}: {
  onLog?: (log: string) => boolean;
  stopCondition?: (log: string) => boolean;
}): Promise<string> {
  const logs: string[] = [];
  let fullySyncPromiseResolve: any;
  const shouldStop = (log: string) =>
    stopCondition
      ? stopCondition(log.toString())
      : /fully synced db/g.test(log.toString()) ||
        /import queues have been consumed/g.test(log.toString());

  let proc = startGateway();

  proc.stderr.on("data", (log: string) => {
    if (shouldStop(log) && fullySyncPromiseResolve) {
      setTimeout(() => {
        fullySyncPromiseResolve();
        fullySyncPromiseResolve = undefined;
      }, 0);
    }

    logs.push(log);
    process.stderr.write(log);
    onLog && onLog(log);
  });

  proc.stdout.on("data", (log: string) => {
    if (shouldStop(log) && fullySyncPromiseResolve) {
      setTimeout(fullySyncPromiseResolve, 0);
    }

    process.stderr.write(log);
    logs.push(log);
    onLog && onLog(log);
    // logs = ' ' + log.toString();
  });

  return await new Promise((resolve, reject) => {
    fullySyncPromiseResolve = async () => {
      if (proc) {
        proc.kill("SIGINT");
        proc = undefined;
      }

      await killPort(PORT);
      await new Promise((res_) => setTimeout(res_, 0));

      resolve(logs.join(" "));
    };
  });
}

export async function setupTestNode(appState: any) {
  const app = express();

  app.get("/hash_list", function (req, res) {
    res
      .status(200)
      .json(
        R.reverse((R.pluck as any)("indep_hash", appState.get("mockBlocks")))
      );
  });

  app.get("/info", function (req, res) {
    res.status(200).json({
      height: appState.get("lastBlockHeight"),
      current: appState.get("lastBlockHash"),
    });
  });

  app.get("/block/height/:id", function (req, res) {
    const match = R.find(R.propEq("height", parseInt(req.params.id)))(
      appState.get("mockBlocks")
    );
    if (match) {
      res.status(200).json(match);
    } else {
      res.status(404);
    }
  });

  app.get("/block/hash/:id", function (req, res) {
    const match = R.find(R.propEq("indep_hash", req.params.id))(
      appState.get("mockBlocks")
    );

    if (match) {
      res.status(200).json(match);
    } else {
      res.status(404);
    }
  });

  app.get("/tx/:id", function (req, res) {
    const match = R.find(R.propEq("id", req.params.id))(
      appState.get("mockTxs")
    );

    if (match) {
      res.status(200).json(match);
    } else {
      res.status(404);
    }
  });

  app.get("*", function (req, res) {
    res.status(404);
  });

  const srv = app.listen(12345);
  return { app, srv };
}
