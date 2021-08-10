import * as R from "rambda";
import express from "express";
import net from "net";
import path from "path";
import killPort from "kill-port";
import child_process, { fork } from "child_process";
import { testEnvVars } from "./setup";

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

export function initDb(): Promise<string> {
  return new Promise((resolve, reject) => {
    let invoked = false;
    const forkps = fork(path.resolve("./", "cassandra/init.cjs"), {
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
}

export function nuke(): Promise<string> {
  return new Promise((resolve, reject) => {
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
}

export function generateMockBlocks({
  totalBlocks,
  offset = 0,
  hashPrefix = "x",
}) {
  const template = {
    nonce: "n1",
    previous_block: hashPrefix,
    timestamp: 1,
    last_retarget: 1,
    diff: "1111",
    height: 0,
    hash: "_____x",
    indep_hash: hashPrefix,
    txs: [],
    tx_root: "root1",
    wallet_list: "wl1",
    reward_addr: "xyz1",
    tags: [],
    reward_pool: "123",
    weave_size: "123",
    block_size: "123",
    cumulative_diff: "123",
    hash_list_merkle: "xxx",
    poa: {
      option: "1",
      tx_path: "txp1",
      data_path: "dp1",
      chunk: "ch1",
    },
  };

  const blockHeights = R.range(offset, offset + totalBlocks);

  return blockHeights.map((height) =>
    R.pipe(
      R.assoc("height", height),
      R.assoc("indep_hash", `${hashPrefix}${height}`),
      R.assoc("previous_block", `${hashPrefix}${height - 1}`)
    )(template)
  );
}

export function startGateway(): any {
  return child_process.spawn(
    "node",
    [
      "--experimental-specifier-resolution=node",
      "--max-old-space-size=4096",
      "--loader=ts-node/esm.mjs",
      "src/Gateway.ts",
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
  const logs = [];
  let fullySyncPromiseResolve: any;
  const shouldStop = (log) =>
    stopCondition
      ? stopCondition(log.toString())
      : /fully synced db/g.test(log.toString()) ||
        /import queues have been consumed/g.test(log.toString());
  let proc = startGateway();
  proc.stderr.on("data", (log: string) => {
    if (shouldStop(log) && fullySyncPromiseResolve) {
      fullySyncPromiseResolve = undefined;
      setTimeout(fullySyncPromiseResolve, 0);
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

      await killPort(3000);
      await new Promise((res_) => setTimeout(res_, 0));

      resolve(logs.join(" "));
    };
  });
}

export async function setupTestNode({ mockBlocks }) {
  const app = express();

  app.get("/hash_list", function (req, res) {
    res.status(200).json(R.reverse(R.pluck("indep_hash", mockBlocks)));
  });

  app.get("/info", function (req, res) {
    res.status(200).json(lastBlock);
  });

  app.get("/block/height/:id", function (req, res) {
    const match = R.find(R.propEq("height", parseInt(req.params.id)))(
      mockBlocks
    );
    if (match) {
      res.status(200).json(match);
    } else {
      res.status(404);
    }
  });

  app.get("/block/hash/:id", function (req, res) {
    const match = R.find(R.propEq("indep_hash", req.params.id))(mockBlocks);

    if (match) {
      res.status(200).json(match);
    } else {
      res.status(404);
    }
  });

  app.get("*", function (req, res) {
    console.error(req);
    res.status(404);
  });

  srv = app.listen(PORT);
  return { app, srv };
}
