import * as R from "rambda";
import cassandra, { types as CassandraTypes } from "cassandra-driver";
import { exists as existsOrig } from "fs";
import fs from "fs/promises";
import { jest } from "@jest/globals";
import util from "util";
import got from "got";
import killPort from "kill-port";
import * as helpers from "./helpers";

const appState: Map<string, any> = new Map();

const exists = util.promisify(existsOrig);

appState.set(
  "mockBlocks",
  helpers.generateMockBlocks({ totalBlocks: 100 }) as any[]
);

const tmpNextBlock: any = R.last(appState.get("mockBlocks"));
appState.set("lastBlockHeight", tmpNextBlock.height as number);
appState.set("lastBlockHash", tmpNextBlock.indep_hash as string);

let app: any;
let srv: any;
let proc: any;
let client: any;

describe("database sync test suite", function () {
  jest.setTimeout(60000);
  beforeAll(async function () {
    await helpers.waitForCassandra();
    client = new cassandra.Client({
      contactPoints: ["localhost:9042"],
      localDataCenter: "datacenter1",
    });
    const { srv, app } = await helpers.setupTestNode(appState);
  });

  afterAll(async () => {
    srv && srv.close();
    if (proc) {
      proc.kill("SIGINT");
      proc = undefined;
    }
    // wait a second for handlers to close
    await new Promise((resolve) => setTimeout(resolve, 1000));
  });

  afterEach(async () => {
    // togglePause();
    if (proc) {
      proc.kill("SIGINT");
      proc = undefined;
    }

    await killPort(3000);
    await new Promise((resolve) => setTimeout(resolve, 1000));
  });
  beforeEach(async () => {
    jest.resetModules();
    jest.setTimeout(60000);
  });

  test("it writes 100 blocks into cassandra", async () => {
    await helpers.nuke();
    await helpers.initDb();

    if (await exists("./cache/hash_list_test.json")) {
      await fs.unlink("./cache/hash_list_test.json");
    }

    const logs = await helpers.runGatewayOnce({});

    const queryResponse = await client.execute(
      "SELECT COUNT(*) FROM testway.block ALLOW FILTERING"
    );

    expect(queryResponse.rows[0].count.toString()).toEqual("100");
  });

  test("it detects correctly fully synced db on startup", async () => {
    const logs = await helpers.runGatewayOnce({});

    const queryResponse = await client.execute(
      "SELECT COUNT(*) FROM testway.block ALLOW FILTERING"
    );

    expect(logs).not.toContain("database seems to be empty");
    expect(logs).not.toContain("Found missing block");
  });

  test("it starts polling and receives new blocks", async () => {
    let shouldStop = false;
    const runp = helpers.runGatewayOnce({
      stopCondition: (log) => {
        if (log.includes("new block arrived at height 100")) {
          shouldStop = true;
          return false;
        }
        return shouldStop;
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const nextBlock: any = helpers.generateMockBlocks({
      totalBlocks: 1,
      offset: 100,
    })[0];

    process.stderr.write(
      "LASTPRE: " + JSON.stringify(R.last(appState.get("mockBlocks"))) + "\n"
    );

    appState.set("mockBlocks", R.append(nextBlock, appState.get("mockBlocks")));
    process.stderr.write("NEXT: " + JSON.stringify(nextBlock) + "\n");
    process.stderr.write(
      "LAST: " + JSON.stringify(R.last(appState.get("mockBlocks"))) + "\n"
    );

    process.stderr.write(
      "LASTHEIGHTPRE: " + JSON.stringify(appState.get("lastBlockHeight")) + "\n"
    );

    process.stderr.write(
      "LASTHASHPRE: " + JSON.stringify(appState.get("lastBlockHash")) + "\n"
    );

    appState.set("lastBlockHeight", nextBlock.height as number);
    appState.set("lastBlockHash", nextBlock.indep_hash as string);

    process.stderr.write(
      "LASTHEIGHTPOST: " +
        JSON.stringify(appState.get("lastBlockHeight")) +
        "\n"
    );

    process.stderr.write(
      "LASTHASHPOST: " + JSON.stringify(appState.get("lastBlockHash")) + "\n"
    );

    await runp;

    await new Promise((resolve) => setTimeout(resolve, 100));

    const queryResponse = await client.execute(
      "SELECT COUNT(*) FROM testway.block ALLOW FILTERING"
    );

    expect(queryResponse.rows[0].count.toString()).toEqual("101");
  });

  test("it recovers when fork changes", async () => {
    let logs = "";
    let fullySyncPromiseResolve: any;
    let newForkPromiseResolve: any;
    proc = proc || helpers.startGateway();
    const logCallback = (log: string) => {
      if (
        /polling for new blocks/g.test(log.toString()) &&
        fullySyncPromiseResolve
      ) {
        fullySyncPromiseResolve();
        fullySyncPromiseResolve = undefined;
      }

      if (
        /blocks are back in sync/g.test(log.toString()) &&
        newForkPromiseResolve
      ) {
        newForkPromiseResolve();
        newForkPromiseResolve = undefined;
      }

      process.stderr.write(log);
      logs += log.toString();
    };
    proc.stderr.on("data", logCallback);
    proc.stdout.on("data", logCallback);
    await new Promise((resolve, reject) => {
      fullySyncPromiseResolve = resolve;
    });

    let nextFork: any[] = helpers.generateMockBlocks({
      totalBlocks: 15,
      offset: 90,
      hashPrefix: "y",
    });

    appState.set(
      "mockBlocks",
      R.splitWhen(R.propEq("height", 90))(appState.get("mockBlocks"))[0]
    );
    nextFork = R.concat(
      [
        R.assoc(
          "previous_block",
          (R.last(appState.get("mockBlocks")) as any).indep_hash,
          R.head(nextFork)
        ),
      ],
      R.slice(1, nextFork.length, nextFork)
    );

    appState.set("mockBlocks", R.concat(appState.get("mockBlocks"), nextFork));

    appState.set(
      "lastBlockHeight",
      (R.last(appState.get("mockBlocks")) as any).height as number
    );
    appState.set(
      "lastBlockHash",
      (R.last(appState.get("mockBlocks")) as any).indep_hash as string
    );

    await new Promise((resolve, reject) => {
      newForkPromiseResolve = resolve;
    });

    const queryResponse = await client.execute(
      "SELECT indep_hash,height FROM testway.block WHERE height>85 AND height<95 ALLOW FILTERING"
    );
    const result = queryResponse.rows.map((obj: any) => ({
      height: parseInt(obj.height),
      hash: obj.indep_hash,
    }));

    expect(
      R.filter(R.equals({ height: 86, hash: "x86" }), result)
    ).toHaveLength(1);
    expect(
      R.filter(R.equals({ height: 87, hash: "x87" }), result)
    ).toHaveLength(1);
    expect(
      R.filter(R.equals({ height: 88, hash: "x88" }), result)
    ).toHaveLength(1);
    expect(
      R.filter(R.equals({ height: 89, hash: "x89" }), result)
    ).toHaveLength(1);
    expect(
      R.filter(R.equals({ height: 90, hash: "y90" }), result)
    ).toHaveLength(1);
    expect(
      R.filter(R.equals({ height: 91, hash: "y91" }), result)
    ).toHaveLength(1);
    expect(
      R.filter(R.equals({ height: 92, hash: "y92" }), result)
    ).toHaveLength(1);
  });
});
