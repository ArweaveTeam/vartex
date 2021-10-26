import { Command, Option } from "commander/esm.mjs";
import { spawn } from "child_process";
import dotenv from "dotenv";

dotenv.config();

const program = new Command();

async function execa(filename, args) {
  const childProcess = spawn(
    "node",
    [
      "--experimental-specifier-resolution=node",
      "--loader=ts-node/esm.mjs",
      `./cli/${filename}`,
    ].concat(args),
    {
      shell: true,
      stdio: "inherit",
      env: { ...process.env, TS_NODE_FILES: "true" },
    }
  );
  return new Promise((done) => {
    childProcess.on("exit", done);
  });
}

async function importBlock(height) {
  const exitCode = await execa("_import-block", [height]);
  if (exitCode === 0) {
    console.log(`block ${height} was successfully imported`);
  } else {
    console.log(`block ${height} couldn't be imported`);
    process.exit(exitCode);
  }
}

async function importManifest(txId) {
  const exitCode = await execa("_import-manifest", [txId]);
  if (exitCode === 0) {
    console.log(`manifest ${txId} was successfully imported`);
  } else {
    console.log(`manifest ${txId} couldn't be imported`);
    process.exit(exitCode);
  }
}

async function addWorker(job) {
  console.log(job);
  switch (job) {
    case "import-manifests": {
      process.exit(await execa("_worker-import-manifests", []));
      break;
    }
  }
}

async function main() {
  program
    .command("add-worker [job]")
    .action(addWorker)
    .description("add a worker for a specific gateway task")
    .option(
      "import-manifests"
      // "add independent worker that consumes incoming manifest imports"
    )
    .action(addWorker);

  const oneShot = program
    .command("one-shot [action]")
    .description("run single action on vartex db for single entity");

  oneShot
    .command("import-block <height>")
    .description("a one shot action to import block manually into database")
    .action(importBlock);

  oneShot
    .command("import-manifest <txid>")
    .description("a one shot action import manifest into database")
    .action(importManifest);

  await program.parseAsync(process.argv);
}

main();
