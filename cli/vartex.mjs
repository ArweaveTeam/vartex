import { Command, Option } from "commander/esm.mjs";
import { spawn } from "child_process";
import ansiStyle from "ansi-styles";
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

async function addWorkerCmd(job) {
  switch (job) {
    case "import-manifests": {
      process.exit(await execa("_worker-import-manifests", []));
      break;
    }
  }
}

async function startCmd(cmd) {
  switch (cmd) {
    case "master": {
      break;
    }
    case "slave": {
      break;
    }
    default: {
      console.error(`unrecoginzed start mode provided: "${cmd}"`);
      process.exit(1);
    }
  }
}

async function main() {
  program.addHelpText(
    "before",
    `
  _....._
     _.. \\ '.
 / /\`  |_ |   \\
; ;  /\` _\\  | ;
|  /|  (_   ; |
; ;  \\  _/ .;
  |'. '.  _.'/
  '._\`
     \`\`\`\`\`
` + `V${ansiStyle.green.open}ar${ansiStyle.green.close}tex`
  );
  const start = program
    .command("start")
    .description("Start a Vartex gateway and listen for requests")
    .argument(
      "[mode]",
      "must be either master or slave mode",
      "no mode selected to start, must be either master or slave"
    )
    .action(startCmd);

  const addWorker = program
    .command("add-worker [job]")
    .action(addWorkerCmd)
    .description("add a worker for a specific gateway task");

  addWorker
    .command(
      "import-manifests"
      // "add independent worker that consumes incoming manifest imports"
    )
    .action(addWorker);

  const oneShot = program
    .command("one-shot [action]")
    .description("run single action on vartex db for single entity");

  oneShot
    .command("import-block <height>")
    .description(
      "a one shot action to import block manually into database" +
        "(this will import txs too and index their tags)"
    )
    .action(importBlock);

  oneShots.command("import-tx <txid>");

  oneShot
    .command("import-manifest <txid>")
    .description("a one shot action import manifest into database")
    .action(importManifest);

  await program.parseAsync(process.argv);
}

main();
