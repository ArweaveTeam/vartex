import { Command } from "commander/esm.mjs";
import { spawn } from "child_process";
import dotenv from "dotenv";

dotenv.config();

const program = new Command();

async function importBlock(height) {
  const childProcess = spawn(
    "node",
    [
      "--experimental-specifier-resolution=node",
      "--loader=ts-node/esm.mjs",
      "./cli/_import-block",
      height,
    ],
    {
      shell: true,
      stdio: "inherit",
      env: { ...process.env, TS_NODE_FILES: "true" },
    }
  );
  return new Promise((done) => {
    childProcess.on("exit", (exitCode) => {
      if (exitCode === 0) {
        console.log(`block ${height} was successfully imported`);
        done();
      } else {
        console.log(`block ${height} couldn't be imported`);
        process.exit(exitCode);
      }
    });
  });
}

async function main() {
  program
    .command("import-block <height>")
    .description(
      "import block manually into database (mostly for internal usage)"
    )
    .action(importBlock);

  await program.parseAsync(process.argv);
}

main();
