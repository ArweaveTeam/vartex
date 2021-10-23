import { importManifest } from "../src/workers/import-manifest";

const args = process.argv.slice(2);

importManifest(args[0]).then((success: boolean) =>
  process.exit(success ? 0 : 1)
);
