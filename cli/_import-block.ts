import { importBlock } from "../src/workers/import-block";

const args = process.argv.slice(2);

importBlock(parseInt(args[0]));
