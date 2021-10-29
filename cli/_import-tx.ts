import { importTx } from "../src/workers/import-tx";

// txId, blockHeight
const args = process.argv.slice(2);

importTx(args[0], args[1]);
