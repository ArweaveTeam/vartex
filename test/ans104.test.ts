import { processAns104Transaction } from "../src/database/ans104.database";

describe("ANS-104 suite", function () {
    it("Should index correctly using existing tx", async function () {
        const txId = "j_pQmIliWmSEwEUNsFupLvxdpiSVP749W1rS7gwWPgw";
        await processAns104Transaction(txId);
    }, 30_000)
});
