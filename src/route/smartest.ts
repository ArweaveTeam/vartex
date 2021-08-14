import { Request, Response, NextFunction } from "express";
import { kohaku } from "kohaku";
import Arweave from 'arweave';

// Or manually specify a host
const arweave = Arweave.init({
    host: 'g3.koi.rocks',
    port: 1984,
    protocol: 'http'
});
const reservedIds = ["cETTyJQYxJLVQ6nC3VxzsZf1x2-6TW2LFkGZa91gUWc"]; // enter a list of txids that are koii-native contracts
// any txids on the above list will have their state gradually update on the gateway for ease of use!

export async function smartestStatus(
  request: Request,
  response: Response,
  next: NextFunction
) {
  response.json({ "status" : 200, "text" : "Smartest is the best."});
}

export async function smartestFetchState(
  request: Request,
  response: Response,
  next: NextFunction
) {
  try {
    const txId = request.params.id;
    if (reservedIds.includes(txId)) {
      // load the contract state using smartest if it's one of ours

      const state = await kohaku.readContract(arweave, txId);
      response.json(state);
    } else {
      // fetch like normal otherwise
      const rawTx = await transactionMapper.get({
        tx_id: txId,
      });

      // would need to parse json here before returning, and probably slow everything down a lot in order to dynamically load smartweave states

      response.json(R.pipe(R.dissoc("tag_count"), R.dissoc("tx_index"))(rawTx));
    }
  } catch (error) {
    return next(error);
  }
}
