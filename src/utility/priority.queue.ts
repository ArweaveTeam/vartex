import { types as CassandraTypes } from "cassandra-driver";
import { head, isEmpty as rIsEmpty } from "rambda";
import { toLong } from "../database/cassandra.database";
import * as R from "rambda";

export interface ITxIncoming {
  type: "tx-incoming";
  txIndex: CassandraTypes.Long;
  txId: string;
  next: (fresolve: unknown) => void;
}

export interface ITxImport {
  type: "tx";
  txIndex: CassandraTypes.Long;
  txId: string;
  height: CassandraTypes.Long;
  fresolve: () => void;
  callback: () => void;
}

type QueueItem = ITxIncoming | ITxImport;

type PriorityComparator = (a: QueueItem, b: QueueItem) => number;

export default class PriorityQueue {
  public queue: Array<QueueItem>;
  public comparator: PriorityComparator;

  constructor(cmp: PriorityComparator) {
    this.comparator = cmp;
    this.queue = [];
  }

  sortQueue(): void {
    this.queue.sort(this.comparator);
  }

  entries(): QueueItem[] {
    // eslint-disable-next-line unicorn/prefer-spread
    return this.queue.slice(0); // https://stackoverflow.com/a/21514254
  }

  // just return the latest, sort to be sure
  peek(): QueueItem {
    this.sortQueue();
    return head(this.queue);
  }

  // remove the head item from the queue
  pop(): void {
    this.queue.shift();
    this.sortQueue;
  }

  enqueue(item: QueueItem): void {
    this.queue.push(item);
    this.sortQueue();
  }

  isEmpty(): boolean {
    return rIsEmpty(this.queue);
  }

  getSize(): number {
    return this.queue.length;
  }

  // hacky solution for tx imports
  hasNoneLt(height: CassandraTypes.Long): boolean {
    const valsLt = R.filter(
      (item: QueueItem) =>
        item.type === "tx" && toLong(item.height).lessThan(height)
    )(this.queue);

    const answer = R.isEmpty(valsLt);

    return answer;
  }
}
