import { types as CassandraTypes } from "cassandra-driver";
import { append, head, isEmpty as rIsEmpty, slice, sort } from "rambda";
import { toLong } from "../database/cassandra.database";
import * as R from "rambda";

export default class PriorityQueue {
  public queue: Array<any>;
  public comparator: (a: any, b: any) => boolean;

  constructor(cmp) {
    this.comparator = cmp;
    this.queue = [];
  }

  sortQueue() {
    this.queue.sort(this.comparator as any);
  }

  entries() {
    // eslint-disable-next-line unicorn/prefer-spread
    return this.queue.slice(0); // https://stackoverflow.com/a/21514254
  }

  // just return the latest, sort to be sure
  peek(): any {
    this.sortQueue();
    return head(this.queue);
  }

  // remove the head item from the queue
  pop(): void {
    this.queue.shift();
    this.sortQueue;
  }

  enqueue(item: any): void {
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
      (item: { height: number | CassandraTypes.Long | string }) =>
        toLong(item.height).lessThan(height)
    )(this.queue);

    const answer = R.isEmpty(valsLt);

    return answer;
  }
}
