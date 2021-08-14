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
    this.queue.sort(this.comparator.bind(this) as any);
  }

  entries() {
    // const entries = R.concat([], this.queue); // copy
    // let purgeCount = 0;
    // while (purgeCount < entries.length) {
    //   this.queue.pop();
    //   purgeCount += 1;
    // }
    return this.queue;
  }

  // purgeUndefs() {
  //   while (this.queue.length > 0 && !this.queue[0]) {
  //     !this.queue.shift();
  //   }
  // }

  removeItem(fn) {
    // const knife = R.findIndex((i) => i && fn(i))(this.queue);
    // delete this.queue[knife];

    this.queue = R.reject(fn)(this.queue);
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
