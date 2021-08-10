import { types as CassandraTypes } from "cassandra-driver";
import { append, head, isEmpty as rIsEmpty, slice, sort } from "rambda";
import * as R from "rambda";

export default class PriorityQueue {
  protected queue = [];
  protected comparator: (a: any, b: any) => boolean;
  constructor(cmp) {
    this.comparator = cmp;
    this.hasNoneLt = this.hasNoneLt.bind(this);
  }
  sortQueue() {
    this.queue = sort(this.comparator as any, this.queue);
  }
  // just return the latest, sort to be sure
  peek(): any {
    this.sortQueue();
    return head(this.queue);
  }

  // remove the head item from the queue
  pop(): void {
    this.queue = slice(1, Number.POSITIVE_INFINITY)(this.queue);
    this.sortQueue;
  }

  enqueue(item: any): void {
    this.queue = append(item, this.queue);
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
    const answer = R.isEmpty(
        R.filter((item) => height.lessThan(item.height)),
    );

    return answer;
  }
}
