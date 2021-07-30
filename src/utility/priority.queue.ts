import { append, head, isEmpty as rIsEmpty, slice, sort } from 'rambda';

export default class PriorityQueue {
  protected queue = [];
  protected comparator: (a: any, b: any) => boolean;
  constructor(cmp) {
    this.comparator = cmp;
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
    this.queue = slice(1, Infinity)(this.queue);
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
}
