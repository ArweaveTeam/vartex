interface ITaskQueueNode<ValueType> {
  value: ValueType;
  next?: ITaskQueueNode<ValueType>;
  prev?: ITaskQueueNode<ValueType>;
}

/**
 * Task queue implemented with doubly linked list
 */
export class TaskQueue<ValueType> {
  private head?: ITaskQueueNode<ValueType>;
  private tail?: ITaskQueueNode<ValueType>;

  *[Symbol.iterator](): Iterator<ITaskQueueNode<ValueType>> {
    let currentHead = this.head;
    while (currentHead) {
      yield currentHead;
      currentHead = currentHead.next;
    }
  }

  /**
   * Puts new task at the end of the list
   * @param task Task to add to the queue
   */
  enqueue(task: ValueType): void {
    const newNode: ITaskQueueNode<ValueType> = {
      value: task,
    };
    if (this.tail) {
      this.tail.next = newNode;
      newNode.prev = this.tail;
    } else {
      this.head = newNode;
    }

    this.tail = newNode;
  }

  /**
   * Remove a task node from the queue
   * @param taskNode Queue's node to remove
   */
  remove(taskNode: ITaskQueueNode<ValueType>): void {
    if (taskNode === this.head) {
      this.head = taskNode.next;
      if (this.head) {
        this.head.prev = undefined;
      } else {
        // if we don't have head, we also don't have tail
        this.tail = undefined;
      }
    } else {
      if (taskNode === this.tail) {
        this.tail = taskNode.prev;
      } else {
        // if node is not the tail then it will have .next
        taskNode.next!.prev = taskNode.prev;
      }
      // if node is not the head then it will have .prev
      taskNode.prev!.next = taskNode.next;
    }
  }
}
