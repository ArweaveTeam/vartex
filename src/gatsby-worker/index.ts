import { fork, ChildProcess } from "node:child_process";

import { TaskQueue } from "./task-queue";
import {
  EXECUTE,
  END,
  ERROR,
  RESULT,
  CUSTOM_MESSAGE,
  ParentMessageUnion,
  ChildMessageUnion,
} from "./types";

interface IWorkerOptions {
  numWorkers?: number;
  env?: Record<string, string>;
  logFilter?: (data: string) => boolean;
  workerPoolPrefix?: string;
}

type WrapReturnOfAFunctionInAPromise<
  FunctionThatDoesNotReturnAPromise extends (...arguments_: Array<any>) => any
> = (
  ...a: Parameters<FunctionThatDoesNotReturnAPromise>
) => Promise<ReturnType<FunctionThatDoesNotReturnAPromise>>;

// gatsby-worker will make sync function async, so to keep proper types we need to adjust types so all functions
// on worker pool are async
type EnsureFunctionReturnsAPromise<MaybeFunction> = MaybeFunction extends (
  ...arguments_: Array<any>
) => Promise<any>
  ? MaybeFunction
  : MaybeFunction extends (...arguments_: Array<any>) => any
  ? WrapReturnOfAFunctionInAPromise<MaybeFunction>
  : never;

type WrapReturnInArray<MaybeFunction> = MaybeFunction extends (
  ...arguments_: Array<any>
) => any
  ? (...a: Parameters<MaybeFunction>) => Array<ReturnType<MaybeFunction>>
  : never;

export type CreateWorkerPoolType<ExposedFunctions> = WorkerPool & {
  [FunctionName in keyof ExposedFunctions]: EnsureFunctionReturnsAPromise<
    ExposedFunctions[FunctionName]
  >;
} & {
  all: {
    [FunctionName in keyof ExposedFunctions]: WrapReturnInArray<
      EnsureFunctionReturnsAPromise<ExposedFunctions[FunctionName]>
    >;
  };
};

const childWrapperPath = process.cwd() + `/src/gatsby-worker/child`;

class TaskInfo<T> {
  functionName: T;
  args: Array<any>;
  assignedToWorker?: IWorkerInfo<T>;
  promise: Promise<any>;
  resolve!: (r: any) => void;
  reject!: (e: Error) => void;

  constructor(options: {
    functionName: T;
    args: Array<any>;
    assignedToWorker?: IWorkerInfo<T>;
  }) {
    this.functionName = options.functionName;
    this.args = options.args;
    this.assignedToWorker = options.assignedToWorker;
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

interface IWorkerInfo<T> {
  workerId: number;
  worker: ChildProcess;
  exitedPromise: Promise<{
    code: number | null;
    signal: NodeJS.Signals | null;
  }>;
  currentTask?: TaskInfo<T>;
}

export interface IPublicWorkerInfo {
  workerId: number;
}

/**
 * Worker pool is a class that allow you to queue function execution across multiple
 * child processes, in order to parallelize work. It accepts absolute path to worker module
 * and will expose exported function of that module as properties on WorkerPool instance.
 *
 * Worker pool allows queueing execution of a function on all workers (via `.all` property)
 * as well as distributing execution across workers (via `.single` property)
 */
export class WorkerPool<
  WorkerModuleExports = Record<string, unknown>,
  MessagesFromParent = unknown,
  MessagesFromChild = MessagesFromParent
> {
  /**
   * Schedule task execution on all workers. Useful for setting up workers
   */
  all: {
    [FunctionName in keyof WorkerModuleExports]: WrapReturnInArray<
      EnsureFunctionReturnsAPromise<WorkerModuleExports[FunctionName]>
    >;
  };

  /**
   * Schedule task execution on single worker. Useful to distribute tasks between multiple workers.
   */
  single: {
    [FunctionName in keyof WorkerModuleExports]: EnsureFunctionReturnsAPromise<
      WorkerModuleExports[FunctionName]
    >;
  };

  private workerPoolPrefix: string = "";
  private workers: Array<IWorkerInfo<keyof WorkerModuleExports>> = [];
  private taskQueue = new TaskQueue<TaskInfo<keyof WorkerModuleExports>>();
  private idleWorkers: Set<IWorkerInfo<keyof WorkerModuleExports>> = new Set();
  private listeners: Array<
    (message: MessagesFromChild, workerId: string) => void
  > = [];

  constructor(private workerPath: string, private options?: IWorkerOptions) {
    this.workerPoolPrefix = options.workerPoolPrefix || "";
    const single: Partial<WorkerPool<WorkerModuleExports>["single"]> = {};
    const all: Partial<WorkerPool<WorkerModuleExports>["all"]> = {};

    {
      // we don't need to retain these
      import(workerPath).then((module: WorkerModuleExports) => {
        const exportNames = Object.keys(module) as Array<
          keyof WorkerModuleExports
        >;

        for (const exportName of exportNames) {
          if (typeof module[exportName] !== `function`) {
            // We only expose functions. Exposing other types
            // would require additional handling which doesn't seem
            // worth supporting given that consumers can just access
            // those via require/import instead of WorkerPool interface.
            continue;
          }

          single[exportName] = this.scheduleWorkSingle.bind(
            this,
            exportName
          ) as WorkerPool<WorkerModuleExports>["single"][typeof exportName];

          all[exportName] = this.scheduleWorkAll.bind(
            this,
            exportName
          ) as unknown as WorkerPool<WorkerModuleExports>["all"][typeof exportName];
        }
      });

      this.single = single as WorkerPool<WorkerModuleExports>["single"];
      this.all = all as WorkerPool<WorkerModuleExports>["all"];
      this.startAll();
    }
  }
  private startAll(): void {
    const options = this.options;
    for (let workerId = 1; workerId <= (options?.numWorkers ?? 1); workerId++) {
      const worker = fork(childWrapperPath, {
        cwd: process.cwd(),
        env: {
          ...process.env,
          ...(options?.env ?? {}),
          GATSBY_WORKER_ID: workerId.toString(),
          GATSBY_WORKER_MODULE_PATH: this.workerPath,
        },
        execArgv: process.execArgv,
        silent: true,
      });

      worker.stdout.on(
        "data",
        (data) => options.logFilter(data) && process.stdout.write(data + "\n")
      );
      worker.stderr.on(
        "data",
        (data) => options.logFilter(data) && process.stdout.write(data + "\n")
      );

      const workerInfo: IWorkerInfo<keyof WorkerModuleExports> = {
        workerId,
        worker,
        exitedPromise: new Promise((resolve) => {
          worker.on(`exit`, (code, signal) => {
            if (workerInfo.currentTask) {
              console.error("Received code:", code, "signal:", signal);
              // worker exited without finishing a task
              workerInfo.currentTask.reject(
                new Error(`Worker exited before finishing task`)
              );
            }
            // remove worker from list of workers
            this.workers.splice(this.workers.indexOf(workerInfo), 1);
            resolve({ code, signal });
          });
        }),
      };

      worker.on(`message`, (message: ChildMessageUnion) => {
        switch (message[0]) {
          case RESULT: {
            if (!workerInfo.currentTask) {
              throw new Error(
                `Invariant: gatsby-worker received execution result, but it wasn't expecting it.`
              );
            }
            const task = workerInfo.currentTask;
            workerInfo.currentTask = undefined;
            this.checkForWork(workerInfo);
            task.resolve(message[1]);

            break;
          }
          case ERROR: {
            if (!workerInfo.currentTask) {
              throw new Error(
                `Invariant: gatsby-worker received execution rejection, but it wasn't expecting it.`
              );
            }

            let error = message[4];

            if (error !== null && typeof error === `object`) {
              const extra = error;

              const NativeCtor = (global as any)[message[1]];
              const Ctor =
                typeof NativeCtor === `function` ? NativeCtor : Error;

              error = new Ctor(message[2]);
              // @ts-ignore type doesn't exist on Error, but that's what jest-worker does for errors :shrug:
              error.type = message[1];
              error.stack = message[3];

              for (const key in extra) {
                if (Object.prototype.hasOwnProperty.call(extra, key)) {
                  (error as any)[key] = (extra as any)[key];
                }
              }
            }

            const task = workerInfo.currentTask;
            workerInfo.currentTask = undefined;
            this.checkForWork(workerInfo);
            task.reject(error);

            break;
          }
          case CUSTOM_MESSAGE: {
            for (const listener of this.listeners) {
              listener(
                message[1] as MessagesFromChild,
                this.workerPoolPrefix + ":" + workerId
              );
            }

            break;
          }
          // No default
        }
      });

      this.workers.push(workerInfo);
      this.idleWorkers.add(workerInfo);
    }
  }

  /**
   * Kills worker processes and rejects and ongoing or pending tasks.
   * @returns Array of promises for each worker that will resolve once worker did exit.
   */
  end(): Array<Promise<number | null>> {
    const results = this.workers.map(async (workerInfo) => {
      // tell worker to end gracefully
      const endMessage: ParentMessageUnion = [END];

      workerInfo.worker.send(endMessage);

      // force exit if worker doesn't exit gracefully quickly
      const forceExitTimeout = setTimeout(() => {
        workerInfo.worker.kill(`SIGKILL`);
      }, 1000);

      const exitResult = await workerInfo.exitedPromise;

      clearTimeout(forceExitTimeout);

      return exitResult.code;
    });

    Promise.all(results).then(() => {
      // make sure we fail queued tasks as well
      for (const taskNode of this.taskQueue) {
        taskNode.value.reject(new Error(`Worker exited before finishing task`));
      }
      this.workers = [];
      this.idleWorkers = new Set();
    });

    return results;
  }

  /**
   * Kills all running worker processes and spawns a new pool of processes
   */
  async restart(): Promise<void> {
    await Promise.all(this.end());
    this.startAll();
  }

  getWorkerInfo(): Array<IPublicWorkerInfo> {
    return this.workers.map((worker) => {
      return { workerId: worker.workerId };
    });
  }

  private checkForWork<T extends keyof WorkerModuleExports>(
    workerInfo: IWorkerInfo<T>
  ): void {
    // check if there is task in queue
    for (const taskNode of this.taskQueue) {
      const task = taskNode.value;
      if (!task.assignedToWorker || task.assignedToWorker === workerInfo) {
        this.doWork(task, workerInfo);
        this.taskQueue.remove(taskNode);

        return;
      }
    }

    // no task found, so just marking worker as idle
    this.idleWorkers.add(workerInfo);
  }

  private doWork<T extends keyof WorkerModuleExports>(
    taskInfo: TaskInfo<T>,
    workerInfo: IWorkerInfo<T>
  ): void {
    // block worker
    workerInfo.currentTask = taskInfo;
    this.idleWorkers.delete(workerInfo);

    const message: ParentMessageUnion = [
      EXECUTE,
      taskInfo.functionName,
      taskInfo.args,
    ];
    workerInfo.worker.send(message);
  }

  private scheduleWork<T extends keyof WorkerModuleExports>(
    taskInfo: TaskInfo<T>
  ): Promise<unknown> {
    let workerToExecuteTaskNow:
      | IWorkerInfo<keyof WorkerModuleExports>
      | undefined;

    if (taskInfo.assignedToWorker) {
      if (this.idleWorkers.has(taskInfo.assignedToWorker)) {
        workerToExecuteTaskNow = taskInfo.assignedToWorker;
      }
    } else {
      workerToExecuteTaskNow = this.idleWorkers.values().next().value;
    }

    if (workerToExecuteTaskNow) {
      this.doWork(taskInfo, workerToExecuteTaskNow);
    } else {
      this.taskQueue.enqueue(taskInfo);
    }

    return taskInfo.promise;
  }

  private scheduleWorkSingle<T extends keyof WorkerModuleExports>(
    functionName: T,
    ...arguments_: Array<unknown>
  ): Promise<unknown> {
    return this.scheduleWork(new TaskInfo({ functionName, args: arguments_ }));
  }

  private scheduleWorkAll<T extends keyof WorkerModuleExports>(
    functionName: T,
    ...arguments_: Array<unknown>
  ): Array<Promise<unknown>> {
    return this.workers.map((workerInfo) =>
      this.scheduleWork(
        new TaskInfo({
          assignedToWorker: workerInfo,
          functionName,
          args: arguments_,
        })
      )
    );
  }

  onMessage(
    listener: (message: MessagesFromChild, workerId: string) => void
  ): void {
    this.listeners.push(listener);
  }

  sendMessage(message: MessagesFromParent, workerId: number): void {
    // console.error("send message", msg);
    const worker = this.workers[workerId - 1];
    if (!worker) {
      throw new Error(`There is no worker with "${workerId}" id.`);
    }

    const poolMessage = [CUSTOM_MESSAGE, message];
    worker.worker.send(poolMessage);
  }
}

export * from "./child";
