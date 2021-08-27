interface WorkerReady {
  type: `worker:ready`;
  workerId?: any;
}

interface WorkerInfoLog {
  type: `log:info`;
  message: any;
  payload?: any;
}

interface WorkerProgressLog {
  type: `log:progress`;
  payload: any;
}

export type MessagesFromWorker =
  | WorkerProgressLog
  | WorkerInfoLog
  | WorkerReady;

interface ParentProgressPoll {
  type: `poll:progress`;
}

export type MessagesFromParent = ParentProgressPoll;
