interface WorkerReady {
  type: `worker:ready`;
}

interface WorkerInfoLog {
  type: `log:info`;
  message: string;
  payload?: unknown;
}

interface WorkerProgressLog {
  type: `log:progress`;
  payload: unknown;
}

interface WorkerNewBlockResponse {
  type: "block:new";
  payload: unknown;
}

interface WorkerStatTxFlight {
  type: "stats:tx:flight";
  payload: number;
}

export type MessagesFromWorker =
  | WorkerStatTxFlight
  | WorkerNewBlockResponse
  | WorkerProgressLog
  | WorkerInfoLog
  | WorkerReady;

interface ParentProgressPoll {
  type: `poll:progress`;
}

export type MessagesFromParent = ParentProgressPoll;
