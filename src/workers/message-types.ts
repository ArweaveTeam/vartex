interface WorkerReady {
  type: `worker:ready`;
  payload?: unknown;
}

interface WorkerErrorLog {
  type: `log:error`;
  message: string;
  payload?: unknown;
}

interface WorkerWarningLog {
  type: `log:warn`;
  message: string;
  payload?: unknown;
}

interface WorkerInfoLog {
  type: `log:info`;
  message: string;
  payload?: unknown;
}

interface WorkerProgressLog {
  type: `log:progress`;
  payload?: unknown;
}

interface WorkerNewBlockResponse {
  type: "block:new";
  payload?: unknown;
}

interface WorkerStatTxFlight {
  type: "stats:tx:flight";
  payload?: number;
}

export type MessagesFromWorker =
  | WorkerStatTxFlight
  | WorkerNewBlockResponse
  | WorkerProgressLog
  | WorkerErrorLog
  | WorkerWarningLog
  | WorkerInfoLog
  | WorkerReady;

interface ParentProgressPoll {
  type: `poll:progress`;
}

export type MessagesFromParent = ParentProgressPoll;
