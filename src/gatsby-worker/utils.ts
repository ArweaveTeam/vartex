export const isPromise = (object: any): object is PromiseLike<unknown> =>
  !!object &&
  (typeof object === `object` || typeof object === `function`) &&
  typeof object.then === `function`;

export const isRunning = (pid: number): boolean => {
  try {
    // "As a special case, a signal of 0 can be used to test for the existence of a process."
    // See https://nodejs.org/api/process.html#process_process_kill_pid_signal
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};
