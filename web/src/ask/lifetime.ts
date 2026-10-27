// Own the turn's wall-clock budget and cancellation, including providers that
// fail to settle their promise after abort. Shared SQL readers keep their own
// bounded deadlines; cancelling one visitor must not cancel another's cache fill.
export function turnLifetime(ms: number, parent?: AbortSignal) {
  const controller = new AbortController();
  const abort = () => controller.abort(parent?.reason);
  parent?.addEventListener("abort", abort, { once: true });
  if (parent?.aborted) abort();
  const timer = setTimeout(
    () =>
      controller.abort(
        new DOMException("The answer timed out", "TimeoutError"),
      ),
    ms,
  );
  return {
    signal: controller.signal,
    wait<T>(work: () => Promise<T>): Promise<T> {
      const signal = controller.signal;
      let stopped: (() => void) | undefined;
      return new Promise<T>((resolve, reject) => {
        if (signal.aborted) {
          reject(signal.reason);
          return;
        }
        stopped = () => reject(signal.reason);
        signal.addEventListener("abort", stopped, { once: true });
        // Observe late rejection as well as completion, and detach the listener
        // when either work or cancellation wins.
        Promise.resolve()
          .then(() => {
            signal.throwIfAborted();
            return work();
          })
          .then(resolve, reject);
      }).finally(() => {
        if (stopped) signal.removeEventListener("abort", stopped);
      });
    },
    dispose() {
      clearTimeout(timer);
      parent?.removeEventListener("abort", abort);
      controller.abort(new DOMException("Turn finished", "AbortError"));
    },
  };
}
