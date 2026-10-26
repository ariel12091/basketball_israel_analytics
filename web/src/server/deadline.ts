// Wait up to 2 s for a pool slot: under a burst a short wait succeeds well
// inside the 5 s deadline, where 500 ms turned it into a 503 (r10, 100 users).
export const QUEUE_MS = 2000,
  // A measured cold TLS/login + BEGIN took 853 ms from this workstation.
  CONNECT_MS = 1500,
  SQL_MAX_MS = 3000;
export class DeadlineError extends Error {
  constructor() {
    super("Request deadline exceeded");
    this.name = "DeadlineError";
  }
}
export class BusyError extends Error {
  constructor() {
    super("Service busy; retry shortly");
    this.name = "BusyError";
  }
}
export class Deadline {
  private readonly end: number;
  /** sqlMaxMs: database time this request may use. Heavy readers that Shiny
   * runs under its 20 s statement timeout (onoff_compute, lineup readers) pass
   * that budget until Phase 8 makes them fast; everything else keeps 3 s. */
  constructor(
    totalMs = 5000,
    readonly sqlMaxMs = SQL_MAX_MS,
  ) {
    this.end = performance.now() + totalMs;
  }
  remaining() {
    return Math.max(0, this.end - performance.now());
  }
  expired() {
    return this.remaining() <= 0;
  }
}
