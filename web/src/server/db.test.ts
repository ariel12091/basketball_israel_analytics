import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { PoolClient } from "pg";
import { SnapshotDatabase, Deadline, DeadlineError, BusyError, snapshotLimits } from "./db";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function client() {
  const query = vi.fn(async (_sql: string, _args?: unknown[]) => ({ rows: [] as unknown[] }));
  const release = vi.fn();
  return { query, release, value: { query, release } as unknown as PoolClient };
}
beforeEach(() => vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "performance"] }));
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); });

describe("SnapshotDatabase lifecycle (fake pool, real admission/cleanup code)", () => {
  it("validates pool and queue limits before any pool can be constructed", () => {
    expect(snapshotLimits({})).toEqual({ max: 2, queueMax: 20 });
    expect(snapshotLimits({ DB_QUEUE_MAX: "0" })).toEqual({ max: 2, queueMax: 0 });
    for (const value of ["NaN", "Infinity", "-1", "1.5", "1001", "", " "])
      expect(() => snapshotLimits({ DB_QUEUE_MAX: value })).toThrow();
    expect(() => new SnapshotDatabase({ connect: vi.fn() }, 1, NaN)).toThrow("DB_QUEUE_MAX");
  });
  it("commits success, returns exactly one client and clears its timers", async () => {
    const c = client();
    const pool = { connect: vi.fn(async () => c.value) };
    const db = new SnapshotDatabase(pool, 1);
    await expect(db.withSnapshot(new Deadline(), async q => {
      await q("SELECT $1", [7]);
      return 42;
    })).resolves.toBe(42);
    expect(c.query.mock.calls.map(([sql]) => sql)).toEqual([
      "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout = '3000ms'; SET LOCAL idle_in_transaction_session_timeout = '5s'",
      "SELECT $1", "COMMIT",
    ]);
    expect(c.release.mock.calls).toEqual([[]]);
    expect(vi.getTimerCount()).toBe(0);
  });

  for (const stage of ["BEGIN", "query", "callback", "COMMIT"] as const)
    it(`destroys a failed ${stage} client once and admits the next request`, async () => {
      const bad = client(), good = client();
      const failure = Error(stage);
      bad.query.mockImplementation(async sql => {
        if (sql.startsWith(stage) || stage === "query" && sql === "SELECT bad") throw failure;
        return { rows: [] };
      });
      const pool = { connect: vi.fn().mockResolvedValueOnce(bad.value).mockResolvedValue(good.value) };
      const db = new SnapshotDatabase(pool, 1);
      await expect(db.withSnapshot(new Deadline(), async q => {
        if (stage === "callback") throw failure;
        await q("SELECT bad", []);
      })).rejects.toBe(failure);
      expect(bad.release.mock.calls).toEqual([[true]]);
      await expect(db.withSnapshot(new Deadline(), async () => "recovered")).resolves.toBe("recovered");
      expect(good.release.mock.calls).toEqual([[]]);
      expect(vi.getTimerCount()).toBe(0);
    });

  it("recovers its admission slot when pool.connect rejects", async () => {
    const good = client();
    const pool = { connect: vi.fn().mockRejectedValueOnce(Error("connect failed")).mockResolvedValue(good.value) };
    const db = new SnapshotDatabase(pool, 1);
    await expect(db.withSnapshot(new Deadline(), async () => 0)).rejects.toThrow("connect failed");
    await expect(db.withSnapshot(new Deadline(), async () => 1)).resolves.toBe(1);
    expect(good.release).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("destroys a connection that arrives after acquisition timeout without starting a transaction", async () => {
    const pending = deferred<PoolClient>(), late = client(), good = client();
    const pool = { connect: vi.fn().mockReturnValueOnce(pending.promise).mockResolvedValue(good.value) };
    const db = new SnapshotDatabase(pool, 1);
    const callback = vi.fn(async () => 0);
    const request = expect(db.withSnapshot(new Deadline(), callback)).rejects.toBeInstanceOf(BusyError);
    await vi.advanceTimersByTimeAsync(1500);
    await request;
    pending.resolve(late.value);
    await vi.advanceTimersByTimeAsync(0);
    expect(callback).not.toHaveBeenCalled();
    expect(late.query).not.toHaveBeenCalled();
    expect(late.release.mock.calls).toEqual([[true]]);
    await expect(db.withSnapshot(new Deadline(), async () => 1)).resolves.toBe(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds the queue and removes expired work before it can acquire a client", async () => {
    const c = client(), hold = deferred<void>();
    const pool = { connect: vi.fn(async () => c.value) };
    const db = new SnapshotDatabase(pool, 1, 1);
    const first = db.withSnapshot(new Deadline(), async () => hold.promise);
    await vi.advanceTimersByTimeAsync(0);
    const expiredCallback = vi.fn(async () => 2);
    const waiting = expect(db.withSnapshot(new Deadline(), expiredCallback)).rejects.toBeInstanceOf(BusyError);
    await expect(db.withSnapshot(new Deadline(), async () => 3)).rejects.toBeInstanceOf(BusyError);
    await vi.advanceTimersByTimeAsync(500);
    await waiting;
    hold.resolve(); await first;
    expect(expiredCallback).not.toHaveBeenCalled();
    expect(pool.connect).toHaveBeenCalledTimes(1);
    await expect(db.withSnapshot(new Deadline(), async () => 4)).resolves.toBe(4);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("hands admission to waiting callers in order without exceeding its one active callback", async () => {
    const c = client(), hold = deferred<void>(), order: number[] = [];
    const db = new SnapshotDatabase({ connect: vi.fn(async () => c.value) }, 1, 2);
    const first = db.withSnapshot(new Deadline(), async () => { order.push(1); await hold.promise; });
    await vi.advanceTimersByTimeAsync(0);
    const second = db.withSnapshot(new Deadline(), async () => { order.push(2); });
    const third = db.withSnapshot(new Deadline(), async () => { order.push(3); });
    expect(order).toEqual([1]);
    hold.resolve();
    await Promise.all([first, second, third]);
    expect(order).toEqual([1, 2, 3]);
    expect(c.release.mock.calls).toEqual([[], [], []]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("discards late SQL results after the overall deadline and never commits or double-releases", async () => {
    const c = client(), pending = deferred<{ rows: unknown[] }>();
    c.query.mockImplementation(async sql => sql === "SELECT delayed" ? pending.promise : { rows: [] });
    const db = new SnapshotDatabase({ connect: vi.fn(async () => c.value) }, 1);
    const failed = expect(db.withSnapshot(new Deadline(1000), async q => q("SELECT delayed", []))).rejects.toBeInstanceOf(DeadlineError);
    await vi.advanceTimersByTimeAsync(1000);
    await failed;
    expect(c.release.mock.calls).toEqual([[true]]);
    pending.resolve({ rows: [{ late: true }] });
    await vi.advanceTimersByTimeAsync(0);
    expect(c.query.mock.calls.some(([sql]) => sql === "COMMIT")).toBe(false);
    expect(c.release.mock.calls).toEqual([[true]]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("leaves no wrapper timers or unreleased checkouts over repeated success/failure cycles", async () => {
    const c = client();
    const db = new SnapshotDatabase({ connect: vi.fn(async () => c.value) }, 1);
    for (let i = 0; i < 100; i++) {
      if (i % 2) await expect(db.withSnapshot(new Deadline(), async () => { throw Error("cycle"); })).rejects.toThrow("cycle");
      else await db.withSnapshot(new Deadline(), async () => i);
      expect(c.release).toHaveBeenCalledTimes(i + 1);
      expect(vi.getTimerCount()).toBe(0);
    }
    // Explicit ownership/timer invariant, not a claim about process heap or pg internals.
    expect(c.release.mock.calls.filter(args => args[0] === true)).toHaveLength(50);
  });
});
