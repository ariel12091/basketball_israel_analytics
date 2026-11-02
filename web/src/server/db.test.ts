import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { PoolClient } from "pg";
import { SnapshotDatabase, Deadline, DeadlineError, BusyError, snapshotLimits } from "./db";
import { QUEUE_MS, HEAVY_QUEUE_MS } from "./deadline";

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
    expect(snapshotLimits({})).toEqual({ max: 2, queueMax: 20, heavyMax: 1 });
    expect(snapshotLimits({ DB_QUEUE_MAX: "0" })).toEqual({ max: 2, queueMax: 0, heavyMax: 1 });
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
    await vi.advanceTimersByTimeAsync(QUEUE_MS);
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

describe("SnapshotDatabase heavy lane (load test 2026-11-02)", () => {
  it("defaults the heavy cap to 2, keeping a slot for quick reads in small pools", () => {
    expect(snapshotLimits({ DB_POOL_MAX: "6" }).heavyMax).toBe(2);
    expect(snapshotLimits({ DB_POOL_MAX: "2" }).heavyMax).toBe(1);
    expect(snapshotLimits({ DB_POOL_MAX: "1" }).heavyMax).toBe(1);
    expect(snapshotLimits({ DB_POOL_MAX: "6", DB_HEAVY_MAX: "3" }).heavyMax).toBe(3);
    expect(() => snapshotLimits({ DB_POOL_MAX: "6", DB_HEAVY_MAX: "7" })).toThrow("DB_HEAVY_MAX");
    expect(() => snapshotLimits({ DB_POOL_MAX: "6", DB_HEAVY_MAX: "" })).toThrow("Empty");
  });

  it("holds a second heavy read while its lane is full, and still runs quick reads", async () => {
    const c = client(), hold = deferred<void>(), ran: string[] = [];
    const db = new SnapshotDatabase({ connect: vi.fn(async () => c.value) }, 3, 5, 1);
    const heavyA = db.withSnapshot(new Deadline(25000, 20000), async () => { ran.push("heavyA"); await hold.promise; }, { heavy: true });
    await vi.advanceTimersByTimeAsync(0);
    const heavyB = db.withSnapshot(new Deadline(25000, 20000), async () => { ran.push("heavyB"); }, { heavy: true });
    const quick = db.withSnapshot(new Deadline(), async () => { ran.push("quick"); });
    await quick;
    expect(ran).toEqual(["heavyA", "quick"]);
    hold.resolve();
    await Promise.all([heavyA, heavyB]);
    expect(ran).toEqual(["heavyA", "quick", "heavyB"]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("passes over a waiting heavy read to admit the quick read queued behind it", async () => {
    const c = client(), holdHeavy = deferred<void>(), holdQuick = deferred<void>(), ran: string[] = [];
    const db = new SnapshotDatabase({ connect: vi.fn(async () => c.value) }, 2, 5, 1);
    const heavyA = db.withSnapshot(new Deadline(25000, 20000), async () => { ran.push("heavyA"); await holdHeavy.promise; }, { heavy: true });
    const quickB = db.withSnapshot(new Deadline(), async () => { ran.push("quickB"); await holdQuick.promise; });
    await vi.advanceTimersByTimeAsync(0);
    const heavyC = db.withSnapshot(new Deadline(25000, 20000), async () => { ran.push("heavyC"); }, { heavy: true });
    const quickD = db.withSnapshot(new Deadline(), async () => { ran.push("quickD"); });
    holdQuick.resolve();
    await Promise.all([quickB, quickD]);
    expect(ran).toEqual(["heavyA", "quickB", "quickD"]);
    holdHeavy.resolve();
    await Promise.all([heavyA, heavyC]);
    expect(ran).toEqual(["heavyA", "quickB", "quickD", "heavyC"]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("lets a heavy read wait longer than a quick read before refusing it", async () => {
    const c = client(), hold = deferred<void>();
    const db = new SnapshotDatabase({ connect: vi.fn(async () => c.value) }, 1, 5, 1);
    const running = db.withSnapshot(new Deadline(25000, 20000), async () => { await hold.promise; });
    await vi.advanceTimersByTimeAsync(0);
    const quick = db.withSnapshot(new Deadline(), async () => 1);
    const heavy = db.withSnapshot(new Deadline(25000, 20000), async () => 2, { heavy: true });
    const quickResult = quick.catch((e) => e);
    const heavyResult = heavy.then((v) => v, (e) => e);
    await vi.advanceTimersByTimeAsync(QUEUE_MS + 1);
    expect(await quickResult).toBeInstanceOf(BusyError);
    await vi.advanceTimersByTimeAsync(HEAVY_QUEUE_MS - QUEUE_MS - 500);
    hold.resolve();
    expect(await heavyResult).toBe(2);
    await running;
    expect(vi.getTimerCount()).toBe(0);
  });
});
