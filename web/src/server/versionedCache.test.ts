import { afterEach, expect, it, vi } from "vitest";
const { probe, hint } = vi.hoisted(() => ({ probe: vi.fn(async () => "v1"), hint: vi.fn((): string | undefined => undefined) }));
vi.mock("./dataVersion", () => ({ currentDataVersion: probe, requestedVersion: hint }));
import { VersionedCache } from "./versionedCache";
type Value = { version: string; text: string; partial?: boolean };
const value = (version = "v1", text = "data"): Value => ({ version, text });
const make = (count = 2, bytes = 1000, ttl = 60000) => new VersionedCache<Value>(count, (v) => v.version, (v) => !v.partial, bytes, ttl);
function deferred<T>() {
  let resolve!: (v: T) => void, reject!: (e: Error) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
afterEach(() => { vi.restoreAllMocks(); probe.mockReset().mockResolvedValue("v1"); hint.mockReset().mockReturnValue(undefined); });

it("a request naming a version reuses that version's answer without a probe, never another's", async () => {
  const cache = make(), load = vi.fn(async () => value());
  await cache.get("il", "context", load);
  hint.mockReturnValue("v1");
  await cache.get("il", "context", load);
  expect(load).toHaveBeenCalledTimes(1);
  expect(probe).toHaveBeenCalledTimes(1);
  // A stale or newer name misses; the fresh read is returned but not filed under it.
  hint.mockReturnValue("v0"); load.mockResolvedValue(value("v2"));
  expect((await cache.get("il", "context", load)).version).toBe("v2");
  expect((await cache.get("il", "context", load)).version).toBe("v2");
  expect(load).toHaveBeenCalledTimes(3);
});

it("reuses a current answer but a publication change reloads before TTL expiry", async () => {
  const cache = make(), load = vi.fn(async () => value());
  await cache.get("il", "context", load);
  await cache.get("il", "context", load);
  expect(load).toHaveBeenCalledTimes(1);
  probe.mockResolvedValue("v2"); load.mockResolvedValue(value("v2"));
  expect((await cache.get("il", "context", load)).version).toBe("v2");
  expect(load).toHaveBeenCalledTimes(2);
  expect(probe).toHaveBeenCalledTimes(3);
});
it("coalesces concurrent loads of the same version/context", async () => {
  const cache = make(), pending = deferred<Value>(), load = vi.fn(() => pending.promise);
  const first = cache.getAtVersion("il", "v1", "a", load);
  const second = cache.getAtVersion("il", "v1", "a", load);
  expect(first).toBe(second);
  pending.resolve(value()); await first;
  expect(load).toHaveBeenCalledTimes(1);
});
it("late rejection of an evicted load cannot erase its replacement", async () => {
  const cache = make(1), old = deferred<Value>();
  const failed = expect(cache.getAtVersion("il", "v1", "a", () => old.promise)).rejects.toThrow("old");
  await cache.getAtVersion("il", "v1", "b", async () => value());
  const replacement = vi.fn(async () => value("v1", "replacement"));
  await cache.getAtVersion("il", "v1", "a", replacement);
  old.reject(Error("old")); await failed;
  expect((await cache.getAtVersion("il", "v1", "a", replacement)).text).toBe("replacement");
  expect(replacement).toHaveBeenCalledTimes(1);
  expect(cache.usage.entries).toBe(1);
});
it("late success of an evicted load cannot alter retained bytes", async () => {
  const cache = make(1), old = deferred<Value>();
  const pending = cache.getAtVersion("il", "v1", "a", () => old.promise);
  await cache.getAtVersion("il", "v1", "b", async () => value());
  const before = cache.usage;
  old.resolve(value("v1", "x".repeat(2000))); await pending;
  expect(cache.usage).toEqual(before);
});
it("expires settled answers and does not expire a pending shared load", async () => {
  let now = 0; vi.spyOn(Date, "now").mockImplementation(() => now);
  const cache = make(2, 1000, 10), pending = deferred<Value>(), load = vi.fn(() => pending.promise);
  const first = cache.getAtVersion("il", "v1", "a", load);
  now = 100;
  expect(cache.getAtVersion("il", "v1", "a", load)).toBe(first);
  pending.resolve(value()); await first;
  now = 111;
  await cache.getAtVersion("il", "v1", "a", load);
  expect(load).toHaveBeenCalledTimes(2);
});
it("enforces encoded-byte and entry budgets over diverse contexts", async () => {
  const cache = make(4, 180);
  for (let i = 0; i < 100; i++) {
    await cache.getAtVersion("il", "v1", String(i), async () => value("v1", "x".repeat(80)));
    expect(cache.usage.bytes).toBeLessThanOrEqual(180);
    expect(cache.usage.entries).toBeLessThanOrEqual(4);
  }
});
it("oversized, partial, unknown and version-mismatched answers are not retained", async () => {
  for (const v of [value("v1", "x".repeat(1000)), { ...value(), partial: true }, value("v2")]) {
    const cache = make(2, 100), load = vi.fn(async () => v);
    await cache.getAtVersion("il", "v1", "a", load); await cache.getAtVersion("il", "v1", "a", load);
    expect(load).toHaveBeenCalledTimes(2); expect(cache.usage.entries).toBe(0);
  }
  const cache = make(), load = vi.fn(async () => value("unknown"));
  await cache.getAtVersion("il", "unknown", "a", load); await cache.getAtVersion("il", "unknown", "a", load);
  expect(load).toHaveBeenCalledTimes(2);
});
it("failed loads and failed publication probes cannot serve stale data", async () => {
  const cache = make(), load = vi.fn(async () => value());
  load.mockRejectedValueOnce(Error("reader failed"));
  await expect(cache.get("il", "a", load)).rejects.toThrow("reader failed");
  await cache.get("il", "a", load);
  probe.mockRejectedValueOnce(Error("version unavailable"));
  await expect(cache.get("il", "a", load)).rejects.toThrow("version unavailable");
  expect(load).toHaveBeenCalledTimes(2);
});
it("isolates all competitions even when their version and context are identical", async () => {
  const cache = make(4), load = vi.fn(async () => value());
  for (const league of ["il", "euroleague", "eurocup"] as const) await cache.getAtVersion(league, "v1", "same", load);
  expect(load).toHaveBeenCalledTimes(3);
});
