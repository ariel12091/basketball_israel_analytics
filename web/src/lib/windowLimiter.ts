export type LimitResult = { ok: true } | { ok: false; retryAfterSec: number };
/** Per-instance sliding windows. Fail closed at memory capacity rather than
 * evicting active clients (which would reset their allowance). */
export class WindowLimiter {
  private seen = new Map<string, number[]>();
  private events = 0;
  constructor(
    private windowMs: number,
    private maxKeys = 10_000,
    private maxEvents = 100_000,
  ) {
    for (const n of [windowMs, maxKeys, maxEvents])
      if (!Number.isSafeInteger(n) || n < 1)
        throw new Error("Invalid limiter budget");
  }
  get usage() {
    return { keys: this.seen.size, events: this.events };
  }
  private trim(key: string, now: number) {
    const old = this.seen.get(key);
    if (!old) return [];
    const recent = old.filter((t) => t > now - this.windowMs);
    this.events -= old.length - recent.length;
    if (recent.length) this.seen.set(key, recent);
    else this.seen.delete(key);
    return recent;
  }
  check(requests: { key: string; limit: number }[], now: number): LimitResult {
    const keys = new Map<string, number>();
    for (const { key, limit } of requests) {
      if (key.length > 256)
        return { ok: false, retryAfterSec: Math.ceil(this.windowMs / 1000) };
      if (!Number.isSafeInteger(limit) || limit < 1)
        throw new Error("Invalid request limit");
      keys.set(key, Math.min(keys.get(key) ?? limit, limit));
    }
    let wait = 0;
    for (const [key, limit] of keys) {
      const recent = this.trim(key, now);
      if (recent.length >= limit)
        wait = Math.max(wait, recent[0] + this.windowMs - now);
    }
    if (wait > 0)
      return { ok: false, retryAfterSec: Math.max(1, Math.ceil(wait / 1000)) };
    const newKeys = () =>
      [...keys.keys()].filter((key) => !this.seen.has(key)).length;
    if (
      this.seen.size + newKeys() > this.maxKeys ||
      this.events + keys.size > this.maxEvents
    ) {
      for (const key of this.seen.keys()) this.trim(key, now);
      if (
        this.seen.size + newKeys() > this.maxKeys ||
        this.events + keys.size > this.maxEvents
      ) {
        let earliest = now + this.windowMs;
        for (const times of this.seen.values())
          earliest = Math.min(earliest, times[0] + this.windowMs);
        return {
          ok: false,
          retryAfterSec: Math.max(1, Math.ceil((earliest - now) / 1000)),
        };
      }
    }
    for (const key of keys.keys()) {
      const times = this.seen.get(key) ?? [];
      times.push(now);
      this.seen.set(key, times);
      this.events++;
    }
    return { ok: true };
  }
}
export function createLimiter(
  max: number | (() => number),
  windowMs: number,
  maxKeys = 10_000,
) {
  const limiter = new WindowLimiter(windowMs, maxKeys);
  return (keys: string[], now: number): LimitResult =>
    limiter.check(
      keys.map((key) => ({
        key,
        limit: typeof max === "function" ? max() : max,
      })),
      now,
    );
}
