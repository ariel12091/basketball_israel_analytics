import { expect, it } from "vitest";
import { WindowLimiter } from "./windowLimiter";
it("fails closed under recent-key floods without evicting active allowances", () => {
  const limiter = new WindowLimiter(60000, 2, 10);
  const check = (key: string, now = 0) => limiter.check([{ key, limit: 1 }], now);
  expect(check("a").ok).toBe(true); expect(check("b").ok).toBe(true);
  for (let i = 0; i < 100; i++) expect(check(String(i)).ok).toBe(false);
  expect(limiter.usage).toEqual({ keys: 2, events: 2 });
  expect(check("a").ok).toBe(false);
  expect(check("new", 60000).ok).toBe(true);
  expect(limiter.usage).toEqual({ keys: 1, events: 1 });
});
it("bounds total timestamp retention and permits recovery after expiry", () => {
  const limiter = new WindowLimiter(1000, 10, 3);
  const keys = [{ key: "a", limit: 100 }, { key: "b", limit: 100 }];
  expect(limiter.check(keys, 0).ok).toBe(true);
  expect(limiter.check(keys, 0).ok).toBe(false);
  expect(limiter.usage.events).toBe(2);
  expect(limiter.check(keys, 1000).ok).toBe(true);
});
it("deduplicates keys and does not charge any bucket for a denied request", () => {
  const limiter = new WindowLimiter(1000);
  expect(limiter.check([{ key: "ip", limit: 1 }, { key: "ip", limit: 1 }], 0).ok).toBe(true);
  expect(limiter.usage.events).toBe(1);
  expect(limiter.check([{ key: "new", limit: 1 }, { key: "ip", limit: 1 }], 0).ok).toBe(false);
  expect(limiter.check([{ key: "new", limit: 1 }], 0).ok).toBe(true);
});
