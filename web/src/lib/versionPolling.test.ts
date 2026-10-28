import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { pollVersions } from "./versionPolling";
let target: EventTarget;
let requests: { signal: AbortSignal; resolve: (value: Response) => void }[];
beforeEach(() => {
  vi.useFakeTimers(); target = new EventTarget(); requests = [];
  vi.stubGlobal("window", target);
  vi.stubGlobal("fetch", vi.fn((_url, opts) => new Promise<Response>((resolve) => requests.push({ signal: opts.signal, resolve }))));
});
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); vi.unstubAllGlobals(); });
const reply = (version: string) => Response.json({ leagues: [{ league: "il", version }] });
it("aborts an older poll and ignores its late response after the newer poll", async () => {
  const onValue = vi.fn(), cleanup = pollVersions(onValue);
  target.dispatchEvent(new Event("focus"));
  expect(requests[0].signal.aborted).toBe(true);
  requests[1].resolve(reply("new")); await vi.advanceTimersByTimeAsync(0);
  requests[0].resolve(reply("old")); await vi.advanceTimersByTimeAsync(0);
  expect(onValue).toHaveBeenCalledTimes(1);
  expect(onValue.mock.calls[0][0].leagues[0].version).toBe("new");
  cleanup(); expect(vi.getTimerCount()).toBe(0);
});
it("cleanup invalidates pending JSON, removes the focus listener and stops the timer", async () => {
  const onValue = vi.fn(), cleanup = pollVersions(onValue);
  let finish!: (value: unknown) => void;
  const response = reply("late");
  vi.spyOn(response, "json").mockImplementation(() => new Promise((yes) => { finish = yes; }));
  requests[0].resolve(response); await vi.advanceTimersByTimeAsync(0);
  cleanup(); expect(requests[0].signal.aborted).toBe(true);
  finish({ leagues: [{ league: "il", version: "late" }] }); await vi.advanceTimersByTimeAsync(120000);
  target.dispatchEvent(new Event("focus"));
  expect(onValue).not.toHaveBeenCalled(); expect(requests).toHaveLength(1);
  expect(vi.getTimerCount()).toBe(0);
});
