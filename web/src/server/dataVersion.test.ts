import { afterEach, expect, it, vi } from "vitest";
const { snapshot, query } = vi.hoisted(() => ({ snapshot: vi.fn(), query: vi.fn() }));
vi.mock("./db", async (original) => ({ ...(await original<typeof import("./db")>()), withSnapshot: snapshot }));
import { currentDataVersion, VERSION_SQL } from "./dataVersion";
afterEach(() => { snapshot.mockReset(); query.mockReset(); });
it("coalesces only active probes, isolates IL, and probes again after completion", async () => {
  let finish!: (v: { value: string }[]) => void;
  query.mockImplementation(() => new Promise((yes) => { finish = yes; }));
  snapshot.mockImplementation((_d, read) => read(query));
  const e = currentDataVersion("euroleague"), u = currentDataVersion("eurocup");
  expect(e).toBe(u); expect(snapshot).toHaveBeenCalledTimes(1);
  finish([{ value: "v1" }]); expect(await e).toBe("v1");
  query.mockResolvedValue([{ value: "v2" }]);
  expect(await currentDataVersion("eurocup")).toBe("v2");
  await currentDataVersion("il");
  expect(query.mock.calls.map((c) => c[0])).toEqual([VERSION_SQL.euro, VERSION_SQL.euro, VERSION_SQL.il]);
});
it("clears rejected probes so recovery can obtain a fresh version", async () => {
  snapshot.mockRejectedValueOnce(Error("offline")).mockResolvedValueOnce("recovered");
  await expect(currentDataVersion("il")).rejects.toThrow("offline");
  expect(await currentDataVersion("il")).toBe("recovered");
});
