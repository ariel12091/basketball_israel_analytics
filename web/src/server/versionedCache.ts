import "server-only";
import type { League } from "../rules/gameLogs";
import { currentDataVersion } from "./dataVersion";

type Entry<T> = { promise: Promise<T>; expires: number; bytes: number };
/** Version namespaces, bounded serialized retention and ownership-safe single flight.
 * Encoded bytes are a retention budget, not a measurement of V8 heap use. */
export class VersionedCache<T> {
  private entries = new Map<string, Entry<T>>();
  private bytes = 0;
  get usage() {
    return { entries: this.entries.size, bytes: this.bytes };
  }
  constructor(
    private maxEntries: number,
    private versionOf: (value: T) => string,
    private cacheable: (value: T) => boolean = () => true,
    private maxBytes = 16 * 1024 * 1024,
    private ttlMs = 60_000,
  ) {
    for (const value of [maxEntries, maxBytes, ttlMs])
      if (!Number.isSafeInteger(value) || value < 1)
        throw new Error("Invalid cache budget");
  }
  async get(scope: League, key: string, load: () => Promise<T>): Promise<T> {
    return this.getAtVersion(scope, await currentDataVersion(scope), key, load);
  }
  getAtVersion(
    scope: League,
    version: string,
    key: string,
    load: () => Promise<T>,
  ): Promise<T> {
    if (!version || version === "unknown") return load();
    const id = JSON.stringify([scope, version, key]);
    const now = Date.now();
    for (const [k, entry] of this.entries)
      if (entry.expires <= now) this.remove(k, entry);
    const hit = this.entries.get(id);
    if (hit) {
      this.entries.delete(id);
      this.entries.set(id, hit);
      return hit.promise;
    }
    while (this.entries.size >= this.maxEntries) this.evict();
    const entry: Entry<T> = {
      expires: Infinity,
      bytes: 0,
      promise: undefined!,
    };
    entry.promise = Promise.resolve()
      .then(load)
      .then((value) => {
        // An evicted/replaced completion never changes the replacement's budget.
        if (this.entries.get(id) !== entry) return value;
        if (this.versionOf(value) !== version || !this.cacheable(value)) {
          this.remove(id, entry);
          return value;
        }
        let bytes: number;
        try {
          bytes = Buffer.byteLength(JSON.stringify(value), "utf8");
        } catch {
          this.remove(id, entry);
          return value;
        }
        if (bytes > this.maxBytes) {
          this.remove(id, entry);
          return value;
        }
        entry.bytes = bytes;
        this.bytes += bytes;
        entry.expires = Date.now() + this.ttlMs;
        while (this.bytes > this.maxBytes) this.evict();
        return value;
      })
      .catch((error) => {
        this.remove(id, entry);
        throw error;
      });
    this.entries.set(id, entry);
    return entry.promise;
  }
  private remove(id: string, entry: Entry<T>) {
    if (this.entries.get(id) !== entry) return;
    this.entries.delete(id);
    this.bytes -= entry.bytes;
  }
  private evict() {
    const first = this.entries.entries().next().value;
    if (first) this.remove(first[0], first[1]);
  }
}
