// Sliding-window question limit per key (browser id, IP). Process memory only:
// on Vercel each instance counts separately, so the provider's monthly spend
// cap is the limit that holds (spec section 7).
export type LimitResult = { ok: true } | { ok: false; retryAfterSec: number };

export function createLimiter(max: number | (() => number), windowMs: number) {
  const seen = new Map<string, number[]>();
  return function check(keys: string[], now: number): LimitResult {
    const limit = typeof max === "function" ? max() : max;
    let wait = 0;
    for (const k of keys) {
      const recent = (seen.get(k) ?? []).filter((t) => t > now - windowMs);
      seen.set(k, recent);
      if (recent.length >= limit) wait = Math.max(wait, recent[0] + windowMs - now);
    }
    if (wait > 0) return { ok: false, retryAfterSec: Math.ceil(wait / 1000) };
    for (const k of keys) seen.get(k)!.push(now);
    if (seen.size > 10_000)
      for (const [k, v] of seen) if (!v.some((t) => t > now - windowMs)) seen.delete(k);
    return { ok: true };
  };
}

// ASK_RATE_LIMIT raises it for the eval run on Preview (default 20 per hour).
export const checkLimit = createLimiter(
  () => Math.max(1, Math.trunc(Number(process.env.ASK_RATE_LIMIT)) || 20),
  3_600_000,
);
