// Sliding-window question limit per key (browser id, IP). Process memory only:
// on Vercel each instance counts separately, so the provider's monthly spend
// cap is the limit that holds (spec section 7).
import { createLimiter } from "../lib/windowLimiter";
export { createLimiter, type LimitResult } from "../lib/windowLimiter";

// ASK_RATE_LIMIT raises it for the eval run on Preview (default 20 per hour).
export const checkLimit = createLimiter(
  () => Math.max(1, Math.trunc(Number(process.env.ASK_RATE_LIMIT)) || 20),
  3_600_000,
);
