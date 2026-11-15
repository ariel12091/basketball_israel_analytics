import type { NextConfig } from "next";
// Content-Security-Policy. Scripts and styles may come only from this origin
// (plus jsDelivr's pinned icon stylesheet and its font); no plugins, no
// <base> or form hijacking, no framing. Scripts keep 'unsafe-inline': Next
// writes each page's data as inline scripts, and the tab pages are
// CDN-cached HTML, so a per-request nonce is impossible and a hash list
// cannot cover them (a hash would also switch 'unsafe-inline' off).
// Development adds what React's dev tooling needs (eval, the HMR socket).
const dev = process.env.NODE_ENV === "development";
const CSP = [
  "default-src 'self'",
  `script-src 'self' 'unsafe-inline'${dev ? " 'unsafe-eval'" : ""}`,
  "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",
  "font-src 'self' data: https://cdn.jsdelivr.net",
  "img-src 'self' data: blob:",
  `connect-src 'self'${dev ? " ws: wss:" : ""}`,
  "object-src 'none'",
  "base-uri 'self'",
  "form-action 'self'",
  "frame-ancestors 'none'",
].join("; ");
const config: NextConfig = {
  poweredByHeader: false,
  turbopack: { root: process.cwd() },
  // Vercel's CDN shares a tab's first HTML for up to an hour (plus a day
  // served while it re-renders in the background). The data changes nightly,
  // so a shorter life only adds function renders (Fast Origin Transfer). Safe
  // because the first table is the page's only server-read content and each
  // of these pages polls /api/v1/version on load and refetches when its table
  // is older (TabKit useVersion); Compare and Gameflow do not, so they stay
  // uncached. A deployment starts with an empty cache. Client
  // navigations (RSC payloads) and the tests' client-data mode are left out.
  // A page whose first read failed answers 500 (lib/tabPages FirstReadFailed),
  // which the CDN does not keep.
  async headers() {
    return [
      // No other site may frame the app (clickjacking), browsers must not
      // guess content types, and other sites see only the origin we link from.
      {
        source: "/:path*",
        headers: [
          { key: "X-Content-Type-Options", value: "nosniff" },
          { key: "X-Frame-Options", value: "DENY" },
          { key: "Content-Security-Policy", value: CSP },
          { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
        ],
      },
      {
        source:
          "/:league(il|euroleague|eurocup)/:tab(home|game-logs|onoff|lineups|team-ratings|player-stats)",
        missing: [
          { type: "header", key: "rsc" },
          { type: "header", key: "next-router-prefetch" },
          { type: "header", key: "x-ibpl-client-data" },
        ],
        headers: [
          {
            key: "Vercel-CDN-Cache-Control",
            value: "public, s-maxage=3600, stale-while-revalidate=86400",
          },
        ],
      },
    ];
  },
};
export default config;
