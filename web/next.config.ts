import type { NextConfig } from "next";
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
