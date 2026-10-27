"use client";
import { useEffect } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";

/** app.js desiredValue(): an unscoped visit restores the saved competition. */
export default function HomeEntry() {
  const router = useRouter();
  useEffect(() => {
    let league = "il";
    try {
      const saved = localStorage.getItem("ibpl_league_select");
      if (saved === "E") league = "euroleague";
      else if (saved === "U") league = "eurocup";
    } catch {}
    router.replace(`/${league}/home`);
  }, [router]);
  return (
    <main className="container" aria-label="Choose league">
      <p>Opening Home…</p>
      <nav aria-label="Leagues">
        <Link href="/il/home">Israeli League</Link>
        {" · "}
        <Link href="/euroleague/home">EuroLeague</Link>
        {" · "}
        <Link href="/eurocup/home">EuroCup</Link>
      </nav>
    </main>
  );
}
