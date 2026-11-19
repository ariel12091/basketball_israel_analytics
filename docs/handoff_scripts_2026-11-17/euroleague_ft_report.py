"""Render the saved measurement artifacts into the repository handoff report."""
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "euroleague/data/diagnostics/ft_stoppage_measure"


def main():
    summary = json.loads((DATA / "summary.json").read_text())
    df = pd.read_csv(DATA / "buckets.csv")
    fixtures = pd.read_csv(DATA / "fixture_comparison.csv")
    contexts = json.loads((DATA / "contexts.json").read_text())
    total = int(df.ft_rows.sum())
    unique = df[df.strict_fit.eq("unique")]
    provisional = int(df.provisional.sum())
    unresolved = int(df.unresolved.sum())
    checked = summary["counters"]["counter_rows"]
    assert checked == total, "Every measured FT must appear in exactly one clock bucket"
    assert len(fixtures) == 18 and fixtures.fit.eq("unique").sum() == 15
    assert fixtures.loc[fixtures.fit.eq("unique"), "partition_matches"].eq(True).all()
    assert sum(int(e["grouping_status"] == "unresolved") for c in contexts
               for e in c["events"] if e["play_type"] in {"FTM", "FTA"}) == unresolved
    mismatch = summary["counters"].get("counter_mismatch", 0)
    missing = summary["counters"].get("missing_counter", 0)
    lines = ["# EuroLeague / EuroCup FT stoppage measurements — 2026-11-17", "",
        "## Result", "",
        f"Measured **{summary['games']} cached games, {total:,} FT rows, {len(df):,} period/clock buckets containing FTs**. "
        f"Strict entitlement counts have one candidate allocation in {len(unique):,} buckets "
        f"({len(unique)/len(df):.1%}). This is feasibility evidence, not confirmation of a trip or possession endpoint.", "",
        f"Of {provisional:,} currently provisional FTs, {int(unique.provisional.sum()):,} lie in uniquely fitting buckets; "
        f"of {unresolved} unresolved FTs, {int(unique.unresolved.sum())} do. "
        "The main design correction is to build incidents from event evidence and sequence, allowing clock discrepancies. "
        "A period/clock key cannot be an incident identifier.", "",
        "## Coverage and provenance", "",
        "Both competitions and both cached seasons are included. There are 786 per-game JSON caches; "
        "the combined competition CSVs contain 784 distinct games and add no games. "
        "The handoff's 819-game database snapshot is a different population and was not re-queried. "
        "No database writes, re-fetches, parser edits, or production reloads were performed.", "",
        f"Baseline checkout: `{summary['checkout']}`. Parser SHA256: `{summary['parser_sha256']}`. "
        "The checkout parser matches local `origin/main` for this file. "
        "`manifest.json` records each input game's SHA256 and row count; `summary.json` records all read/parse errors.", "",
        f"Read/parse errors: {len(summary['errors'])}.", "",
        "## Experiment definition", "",
        "For each game, period, and provider clock, independently allocate FT shooter counts to opposing committed foul rows. "
        "The strict hypothesis gives technical/coach/bench fouls one FT and unsportsmanlike/disqualifying fouls two. "
        "An ordinary personal can receive zero, two, or three; one is permitted only with a same-team, same-shooter made basket in the bucket. "
        "When foul-drawn rows exist, ordinary trips must use a drawn player. With multiple drawn players the experiment allows all of them; "
        "it does not infer a foul-to-drawn pairing. Offensive fouls create no FT entitlement in this experiment. "
        "This deliberately does not model bonus state, shooting motion, replacement shooters, cancellation rules, or every special penalty exception.", "",
        "Enumerate allocations to distinct foul rows, stopping at the second solution. "
        "Zero solutions means the hypothesis and scope do not explain the feed; it does not establish a provider error. "
        "One solution is unique only under these assumptions. FT attempts by the same shooter are exchangeable: "
        "two trips by one shooter remain unresolved at the attempt level even with unique counts. "
        "A separate sensitivity run allows any special foul zero FTs; that is a relaxation, not a basketball cancellation algorithm.", "",
        "## Counts by population", "",
        "| Competition / season | Games with FTs | FT rows | Buckets | Unique | Ambiguous | No fit |",
        "|---|---:|---:|---:|---:|---:|---:|"]
    for p in summary["populations"]:
        fits = p["strict_fit"]
        lines.append(f"| {p['competition']} / {p['season']} | {p['games']} | {p['ft_rows']:,} | {p['buckets']:,} | "
                     f"{fits.get('unique', 0):,} | {fits.get('ambiguous', 0):,} | {fits.get('no_fit', 0):,} |")
    lines += ["", "## Current parser status versus strict feasibility", "",
              "| Fit | Buckets | Confirmed FT rows | Provisional FT rows | Unresolved FT rows |",
              "|---|---:|---:|---:|---:|"]
    for fit, sub in df.groupby("strict_fit"):
        lines.append(f"| {fit} | {len(sub):,} | {int(sub.confirmed.sum()):,} | {int(sub.provisional.sum()):,} | {int(sub.unresolved.sum()):,} |")
    partitions = unique[unique.partition_matches.notna()]
    unequal = partitions[partitions.partition_matches.eq(False)]
    risk = int(df.boundary_risk.sum())
    repeated = int(unique.repeated_shooter_trips.sum())
    lines += ["", f"Among {len(partitions):,} uniquely fitting buckets with distinct trip shooters, "
              f"{len(unequal):,} candidate FT-row partitions differ from the current parser. "
              f"{repeated:,} uniquely fitting buckets have multiple trips for one shooter, so this experiment cannot assign their individual attempts.", "",
              "The three partition differences are the same pattern: a technical/coach FT interrupts another player's personal trip. "
              "E2025/188 Q2 03:41: Morris's rows 218/222/223 form a three-shot candidate; Vezenkov's row 221 is the technical. "
              "E2025/355 Q3 09:38: Milutinov's rows 269/273 form a two-shot candidate; Vezenkov's row 272 is the coach penalty. "
              "U2025/2 Q3 06:42: Taylor's rows 345/351/352 form a three-shot candidate; Russell's row 350 is the technical. "
              "The baseline sends the resumed personal FTs to an earlier parent in the two EuroLeague cases and leaves the first resumed FT unresolved in the EuroCup case.", "",
              "The cancellation relaxation changes 10 no-fit buckets to unique and one to ambiguous, but also changes 61 strictly unique buckets to ambiguous. "
              "That sensitivity argues for explicit cancellation evidence rather than letting every special penalty disappear.", "",
              "## Clock and scope limitations", "",
              f"{risk:,} buckets ({risk/len(df):.1%}) contain a rebound, turnover, offensive foul, jump ball, or multiple field-goal attempts. "
              "This is a conservative scope-warning flag: a final-FT rebound or a penalty-associated turnover can belong to the incident. "
              f"{int(df.clock_runs.gt(1).sum())} buckets revisit a clock after a different clock row. "
              f"{int((df.strict_fit.eq('no_fit') & df.foul_count.eq(0)).sum()):,} no-fit buckets contain no eligible foul row at all.", "",
              "Examples already in the labelled fixtures: E2025/1 Q1 has an and-one foul/basket at 06:01 and its FT at 05:53; "
              "the 05:53 bucket also contains several live-ball plays. E2025/7's coach technical and FT cross clocks too. "
              "The current fixture set has 18 FT-bearing fixture/clock groups: 15 fit uniquely and all 15 reproduce the labelled FT partition; "
              "two fail due to clock shifts and one due to offsetting technicals. The cancellation relaxation fits the latter, leaving both clock-shift failures.", "",
              "## PLAYINFO shooter counters", "",
              f"Parsed {checked-missing:,}/{checked:,} FT descriptions. {mismatch:,} rows do not advance the named player's "
              "game-cumulative (made, attempted) counter by (1,1) for FTM or (0,1) for FTA. "
              "Checks use team/player identity, start each game at (0,0), carry across periods, and compare to the previous observed counter. "
              "After a mismatch the next comparison uses the newly observed value, avoiding a cascade. "
              "These counters provide a consistency check, not trip size or independent proof of shooter attribution. "
              "An FT recorded under the wrong player may still advance that recorded player's counter correctly.", "",
              "## Review of every previously unresolved FT", "",
              "All 15 listed games were re-parsed from local cache. Detailed source-order windows are in `blocked_cases.md`. "
              "The following are observations and candidate explanations; no feed corrections or accepted-defect rows were added.", "",
              "| Game | FT source order(s) | Observation |",
              "|---|---|---|",
              "| E2025/91 | 265, 586 | Foul before made basket/challenge; second incident also has an opposing technical FT. |",
              "| E2025/98 | 377 | Tonut draws foul and shoots first; Mannion is recorded on second, with an assist between. |",
              "| E2025/136 | 397 | Technical then team turnover then FT. |",
              "| E2025/302 | 252 | Foul and challenge before the and-one basket. |",
              "| E2025/303 | 558 | Unsportsmanlike plus opposing personal/turnover; two same-shooter FTs fit the unsportsmanlike. |",
              "| E2025/383 | 408 | Bench foul logged after its FT. |",
              "| E2026/31 | 372 | Waters shoots, is substituted out, replacement Fodzo Dada shoots the next FT. Replacement-shooter exception needs evidence. |",
              "| E2026/73 | 226 | Coach foul, then offensive foul/turnover, then coach-penalty FT. |",
              "| U2025/2 | 351 | Three Taylor FTs interrupted by Russell's technical FT. |",
              "| U2025/79 | 58 | Devoe shoots, exits, Smith enters and shoots. Replacement-shooter exception needs evidence. |",
              "| U2025/86 | 466 | Williams draws foul/shoots; next FT is recorded for opposing-team Danusevicius. |",
              "| U2026/5 | 477, 603 | Personal + technical/coach FT followed by team turnover, then remaining personal FTs. |",
              "| U2026/6 | 305, 427 | First is foul before and-one basket; second has an opposing made three between foul and two FTs. Boundary/order ambiguity remains. |",
              "| U2026/56 | 458 | Bilan's two FTs cross 04:47/04:24 amid a technical and live-ball rows; inspect wider sequence before classifying. |",
              "| U2026/109 | 139 | Baslyk shoots, exits, Kennedy enters and shoots. Replacement-shooter exception needs evidence. |", "",
              "## Design to carry forward", "",
              "1. Use source event order for identity. Construct candidate incident windows with explicit live/dead-ball transitions; clock is supporting evidence. "
              "Allow a matching and-one basket on either side of its foul, and record uncertain live-ball boundaries rather than swallowing them.",
              "2. Keep foul-to-drawn-player pairing, penalty cancellation, shooter replacement, and FT entitlement allocation explicit. "
              "Three cached cases change shooter exactly at a substitution, so a one-shooter invariant needs a justified exception path.",
              "3. Solve entitlement counts, then individual FT attempts, then attempt order/trip size. "
              "Keep incident ID, foul parent, trip ID, attempt number, and possession endpoint separate. "
              "A unique aggregate allocation must not automatically upgrade grouping_status.",
              "4. Implement beside the parser only after resolving these scope and rule gaps. Compare parents, complete trip partitions, "
              "and endpoint changes across the cached population; retain unresolved cases. Reloading production is a separate step.", "",
              "## Reproduce and validation", "", "```powershell",
              "& euroleague/.venv/Scripts/python.exe docs/handoff_scripts_2026-11-17/euroleague_ft_measure.py",
              "& euroleague/.venv/Scripts/python.exe docs/handoff_scripts_2026-11-17/euroleague_ft_cases.py",
              "& euroleague/.venv/Scripts/python.exe docs/handoff_scripts_2026-11-17/euroleague_ft_report.py",
              "& euroleague/.venv/Scripts/python.exe -m unittest discover -s docs/handoff_scripts_2026-11-17 -p test_euroleague_ft_measure.py -v",
              "```", "", "Validation passed: five experiment tests (reordering, technical/personal separation, ambiguity, conflicting shooters, "
              "missing fouls/cancellation); all 19 existing parser tests; all 15 uniquely fitting labelled fixture partitions. "
              "No user-facing or shared Israeli implementation changed, so mobile verification and Israeli parser regression runs are not applicable.", "",
              "Detailed artifacts (ignored): `euroleague/data/diagnostics/ft_stoppage_measure/` contains `summary.json`, `manifest.json`, "
              "`buckets.csv`, `contexts.json`, `counter_anomalies.csv`, `fixture_comparison.csv`, and `blocked_cases.md`.", ""]
    (ROOT / "docs/euroleague_ft_stoppage_measurements_2026-11-17.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote report: {len(df)} buckets, {total} FTs")


if __name__ == "__main__":
    main()
