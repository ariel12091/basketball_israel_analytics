// What happened in a run, judged against what the players on the floor
// usually do (docs/shared_definitions.md, "Gameflow run cause" and "Gameflow
// run player priors"; user 2026-11-22). Israeli league only (D43).
//
// Every factor is a list of chances, one per shot or possession, and the
// observed count. The expected count is their sum; how unusual the count is
// comes from their exact distribution (Poisson-binomial), not a normal curve.
// The priors are counts from the reader (server/runPriors.ts); this file
// shrinks them toward the league and compares.
import type { GameflowEvent, Side } from "./gameflow";
import type { Run } from "./gameflowRuns";
import { rRound } from "./round";

/** [made, attempts] for shooting; [count, floor possessions] for the rest. */
export type PlayerPrior = Record<
  "fg2" | "fg3" | "ft" | "tov" | "oreb" | "dreb",
  [number, number]
>;
export type LeagueRates = {
  fg2: number;
  fg3: number;
  ft: number;
  /** Turnovers per player per floor possession, and per team possession. */
  tovPlayer: number;
  tovTeam: number;
  /** Offensive / defensive rebounds per player per floor possession. */
  orebPlayer: number;
  drebPlayer: number;
  /** Offensive rebounds per missed field goal. */
  orebMiss: number;
  /** Points per team possession: the value of a turnover or a rebound. */
  ppp: number;
};
export type RunPriors = {
  /** By `${side}:${player_id}`. */
  players: Map<string, PlayerPrior>;
  league: LeagueRates;
};

/** League-average pseudo-counts each player's record is pulled toward. */
export const SHRINK = { fg2: 55, fg3: 89, ft: 25, tov: 300, reb: 400 } as const;
/** Offensive-rebound model, fitted on every missed field goal of 2025-26
 *  (15,915): log-odds per unit of the offense five's OREB rate relative to the
 *  league, the defense five's DREB rate, and the height edge per 10 cm. */
export const OREB_WEIGHTS = {
  offense: 0.95,
  defense: -0.22,
  height: 0.08,
} as const;
export const RULE = {
  leadPts: 2.5,
  secondPts: 1.5,
  maxP: 0.1,
  minShots: 4,
  maxClauses: 3,
  /** "Barely turned it over": at most 1 in 8+ possessions, 2+ expected. */
  clean: { minPoss: 8, maxTov: 1, minExpected: 2 },
} as const;

export type FactorKind = "fg2" | "fg3" | "ft" | "tov" | "oreb";
export type Factor = {
  /** "run": the run team's offense; "other": the other team's. */
  end: "run" | "other";
  side: Side;
  kind: FactorKind;
  obs: number;
  n: number;
  expected: number;
  /** Points toward the run team, against expectation. */
  pts: number;
  /** Chance of a result at least this good for the run team. */
  p: number;
  /** Who turned it over or rebounded, most first (2+ only). */
  who: [string, number][];
};

/** P(X >= obs) (upper) or P(X <= obs), X the number of successes. */
export function poissonBinomialTail(ps: number[], obs: number, upper: boolean) {
  let dist = [1];
  for (const p of ps) {
    const next = new Array(dist.length + 1).fill(0);
    dist.forEach((q, i) => {
      next[i] += q * (1 - p);
      next[i + 1] += q * p;
    });
    dist = next;
  }
  const sum = (xs: number[]) => xs.reduce((a, b) => a + b, 0);
  return Math.min(
    1,
    upper ? sum(dist.slice(Math.max(0, obs))) : sum(dist.slice(0, obs + 1)),
  );
}

const shrunk = ([m, n]: [number, number], k: number, lg: number) =>
  (m + k * lg) / (n + k);
const mean = (xs: number[]) => xs.reduce((a, b) => a + b, 0) / xs.length;
const logit = (p: number) => Math.log(p / (1 - p));
const other = (s: Side): Side => (s === "own" ? "opp" : "own");
const VALUE: Record<"fg2" | "fg3" | "ft", number> = { fg2: 2, fg3: 3, ft: 1 };

/** Each factor of a run, both teams' offense, biggest toward the run team first. */
export function runFactors(
  events: GameflowEvent[],
  run: Run,
  priors: RunPriors,
  heights: Map<string, number>,
  surname: (side: Side, player: string) => string,
): Factor[] {
  const L = priors.league,
    inRun = (e: GameflowEvent) =>
      e.elapsed >= run.start && e.elapsed <= run.end,
    prior = (side: Side, player: string | null) =>
      player ? priors.players.get(`${side}:${player}`) : undefined,
    mirror = new Map(
      events.filter((e) => !e.off).map((e) => [`${e.side}:${e.order_key}`, e]),
    );
  const out: Factor[] = [];
  for (const side of [run.side, other(run.side)]) {
    const end = side === run.side ? "run" : "other",
      sign = end === "run" ? 1 : -1,
      mine = events.filter((e) => e.side === side && e.off && inRun(e));
    const push = (
      kind: FactorKind,
      trials: [number, boolean][],
      value: number,
      who: [string, number][] = [],
    ) => {
      if (!trials.length) return;
      const ps = trials.map(([p]) => p),
        obs = trials.filter(([, o]) => o).length,
        expected = ps.reduce((a, b) => a + b, 0);
      // Good for the run team: its own made shots / rebounds up and turnovers
      // down; the other team's made shots / rebounds down and turnovers up.
      const goodUp = (end === "run") !== (kind === "tov");
      const pts = sign * (kind === "tov" ? -1 : 1) * value * (obs - expected);
      out.push({
        end,
        side,
        kind,
        obs,
        n: trials.length,
        expected,
        pts,
        p: poissonBinomialTail(ps, obs, goodUp),
        who,
      });
    };
    for (const kind of ["fg2", "fg3", "ft"] as const) {
      const trials = mine
        .filter((e) => e.kind === kind)
        .map((e): [number, boolean] => {
          const pr = prior(side, e.actor ?? null);
          return [
            pr ? shrunk(pr[kind], SHRINK[kind], L[kind]) : L[kind],
            e.made === true,
          ];
        });
      push(kind, trials, VALUE[kind]);
    }
    const whoOf = (kind: "tov" | "oreb") => {
      const c = new Map<string, number>();
      for (const e of mine)
        if (e.kind === kind && e.actor)
          c.set(
            surname(side, e.actor),
            (c.get(surname(side, e.actor)) ?? 0) + 1,
          );
      return [...c]
        .filter(([, v]) => v >= 2)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 2);
    };
    // Turnovers: each possession against the five on the floor.
    const fiveTov = (five: string[]) => {
      const rates = five.map((p) => {
        const pr = prior(side, p);
        return pr ? shrunk(pr.tov, SHRINK.tov, L.tovPlayer) : L.tovPlayer;
      });
      return rates.length >= 4
        ? (L.tovTeam * mean(rates)) / L.tovPlayer
        : L.tovTeam;
    };
    push(
      "tov",
      mine
        .filter((e) => e.poss_end)
        .map((e): [number, boolean] => [fiveTov(e.players), e.kind === "tov"]),
      L.ppp,
      whoOf("tov"),
    );
    // Offensive rebounds: each missed field goal against both fives.
    const rate = (s: Side, p: string, k: "oreb" | "dreb") => {
      const pr = prior(s, p),
        lg = k === "oreb" ? L.orebPlayer : L.drebPlayer;
      return pr ? shrunk(pr[k], SHRINK.reb, lg) : lg;
    };
    const height = (s: Side, five: string[]) => {
      const hs = five
        .map((p) => heights.get(`${s}:${p}`))
        .filter((h): h is number => h != null);
      return hs.length >= 4 ? mean(hs) : null;
    };
    const misses = mine.filter(
      (e) => (e.kind === "fg2" || e.kind === "fg3") && e.made === false,
    );
    const orebs = Math.min(
      misses.length,
      mine.filter((e) => e.kind === "oreb").length,
    );
    const chances = misses.map((e) => {
      const def = mirror.get(`${other(side)}:${e.order_key}`)?.players ?? [];
      if (e.players.length < 4 || def.length < 4) return L.orebMiss;
      const zo =
          mean(e.players.map((p) => rate(side, p, "oreb"))) / L.orebPlayer - 1,
        zd =
          mean(def.map((p) => rate(other(side), p, "dreb"))) / L.drebPlayer - 1,
        ho = height(side, e.players),
        hd = height(other(side), def),
        cm = ho != null && hd != null ? 100 * (ho - hd) : 0;
      const x =
        logit(L.orebMiss) +
        OREB_WEIGHTS.offense * zo +
        OREB_WEIGHTS.defense * zd +
        (OREB_WEIGHTS.height * cm) / 10;
      return 1 / (1 + Math.exp(-x));
    });
    push(
      "oreb",
      chances.map((p, i): [number, boolean] => [p, i < orebs]),
      L.ppp,
      whoOf("oreb"),
    );
  }
  return out.sort((a, b) => b.pts - a.pts);
}

const unusual = (f: Factor) =>
  f.p < RULE.maxP &&
  !(
    (f.kind === "fg2" || f.kind === "fg3" || f.kind === "ft") &&
    f.n < RULE.minShots
  );
const clean = (f: Factor) =>
  f.end === "run" &&
  f.kind === "tov" &&
  f.n >= RULE.clean.minPoss &&
  f.obs <= RULE.clean.maxTov &&
  f.expected >= RULE.clean.minExpected;

/** The factors a run's line names: led by one worth 2.5+ points and unusual;
 *  then others from 1.5 points, or the run team barely turning it over; at
 *  most three, biggest first. None without a lead. */
export function namedFactors(factors: Factor[]): Factor[] {
  const lead = factors.filter((f) => f.pts >= RULE.leadPts && unusual(f));
  if (!lead.length) return [];
  const second = factors.filter(
    (f) =>
      !lead.includes(f) &&
      ((f.pts >= RULE.secondPts && unusual(f)) || clean(f)),
  );
  return [...lead, ...second]
    .slice(0, RULE.maxClauses)
    .sort((a, b) => b.pts - a.pts);
}

/** Whole numbers from 2 up, one decimal below (1.96 reads "2", not "2.0"). */
const expectedText = (x: number) =>
  rRound(x, 1) >= 2 ? String(rRound(x)) : rRound(x, 1).toFixed(1);
function clause(f: Factor) {
  const who = f.who.map(([n, v]) => `${n} ${v}`).join(", "),
    exp = `(expected ${expectedText(f.expected)}${who ? `; ${who}` : ""})`;
  switch (f.kind) {
    case "fg2":
      return `went ${f.obs}/${f.n} on twos ${exp}`;
    case "fg3":
      return `went ${f.obs}/${f.n} from three ${exp}`;
    case "ft":
      return `went ${f.obs}/${f.n} at the line ${exp}`;
    case "tov":
      return `had ${f.obs || "no"} turnover${f.obs === 1 ? "" : "s"} in ${f.n} possessions ${exp}`;
    case "oreb":
      return `rebounded ${f.obs || "none"} of ${f.n} misses ${exp}`;
  }
}
const list = (xs: string[]) =>
  xs.length < 2
    ? xs.join("")
    : `${xs.slice(0, -1).join(", ")} and ${xs.at(-1)}`;

/** "Hapoel Holon had 12 turnovers in 23 possessions (expected 5; Sanogo 4,
 *  Amir 3) and went 1/10 from three (expected 4)"; null when nothing is named. */
export function factorsSentence(
  named: Factor[],
  names: Record<Side, string>,
): string | null {
  if (!named.length) return null;
  const order: Side[] = [];
  for (const f of named) if (!order.includes(f.side)) order.push(f.side);
  return order
    .map(
      (s) =>
        `${names[s]} ${list(named.filter((f) => f.side === s).map(clause))}`,
    )
    .join("; ");
}
