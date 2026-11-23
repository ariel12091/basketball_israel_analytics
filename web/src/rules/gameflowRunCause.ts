// What happened in a run, judged against what the players on the floor
// usually do (docs/shared_definitions.md, "Gameflow run cause" and "Gameflow
// run player priors"; user 2026-11-22). Israeli league only (D43).
//
// Every factor is a list of chances, one per shot or possession, and the
// observed count. The expected count is their sum; how unusual the count is
// comes from their exact distribution (Poisson-binomial), not a normal curve.
// The priors are counts from the reader (server/runPriors.ts); this file
// shrinks them toward the league and compares.
import { clock, type GameflowEvent, type RibbonModel, type Side } from "./gameflow";
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
  /** Threes per field-goal attempt. */
  fg3Share: number;
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
  /** ...or this share of the run's net margin, when less (user 2026-11-23). */
  leadShare: 0.2,
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

/** The factors a run's line names: led by one worth 2.5+ points (or a fifth
 *  of the run's net margin, when less) and unusual; then others from 1.5
 *  points (or the lead bar, when lower), or the run team barely turning it
 *  over; at most three, biggest first. None without a lead. */
export function namedFactors(factors: Factor[], net = Infinity): Factor[] {
  const leadBar = Math.min(RULE.leadPts, RULE.leadShare * net),
    secondBar = Math.min(RULE.secondPts, leadBar);
  const lead = factors.filter((f) => f.pts >= leadBar && unusual(f));
  if (!lead.length) return [];
  const second = factors.filter(
    (f) =>
      !lead.includes(f) && ((f.pts >= secondBar && unusual(f)) || clean(f)),
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

/** "Gameflow lineup profile": what a five usually does, from its players'
 *  records shrunk as above. Rates are relative to the league (1 = average). */
export type LineupProfile = {
  tallest: number | null;
  meanHeight: number | null;
  /** Players taking at least PROFILE.shooterShare of their shots from three. */
  shooters: number;
  tov: number;
  oreb: number;
  dreb: number;
  /** Three-point percentage, each player weighted by his share of threes. */
  fg3: number;
};
/** The bars sit near the 5th and 95th percentiles of 2026-27's fives,
 *  weighted by floor time (75 games, 6,029 minutes; user 2026-11-23). */
export const PROFILE = {
  /** Pseudo-attempts pulling a player's three-point share to the league's. */
  shareShrink: 30,
  shooterShare: 0.3,
  small: 2.01,
  big: 2.0,
  fewShooters: 2,
  tov: [0.82, 1.15],
  oreb: [0.81, 1.25],
  dreb: [0.89, 1.14],
  fg3: [0.331, 0.378],
  /** The five named must have played this share of the run, and a minute. */
  minShare: 0.5,
  minSecs: 60,
} as const;

export function lineupProfile(
  side: Side,
  five: string[],
  priors: RunPriors,
  heights: Map<string, number>,
): LineupProfile {
  const L = priors.league;
  const per = five.map((p) => {
    const pr = priors.players.get(`${side}:${p}`);
    const share = pr
      ? (pr.fg3[1] + PROFILE.shareShrink * L.fg3Share) /
        (pr.fg2[1] + pr.fg3[1] + PROFILE.shareShrink)
      : L.fg3Share;
    return {
      share,
      fg3: pr ? shrunk(pr.fg3, SHRINK.fg3, L.fg3) : L.fg3,
      tov: pr ? shrunk(pr.tov, SHRINK.tov, L.tovPlayer) : L.tovPlayer,
      oreb: pr ? shrunk(pr.oreb, SHRINK.reb, L.orebPlayer) : L.orebPlayer,
      dreb: pr ? shrunk(pr.dreb, SHRINK.reb, L.drebPlayer) : L.drebPlayer,
    };
  });
  const hs = five
    .map((p) => heights.get(`${side}:${p}`))
    .filter((h): h is number => h != null);
  const known = hs.length === five.length && hs.length > 0;
  return {
    tallest: known ? Math.max(...hs) : null,
    meanHeight: known ? mean(hs) : null,
    shooters: per.filter((x) => x.share >= PROFILE.shooterShare).length,
    tov: mean(per.map((x) => x.tov)) / L.tovPlayer,
    oreb: mean(per.map((x) => x.oreb)) / L.orebPlayer,
    dreb: mean(per.map((x) => x.dreb)) / L.drebPlayer,
    fg3:
      per.reduce((a, x) => a + x.fg3 * x.share, 0) /
      per.reduce((a, x) => a + x.share, 0),
  };
}

const vsLeague = (x: number) =>
  `${Math.round(Math.abs(x - 1) * 100)}% ${x > 1 ? "above" : "below"} the league`;
/** A five's strengths and weaknesses, rarest-first order fixed: size,
 *  shooters, ball security, glass, three-point accuracy. */
export function profileTraits(p: LineupProfile): string[] {
  const P = PROFILE,
    out: string[] = [];
  if (p.tallest != null && p.tallest <= P.small)
    out.push(`small (tallest ${p.tallest.toFixed(2)} m)`);
  else if (p.meanHeight != null && p.meanHeight >= P.big)
    out.push(`big (average ${p.meanHeight.toFixed(2)} m)`);
  if (p.shooters <= P.fewShooters)
    out.push(
      p.shooters
        ? `only ${p.shooters} three-point shooter${p.shooters === 1 ? "" : "s"}`
        : "no three-point shooter",
    );
  else if (p.shooters === 5) out.push("five three-point shooters");
  if (p.tov >= P.tov[1]) out.push(`turnover-prone (${vsLeague(p.tov)})`);
  else if (p.tov <= P.tov[0]) out.push(`careful with the ball (turnovers ${vsLeague(p.tov)})`);
  if (p.oreb >= P.oreb[1]) out.push(`strong on the offensive glass (${vsLeague(p.oreb)})`);
  else if (p.oreb <= P.oreb[0]) out.push(`weak on the offensive glass (${vsLeague(p.oreb)})`);
  if (p.dreb >= P.dreb[1]) out.push(`strong on the defensive glass (${vsLeague(p.dreb)})`);
  else if (p.dreb <= P.dreb[0]) out.push(`weak on the defensive glass (${vsLeague(p.dreb)})`);
  const pct = Math.round(p.fg3 * 1000) / 10;
  if (p.fg3 >= P.fg3[1]) out.push(`good from three (${pct}% expected)`);
  else if (p.fg3 <= P.fg3[0]) out.push(`poor from three (${pct}% expected)`);
  return out;
}

/** Each team's five with the most floor time in the run, when it played at
 *  least half of it and a minute. */
export function mainFives(model: RibbonModel, run: Run, order: Side[]) {
  return order.flatMap((side) => {
    const by = new Map<string, { five: Set<string>; secs: Set<string>; t: number }>();
    for (const l of model.lanes) {
      if (l.side !== side) continue;
      const s = Math.max(run.start, l.start_elapsed),
        e = Math.min(run.end, l.end_elapsed);
      if (e <= s) continue;
      const v = by.get(l.lineup_key) ?? { five: new Set(), secs: new Set(), t: 0 };
      v.five.add(l.player_key);
      const seg = `${l.start_elapsed}`;
      if (!v.secs.has(seg)) {
        v.secs.add(seg);
        v.t += e - s;
      }
      by.set(l.lineup_key, v);
    }
    const best = [...by.values()].sort((a, b) => b.t - a.t)[0];
    const len = run.end - run.start;
    return best &&
      best.five.size === 5 &&
      best.t >= PROFILE.minSecs &&
      best.t >= PROFILE.minShare * len
      ? [{ side, five: [...best.five], secs: best.t }]
      : [];
  });
}

/** "Lineup profile: Hapoel Tel-Aviv's main five in the run (Blakeney, ...;
 *  3:10 of 4:40): small (tallest 2.01 m) and turnover-prone (18% above
 *  the league)."; null when neither main five stands out. */
export function profileLine(
  model: RibbonModel,
  run: Run,
  names: Record<Side, string>,
  priors: RunPriors,
  heights: Map<string, number>,
  surname: (side: Side, player: string) => string,
): string | null {
  const order: Side[] = [run.side, other(run.side)];
  const parts = mainFives(model, run, order).flatMap(({ side, five, secs }) => {
    const traits = profileTraits(lineupProfile(side, five, priors, heights));
    if (!traits.length) return [];
    const len = run.end - run.start,
      when = secs >= len ? "the whole run" : `${clock(secs)} of ${clock(len)}`;
    return [
      `${names[side]}'s main five in the run (${five.map((p) => surname(side, p)).join(", ")}; ${when}): ${list(traits)}`,
    ];
  });
  return parts.length ? `Lineup profile: ${parts.join("; ")}.` : null;
}
