// What happened in a run, judged against what the players on the floor
// usually do (docs/shared_definitions.md, "Gameflow run cause" and "Gameflow
// run player priors"; user 2026-11-22). Israeli league only (D43).
//
// Every factor is a list of chances, one per shot or possession, and the
// observed count. The expected count is their sum; how unusual the count is
// comes from their exact distribution (Poisson-binomial), not a normal curve.
// The priors are counts from the reader (server/runPriors.ts); this file
// shrinks them toward the league and compares.
import type { GameflowEvent, RibbonModel, Side } from "./gameflow";
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
/** How much of the five's turnover edge (its players' mean rate over the
 *  league's) a possession keeps: two ball-handlers on together each handle it
 *  less, so a five differs less than its players' average. Fitted on every
 *  possession of 2025-26 held out game by game (user 2026-11-23). */
export const TOV_FIVE_WEIGHT = 0.6;
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
  /** Why the expected count is high or low (the main fives), when named. */
  why?: string;
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

function shrunk([m, n]: [number, number], k: number, lg: number) {
  return (m + k * lg) / (n + k);
}
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
          // Each shot on its shooter's own record (user 2026-11-25: no
          // spacing term -- it rested on a thin shooter label and moved the
          // expected count by a tenth of a make or two).
          const pr = prior(side, e.actor ?? null);
          return [pr ? shrunk(pr[kind], SHRINK[kind], L[kind]) : L[kind], e.made === true];
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
        ? L.tovTeam * (1 + TOV_FIVE_WEIGHT * (mean(rates) / L.tovPlayer - 1))
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
 *  over; every one that qualifies, biggest first (user 2026-11-23: no cap;
 *  2026-27 has at most four). None without a lead. */
export function namedFactors(factors: Factor[], net = Infinity): Factor[] {
  const leadBar = Math.min(RULE.leadPts, RULE.leadShare * net),
    secondBar = Math.min(RULE.secondPts, leadBar);
  const lead = factors.filter((f) => f.pts >= leadBar && unusual(f));
  if (!lead.length) return [];
  const second = factors.filter(
    (f) =>
      !lead.includes(f) && ((f.pts >= secondBar && unusual(f)) || clean(f)),
  );
  return [...lead, ...second].sort((a, b) => b.pts - a.pts);
}

/** Whole numbers from 2 up, one decimal below (1.96 reads "2", not "2.0"). */
const expectedText = (x: number) =>
  rRound(x, 1) >= 2 ? String(rRound(x)) : rRound(x, 1).toFixed(1);
function clause(f: Factor) {
  const who = f.who.map(([n, v]) => `${n} ${v}`).join(", "),
    exp = `(expected ${expectedText(f.expected)}${f.why ? `, ${f.why}` : ""}${who ? `; ${who}` : ""})`;
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
  tov: number;
  oreb: number;
  dreb: number;
};
/** The bars sit near the 5th and 95th percentiles of 2026-27's fives,
 *  weighted by floor time (75 games, 6,029 minutes; user 2026-11-23). Each
 *  trait is a fact about who was on the floor, context for the count, not a
 *  prediction (user 2026-11-25). No shooter trait: with this league's samples
 *  a "shooter" or "non-shooter" label mostly reflected how few threes a
 *  player had taken (user 2026-11-25). */
export const PROFILE = {
  small: 2.01,
  big: 2.0,
  /** The five's turnover edge after TOV_FIVE_WEIGHT: the old bars on the
   *  players' mean (0.82, 1.15), carried through 1 + 0.6 x (mean - 1). */
  tov: [0.892, 1.09],
  oreb: [0.81, 1.25],
  dreb: [0.89, 1.14],
  /** The fives described must have played this share of the run, and a minute. */
  /** ... a third of the run (user 2026-11-23: half was too much). */
  minShare: 1 / 3,
  minSecs: 60,
} as const;

export function lineupProfile(
  side: Side,
  five: string[],
  priors: RunPriors,
  heights: Map<string, number>,
): LineupProfile {
  const L = priors.league;
  const rate = (p: string, k: "tov" | "oreb" | "dreb") => {
    const pr = priors.players.get(`${side}:${p}`);
    const [lg, n] = {
      tov: [L.tovPlayer, SHRINK.tov],
      oreb: [L.orebPlayer, SHRINK.reb],
      dreb: [L.drebPlayer, SHRINK.reb],
    }[k];
    return pr ? shrunk(pr[k], n, lg) : lg;
  };
  const hs = five
    .map((p) => heights.get(`${side}:${p}`))
    .filter((h): h is number => h != null);
  const known = hs.length === five.length && hs.length > 0;
  return {
    tallest: known ? Math.max(...hs) : null,
    meanHeight: known ? mean(hs) : null,
    tov: 1 + TOV_FIVE_WEIGHT * (mean(five.map((p) => rate(p, "tov"))) / L.tovPlayer - 1),
    oreb: mean(five.map((p) => rate(p, "oreb"))) / L.orebPlayer,
    dreb: mean(five.map((p) => rate(p, "dreb"))) / L.drebPlayer,
  };
}

/** Every five each team used in the run, with its floor time inside it. */
export function runFives(
  model: RibbonModel,
  run: Run,
): Map<Side, { five: string[]; secs: number }[]> {
  const out = new Map<Side, { five: string[]; secs: number }[]>();
  for (const side of ["own", "opp"] as const) {
    const by = new Map<string, { five: Set<string>; segs: Set<number>; t: number }>();
    for (const l of model.lanes) {
      if (l.side !== side) continue;
      const s = Math.max(run.start, l.start_elapsed),
        e = Math.min(run.end, l.end_elapsed);
      if (e <= s) continue;
      const v = by.get(l.lineup_key) ?? { five: new Set(), segs: new Set(), t: 0 };
      v.five.add(l.player_key);
      // One lane row per player per stretch: count each stretch once.
      if (!v.segs.has(l.start_elapsed)) {
        v.segs.add(l.start_elapsed);
        v.t += e - s;
      }
      by.set(l.lineup_key, v);
    }
    out.set(
      side,
      [...by.values()]
        .filter((v) => v.five.size === 5)
        .map((v) => ({ five: [...v.five], secs: v.t })),
    );
  }
  return out;
}

type Profiled = { profile: LineupProfile; secs: number };
/** A trait counts when the fives that have it together played at least a
 *  third of the run and a minute (user 2026-11-23: a team that goes small across
 *  several substitutions still went small). The text is singular for one
 *  five ("a turnover-prone five"), plural for several ("turnover-prone fives"). */
function trait(
  fives: Profiled[],
  len: number,
  has: (p: LineupProfile) => boolean,
  text: (one: boolean, with_: LineupProfile[]) => string,
): string | null {
  const with_ = fives.filter((f) => has(f.profile)),
    secs = with_.reduce((a, f) => a + f.secs, 0);
  return secs >= PROFILE.minSecs && secs >= PROFILE.minShare * len
    ? text(with_.length === 1, with_.map((f) => f.profile))
    : null;
}
const five = (one: boolean, what: string) => (one ? `a ${what} five` : `${what} fives`);

/** Why a named factor's expected count is high or low (user 2026-11-23), from
 *  the fives of the team on offense and of the one defending: "a turnover-prone
 *  five", "a five no taller than 2.01 m", "against a weak defensive-rebounding
 *  five"... At most two; none for shots. */
export function factorReason(
  kind: FactorKind,
  offense: Profiled[],
  defense: Profiled[],
  len: number,
): string | null {
  const P = PROFILE,
    t = (fs: Profiled[], has: (p: LineupProfile) => boolean, text: (one: boolean, w: LineupProfile[]) => string) =>
      trait(fs, len, has, text);
  const small = (fs: Profiled[], pre = "") =>
      t(fs, (p) => p.tallest != null && p.tallest <= P.small, (one, w) => {
        const top = Math.max(...w.map((p) => p.tallest!)).toFixed(2);
        return `${pre}${one ? "a five" : "fives"} no taller than ${top} m`;
      }),
    big = (fs: Profiled[], pre = "") =>
      t(fs, (p) => p.meanHeight != null && p.meanHeight >= P.big, (one, w) =>
        one ? `${pre}a big five averaging ${w[0].meanHeight!.toFixed(2)} m` : `${pre}big fives`);
  const out: (string | null)[] = [];
  if (kind === "tov") {
    out.push(t(offense, (p) => p.tov >= P.tov[1], (one) => five(one, "turnover-prone")));
    out.push(t(offense, (p) => p.tov <= P.tov[0], (one) => five(one, "low-turnover")));
  }
  if (kind === "oreb") {
    out.push(t(offense, (p) => p.oreb >= P.oreb[1], (one) => five(one, "strong offensive-rebounding")));
    out.push(t(offense, (p) => p.oreb <= P.oreb[0], (one) => five(one, "weak offensive-rebounding")));
    out.push(small(offense) ?? big(offense));
    out.push(t(defense, (p) => p.dreb >= P.dreb[1], (one) => `against ${five(one, "strong defensive-rebounding")}`));
    out.push(t(defense, (p) => p.dreb <= P.dreb[0], (one) => `against ${five(one, "weak defensive-rebounding")}`));
    out.push(small(defense, "against ") ?? big(defense, "against "));
  }
  const named = out.filter((x): x is string => x != null).slice(0, 2);
  return named.length ? list(named) : null;
}

/** The named factors, each with its reason when the run's fives give one. */
export function withReasons(
  named: Factor[],
  model: RibbonModel,
  run: Run,
  priors: RunPriors,
  heights: Map<string, number>,
): Factor[] {
  const fives = runFives(model, run),
    profiled = (s: Side): Profiled[] =>
      (fives.get(s) ?? []).map((f) => ({
        profile: lineupProfile(s, f.five, priors, heights),
        secs: f.secs,
      })),
    bySide = { own: profiled("own"), opp: profiled("opp") },
    len = run.end - run.start;
  return named.map((f) => {
    const why = factorReason(f.kind, bySide[f.side], bySide[other(f.side)], len);
    return why ? { ...f, why } : f;
  });
}
