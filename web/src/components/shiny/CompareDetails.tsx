"use client";
import { Fragment, useState, type ReactNode } from "react";
import type { Row } from "@/rules/gameLogs";
import type { Query } from "@/lib/tabQuery";
import { compareSideParams } from "@/lib/compareQuery";
import {
  detailSections,
  gridGapText,
  contextText,
  shootingRates,
  gapInfo,
  num,
  formatCompare,
  reverseGap,
  type MetricDef,
  type DetailSection,
  playerMetric,
  playerShotRows,
  playerSwingValue,
  playerShotSwing,
} from "@/rules/compare";
import { useCompareData, CompareStatus, type CompareSeed } from "./CompareData";
import data from "@/data/compareUi.json";
import ui from "@/data/shinyUi.json";
import { comparePairReady } from "@/lib/compareAsync";
type GridMetric = {
  definition: MetricDef;
  a: number | null;
  b: number | null;
  sub?: [ReactNode, ReactNode];
};
export function CompareGrid({
  sections,
  labels,
  reverse = false,
  player = false,
}: {
  sections: { title: string; metrics: GridMetric[] }[];
  labels: [string, string];
  reverse?: boolean;
  player?: boolean;
}) {
  const [sort, setSort] = useState(0);
  return (
    <div className="cmp-compare-grid">
      <div className="cmp-col-header cmp-col-a cmp-cell cmp-first-row">
        {labels[0]}
      </div>
      <button
        type="button"
        className="cmp-col-header cmp-col-gap cmp-cell cmp-first-row"
        onClick={() => setSort(sort === 0 ? -1 : sort === -1 ? 1 : 0)}
      >
        Gap {sort === 0 ? "↕" : sort < 0 ? "↓" : "↑"}
      </button>
      <div className="cmp-col-header cmp-col-b cmp-cell cmp-first-row">
        {labels[1]}
      </div>
      {sections.map((section, si) => {
        const rows = section.metrics.map((m, index) => ({
          ...m,
          index,
          gap: gapInfo(
            num(m.a),
            num(m.b),
            m.definition.polarity ?? "higher",
            reverse,
          ),
        }));
        const max =
          Math.max(0, ...rows.map((r) => Math.abs(r.gap.gap ?? 0))) || 1;
        if (sort)
          rows.sort(
            (a, b) =>
              sort * ((a.gap.gap ?? 0) - (b.gap.gap ?? 0)) || a.index - b.index,
          );
        return (
          <Fragment key={section.title}>
            <div className="cmp-section-title">{section.title}</div>
            {rows.map((r, j) => {
              const m = r.definition,
                leader =
                  r.gap.a_wins == null
                    ? r.gap.direction
                    : r.gap.a_wins
                      ? "a"
                      : "b",
                gap = r.gap.gap;
              const cls =
                si === sections.length - 1 && j === rows.length - 1
                  ? " cmp-last-row"
                  : "";
              const estimate =
                m.factor && r.a != null && r.b != null
                  ? Math.abs(
                      (r.a - r.b) *
                        Number(
                          (ui.ffImpactWeights as Record<string, number>)[
                            m.factor
                          ],
                        ),
                    )
                  : null;
              return (
                <Fragment key={m.label}>
                  <div className={`cmp-stat-row cmp-col-a cmp-cell${cls}`}>
                    <span className="cmp-stat-label">{m.label}</span>
                    <span
                      className={`cmp-stat-value ${leader === "none" || leader === "a" ? "winner" : "loser"}`}
                    >
                      {formatCompare(r.a, m.fmt, m.fmt === "net")}
                    </span>
                  </div>
                  <div
                    className={`cmp-gap-row cmp-col-gap cmp-cell${cls}`}
                    data-gap={gap}
                  >
                    <span
                      className={`cmp-gap-num ${leader === "none" ? "" : leader + "-color"}`}
                    >
                      {gridGapText(gap, m.fmt, player)}
                    </span>
                    {estimate != null && (
                      <span
                        className="ff-impact-est"
                        title={ui.ffImpactEstTitle}
                      >
                        est. {formatCompare(estimate)} pts
                      </span>
                    )}
                    <div className="cmp-bar-container">
                      <div className="cmp-bar-center" />
                      {gap != null && gap !== 0 && (
                        <div
                          className={`cmp-bar toward-${leader === "a" ? "a" : "b"}`}
                          style={{ width: `${(Math.abs(gap) / max) * 50}%` }}
                        />
                      )}
                    </div>
                  </div>
                  <div className={`cmp-stat-row cmp-col-b cmp-cell${cls}`}>
                    <span className="cmp-stat-label">{m.label}</span>
                    <span
                      className={`cmp-stat-value ${leader === "none" || leader === "b" ? "winner" : "loser"}`}
                    >
                      {formatCompare(r.b, m.fmt, m.fmt === "net")}
                    </span>
                  </div>
                </Fragment>
              );
            })}
          </Fragment>
        );
      })}
    </div>
  );
}
export function CompareContext({
  a,
  b,
  teams = false,
  player = false,
}: {
  a: Row | null;
  b: Row | null;
  teams?: boolean;
  player?: boolean;
}) {
  return (
    <div className="cmp-context-bar">
      {([a, b] as const).map((r, i) => (
        <div className="cmp-context-side" key={i}>
          <span className={`cmp-context-badge ${i ? "b" : "a"}`}>
            {i ? "B" : "A"}
          </span>
          <span className="cmp-context-info">
            {contextText(r, teams, player) ?? "didn't play"}
          </span>
        </div>
      ))}
    </div>
  );
}
export function EntityDetails({
  q,
  seed,
  name,
  labels,
  descriptions,
}: {
  q: Query;
  seed: CompareSeed;
  name: string;
  labels: [string, string];
  descriptions: [string, string];
}) {
  const teams = q.view === "Teams",
    summaryKind = teams ? "ratings" : "lineups",
    ffKind = teams ? "four_factors" : "lineup_ff";
  const ra = useCompareData(compareSideParams(q, "a", summaryKind), seed),
    rb = useCompareData(compareSideParams(q, "b", summaryKind), seed);
  const fa = useCompareData(
      compareSideParams(q, "a", ffKind),
      seed,
      ra.isSuccess || ra.isError,
    ),
    fb = useCompareData(
      compareSideParams(q, "b", ffKind),
      seed,
      rb.isSuccess || rb.isError,
    );
  const shot = (side: "a" | "b") => {
    const s = compareSideParams(q, side, "shooting");
    s.set("teams", q.entity);
    return s;
  };
  const sa = useCompareData(
      shot("a"),
      seed,
      teams && (fa.isSuccess || fa.isError),
    ),
    sb = useCompareData(shot("b"), seed, teams && (fb.isSuccess || fb.isError));
  const ratingsReady = comparePairReady(ra, rb),
    version = ratingsReady ? ra.data!.meta.version : undefined,
    ffReady = comparePairReady(fa, fb, version),
    shotsReady = teams ? comparePairReady(sa, sb, version) : ratingsReady;
  const pick = (rows: Row[] | undefined) =>
    rows?.find(
      (r) => String(teams ? r.team_id : r.sub_lineup_hash) === q.entity,
    ) ?? null;
  const a = ratingsReady ? pick(ra.data?.rows) : null,
    b = ratingsReady ? pick(rb.data?.rows) : null,
    aff = ffReady ? pick(fa.data?.rows) : null,
    bff = ffReady ? pick(fb.data?.rows) : null;
  const shootingA = shotsReady
      ? shootingRates(teams ? (sa.data?.rows ?? []) : a ? [a] : [])
      : null,
    shootingB = shotsReady
      ? shootingRates(teams ? (sb.data?.rows ?? []) : b ? [b] : [])
      : null;
  const statuses = [
    ["A ratings", ra],
    ["B ratings", rb],
    ["A four factors", fa],
    ["B four factors", fb],
    ...(teams
      ? [
          ["A shooting", sa],
          ["B shooting", sb],
        ]
      : []),
  ] as [string, typeof ra][];
  const versions = new Set(
    statuses
      .filter(([, read]) => read.isSuccess && !read.isFetching)
      .map(([, read]) => read.data!.meta.version),
  );
  const sections = detailSections(
    teams,
    [a, b],
    [aff, bff],
    [shootingA, shootingB],
  );
  return (
    <section className="detail-container" aria-label="Entity comparison detail">
      <h3 className="cmp-team-header">{name}</h3>
      <div className="cmp-team-subheader">{descriptions.join(" vs ")}</div>
      {ratingsReady && <CompareContext a={a} b={b} teams={teams} />}
      {statuses.map(([label, query]) => (
        <CompareStatus key={label} label={label} query={query} />
      ))}
      {versions.size > 1 && (
        <div role="alert">
          The detail data changed between reads.{" "}
          <button
            type="button"
            onClick={() => {
              // Keep refresh fan-out at two, including when all sections need updating.
              for (const side of ["A", "B"])
                void (async () => {
                  for (const [label, read] of statuses)
                    if (label.startsWith(side)) await read.refetch();
                })();
            }}
          >
            Refresh detail
          </button>
        </div>
      )}
      <CompareGrid
        sections={sections}
        labels={labels}
        reverse={reverseGap(q.preset)}
      />
      <p className="ff-impact-legend">{ui.ffImpactLegend}</p>
    </section>
  );
}
export function TeamPlayerDetail({
  row,
  q,
  labels,
}: {
  row: Row;
  q: Query;
  labels: [string, string];
}) {
  const sides = ["a", "b"] as const;
  const raw = sides.map(
    (s) =>
      Object.fromEntries(
        (data.TEAM_PLAYER_RAW_COLS as string[]).map((k) => [
          k,
          row[`${k}_${s}`],
        ]),
      ) as Row,
  );
  const specs = data.detail_specs as Record<string, DetailSection>;
  const sections = Object.values(specs).map((section) => ({
    title: section.title,
    metrics: section.metrics.map((m) => {
      const value = (i: number) =>
        row[`played_${sides[i]}`] === false
          ? 0
          : m.raw || m.pct
            ? num(raw[i][m.col!])
            : playerMetric(raw[i], m.col!, q.team_rate);
      return {
        definition: { ...m, fmt: m.raw && m.col === "gp" ? "int" : m.fmt },
        a: value(0),
        b: value(1),
      };
    }),
  }));
  return (
    <section className="detail-container" aria-label="Player comparison detail">
      <h3 className="cmp-team-header">{row.player_name}</h3>
      <div className="cmp-team-subheader">
        {row.team_name} | {q.team_rate}
      </div>
      <CompareContext
        a={row.played_a ? raw[0] : null}
        b={row.played_b ? raw[1] : null}
        player
      />
      <CompareGrid
        sections={sections}
        labels={labels}
        reverse={reverseGap(q.preset)}
        player
      />
    </section>
  );
}
export function PlayerVersus({
  q,
  seed,
  a,
  b,
  names,
  teams,
  teamIds,
  version,
  refresh,
}: {
  q: Query;
  seed: CompareSeed;
  a: Row | null;
  b: Row | null;
  names: [string, string];
  teams: [string, string];
  teamIds: [string, string];
  version: string;
  refresh: () => Promise<unknown>;
}) {
  const self = q.player_compare === "self",
    ids = [q.player_a, self ? q.player_a : q.player_b];
  const params = (side: "a" | "b", kind: string) => {
    const s = compareSideParams(q, side, kind);
    const n = side === "a" ? 0 : 1,
      explicit = teamIds[n];
    if (explicit) s.set("teams", explicit);
    else s.delete("teams");
    return { s, n };
  };
  const ffA = useCompareData(
      params("a", "player_ff").s,
      seed,
      q.player_view === "ff_swing" && !!a,
    ),
    ffB = useCompareData(
      params("b", "player_ff").s,
      seed,
      q.player_view === "ff_swing" && !!b,
    );
  const onA = useCompareData(
      params("a", "onoff").s,
      seed,
      (q.player_view === "shot_profile" || ffA.isSuccess || ffA.isError) && !!a,
    ),
    onB = useCompareData(
      params("b", "onoff").s,
      seed,
      (q.player_view === "shot_profile" || ffB.isSuccess || ffB.isError) && !!b,
    );
  const pick = (rows: Row[] | undefined, i: number) =>
    rows?.find((r) => String(r.player_id) === ids[i]) ?? null;
  const ffReady = comparePairReady(ffA, ffB, version),
    onReady = comparePairReady(onA, onB, version);
  const ffs = ffReady
      ? [pick(ffA.data?.rows, 0), pick(ffB.data?.rows, 1)]
      : [null, null],
    ons = onReady
      ? [pick(onA.data?.rows, 0), pick(onB.data?.rows, 1)]
      : [null, null];
  const trad = [a, b];
  const activeReads =
    q.player_view === "ff_swing"
      ? [ffA, ffB, onA, onB]
      : q.player_view === "shot_profile"
        ? [onA, onB]
        : [];
  const versions = new Set([
    version,
    ...activeReads
      .filter((read) => read.isSuccess && !read.isFetching)
      .map((read) => read.data!.meta.version),
  ]);
  let sections: { title: string; metrics: GridMetric[] }[] = [];
  if (q.player_view === "overall")
    sections = [
      {
        title: "Overall",
        metrics: (data.PVP_STATS as MetricDef[]).map((m) => ({
          definition: {
            ...m,
            fmt: m.type === "pct" ? "num" : q.rate === "Totals" ? "int" : "num",
          },
          a: playerMetric(a, m.col!, q.rate),
          b: playerMetric(b, m.col!, q.rate),
        })),
      },
    ];
  else if (q.player_view === "ff_swing") {
    const stats = data.FF_SWING_STATS as {
      label: string;
      col: string;
      side: string;
      factor?: string;
      invert?: boolean;
      requires_def_poss?: boolean;
    }[];
    sections = ["off", "def", "activity"].map((side) => ({
      title:
        side === "off"
          ? "Offensive Four Factors"
          : side === "def"
            ? "Defensive Four Factors"
            : "Defensive Activity",
      metrics: stats
        .filter((m) => m.side === side)
        .map((m) => {
          const value = (i: number) => playerSwingValue(ffs[i], ons[i], m);
          return {
            definition: {
              label: m.label,
              polarity: m.invert ? "lower" : "higher",
              fmt: "net",
              factor: m.factor,
            },
            a: value(0),
            b: value(1),
            sub: [0, 1].map((i) =>
              m.factor ? (
                <div className="ff-impact-est" key={i}>
                  {Number.isFinite(num(value(i)))
                    ? `est. ${formatCompare(num(value(i)) * Number((ui.ffImpactWeights as Record<string, number>)[m.factor]), "num", true)}${side === "def" ? " pts allowed" : " pts"}`
                    : ""}
                </div>
              ) : null,
            ) as [ReactNode, ReactNode],
          };
        }),
    }));
  } else {
    const shots = ons.map(playerShotRows),
      suffix = [
        "efg",
        "layup_share",
        "dunk_share",
        "rim_share",
        "fg3_share",
        "c3_pct3",
        "mid_share",
      ],
      labels = [
        "eFG%",
        "Lay-up%",
        "Dunk%",
        "Lay+Dunk%",
        "3PA%",
        "Corner 3 Share",
        "2PT Jumper%",
      ];
    sections = ["off", "def"].map((side) => ({
      title: `${side === "off" ? "Offensive" : "Defensive"} Shot Profile (ON - OFF)`,
      metrics: suffix.map((m, j) => ({
        definition: {
          label: labels[j],
          fmt: "net",
          polarity: j ? "neutral" : side === "off" ? "higher" : "lower",
        },
        a: playerShotSwing(shots[0], side, m).d,
        b: playerShotSwing(shots[1], side, m).d,
        sub: shots.map((r, i) =>
          Number.isFinite(num(r?.[`${side}_on_${m}`])) &&
          Number.isFinite(num(r?.[`${side}_off_${m}`])) ? (
            <div className="ibpl-pvp-sub" key={i}>
              on {formatCompare(r?.[`${side}_on_${m}`])} | off{" "}
              {formatCompare(r?.[`${side}_off_${m}`])}
            </div>
          ) : null,
        ) as [ReactNode, ReactNode],
      })),
    }));
  }
  return (
    <section aria-label="Player versus player">
      {versions.size > 1 && (
        <div role="alert">
          The player data changed between reads.{" "}
          <button
            type="button"
            onClick={() => {
              void (async () => {
                await refresh();
                await Promise.all(
                  (["a", "b"] as const).map(async (side) => {
                    if (q.player_view === "ff_swing")
                      await (side === "a" ? ffA : ffB).refetch();
                    await (side === "a" ? onA : onB).refetch();
                  }),
                );
              })();
            }}
          >
            Refresh player comparison
          </button>
        </div>
      )}
      <div className="ibpl-compare-player-head">
        {names.map((name, i) => (
          <div key={i}>
            <h3>{name}</h3>
            <p>{teams[i]}</p>
            <p>
              {formatCompare(trad[i]?.gp, "int")} GP |{" "}
              {formatCompare(num(trad[i]?.minutes) / num(trad[i]?.gp))} MPG
            </p>
          </div>
        ))}
      </div>
      {q.player_view === "ff_swing" && (
        <>
          <CompareStatus label="A four factors" query={ffA} />
          <CompareStatus label="B four factors" query={ffB} />
        </>
      )}
      {q.player_view !== "overall" && (
        <>
          <CompareStatus label="A on/off" query={onA} />
          <CompareStatus label="B on/off" query={onB} />
        </>
      )}
      {!a || !b ? (
        <p>No player data for current filters.</p>
      ) : (
        <div className="ibpl-pvp-stats">
          {sections.map((section) => (
            <Fragment key={section.title}>
              {q.player_view !== "overall" && (
                <h4 className="ibpl-pvp-section">{section.title}</h4>
              )}
              {section.metrics.map((m) => {
                const va = num(m.a),
                  vb = num(m.b),
                  neutral = m.definition.polarity === "neutral",
                  finite = Number.isFinite(va) && Number.isFinite(vb),
                  aBetter =
                    finite &&
                    (m.definition.polarity === "lower" ? va < vb : va > vb),
                  bBetter =
                    finite &&
                    (m.definition.polarity === "lower" ? vb < va : vb > va),
                  diff = Math.abs(va - vb),
                  badge =
                    !neutral && finite && diff > 0.05
                      ? formatCompare(diff, "num", true)
                      : null;
                return (
                  <div className="ibpl-pvp-row" key={m.definition.label}>
                    <div className="ibpl-pvp-a">
                      {aBetter && badge && (
                        <span className="ibpl-pvp-badge">{badge}</span>
                      )}
                      <div>
                        <span
                          className={
                            neutral || aBetter
                              ? "ibpl-pvp-win"
                              : "ibpl-pvp-lose"
                          }
                        >
                          {formatCompare(
                            m.a,
                            m.definition.fmt,
                            m.definition.fmt === "net",
                          )}
                        </span>
                        {m.sub?.[0]}
                      </div>
                    </div>
                    <div className="ibpl-pvp-label">{m.definition.label}</div>
                    <div className="ibpl-pvp-b">
                      <div>
                        <span
                          className={
                            neutral || bBetter
                              ? "ibpl-pvp-win"
                              : "ibpl-pvp-lose"
                          }
                        >
                          {formatCompare(
                            m.b,
                            m.definition.fmt,
                            m.definition.fmt === "net",
                          )}
                        </span>
                        {m.sub?.[1]}
                      </div>
                      {bBetter && badge && (
                        <span className="ibpl-pvp-badge">{badge}</span>
                      )}
                    </div>
                  </div>
                );
              })}
            </Fragment>
          ))}
        </div>
      )}
    </section>
  );
}
