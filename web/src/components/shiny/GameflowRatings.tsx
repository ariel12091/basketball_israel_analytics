"use client";
import { useState, type PointerEvent } from "react";
import type { GameflowPayload } from "@/server/gameflowRepo";
import {
  RATING_MIN_POSS,
  RATING_WINDOW,
  clock,
  ratingAt,
  type GameflowLayout,
  type RatingPoint,
} from "@/rules/gameflow";
import { rRound } from "@/rules/round";
import geometry from "@/data/gameflowLayout.json";

/** Plot height in drawing units, and the gap above and below it. */
const PLOT_H = 96,
  PAD = 10;
type Mode = "game" | "window";
type Key = "ortg" | "drtg";

const fmt = (v: number | null) => (v == null ? "–" : rRound(v, 1).toFixed(1));

/** A step line through the points that carry a value; gaps stay gaps. */
function stepPath(
  points: RatingPoint[],
  key: Key,
  x: (t: number) => number,
  y: (v: number) => number,
  close: number,
) {
  let d = "",
    open = false;
  for (const p of points) {
    const v = p[key];
    if (v == null) {
      open = false;
      continue;
    }
    d += open
      ? `H${x(p.elapsed).toFixed(1)}V${y(v).toFixed(1)}`
      : `M${x(p.elapsed).toFixed(1)},${y(v).toFixed(1)}`;
    open = true;
  }
  return open ? `${d}H${x(close).toFixed(1)}` : d;
}

/**
 * Offensive and defensive rating through the game, under the Gameflow chart
 * on the same time axis. A trial (user 2026-11-07): both versions are offered
 * so they can be compared before one is kept.
 */
export default function GameflowRatings({
  data,
  compact,
  layout,
}: {
  data: GameflowPayload;
  compact: boolean;
  layout?: GameflowLayout;
}) {
  const [mode, setMode] = useState<Mode>("game"),
    [at, setAt] = useState<number | null>(null);
  if (!data.ratings) return null;
  const series = data.ratings[mode],
    L = compact ? geometry.compact : (layout ?? geometry.desktop),
    total = data.bounds[data.bounds.length - 1],
    scale = (L.width - L.gutter) / total,
    x = (t: number) => L.gutter + t * scale,
    last = series[series.length - 1]?.elapsed ?? total,
    values = series.flatMap((p) =>
      [p.ortg, p.drtg].filter((v): v is number => v != null),
    ),
    lo = Math.max(0, Math.floor(Math.min(80, ...values) / 20) * 20),
    hi = Math.ceil(Math.max(140, ...values) / 20) * 20,
    y = (v: number) => PAD + ((hi - v) / (hi - lo)) * PLOT_H,
    ticks = Array.from(
      { length: (hi - lo) / 20 + 1 },
      (_, i) => lo + i * 20,
    ),
    height = PLOT_H + 2 * PAD + 12,
    shown = ratingAt(series, at ?? last),
    periodOf = (t: number) => {
      const p = data.bounds.findIndex((b) => b > t),
        i = p < 0 ? data.bounds.length - 1 : p;
      return `${i < 4 ? `Q${i + 1}` : `OT${i - 3}`} ${clock(Math.ceil(data.bounds[i] - t))}`;
    };
  const track = (e: PointerEvent<SVGRectElement>) => {
    const r = e.currentTarget.getBoundingClientRect(),
      f = Math.max(0, Math.min(1, (e.clientX - r.left) / r.width));
    setAt(f * last);
  };
  const help =
    mode === "game"
      ? `Points per 100 possessions since tip-off. Drawn from the ${RATING_MIN_POSS}th possession; ends on the box-score rating.`
      : `Points per 100 possessions over each side's last ${RATING_WINDOW} possessions, so runs show. Drawn from the ${RATING_WINDOW}th possession.`;
  return (
    <section className="ibpl-gameflow-ratings" aria-label="Ratings through the game">
      <div className="ibpl-gameflow-ratings-head">
        <span className="ibpl-gameflow-ratings-title">Ratings</span>
        <div className="ibpl-gameflow-ratings-modes" role="group" aria-label="Rating window">
          {(
            [
              ["game", "So far"],
              ["window", `Last ${RATING_WINDOW} poss`],
            ] as const
          ).map(([m, label]) => (
            <button
              key={m}
              type="button"
              aria-pressed={mode === m}
              className={mode === m ? "is-on" : undefined}
              onClick={() => setMode(m)}
            >
              {label}
            </button>
          ))}
        </div>
        <span className="ibpl-gameflow-ratings-legend">
          {at != null && <span className="ibpl-gameflow-ratings-when">{periodOf(at)}</span>}
          <span className="is-off">
            {data.meta.ownTeam} offense <b>{fmt(shown.ortg)}</b>
          </span>
          <span className="is-def">
            defense <b>{fmt(shown.drtg)}</b>
          </span>
        </span>
      </div>
      <p className="ibpl-gameflow-ratings-help">{help}</p>
      <svg
        className={`ibpl-ribbon ibpl-gameflow-ratings-chart${compact ? " is-compact" : ""}`}
        viewBox={`0 0 ${L.width} ${height}`}
        width={compact ? L.width : undefined}
        role="img"
        aria-label={`${data.meta.ownTeam} offensive rating ${fmt(shown.ortg)}, defensive rating ${fmt(shown.drtg)}`}
      >
        {ticks.map((v) => (
          <g key={v}>
            <line className="ibpl-ribbon-scale" x1={L.gutter} x2={L.width} y1={y(v)} y2={y(v)} />
            <text className="ibpl-ribbon-scale-label" x={L.gutter - 8} y={y(v) + 3} textAnchor="end">
              {v}
            </text>
          </g>
        ))}
        {data.bounds.map((bound, i) => {
          const opening = i ? data.bounds[i - 1] : 0;
          return (
            <g key={bound}>
              {i > 0 && (
                <line className="ibpl-ribbon-period" x1={x(opening)} x2={x(opening)} y1={PAD} y2={PAD + PLOT_H} />
              )}
              <text className="ibpl-ribbon-period-label" x={x(opening) + 4} y={PAD + PLOT_H + 12}>
                {i < 4 ? `Q${i + 1}` : `OT${i - 3}`}
              </text>
            </g>
          );
        })}
        <path className="ibpl-gameflow-rating-line is-def" d={stepPath(series, "drtg", x, y, last)} />
        <path className="ibpl-gameflow-rating-line is-off" d={stepPath(series, "ortg", x, y, last)} />
        {at != null && (
          <line className="ibpl-gameflow-rating-cursor" x1={x(at)} x2={x(at)} y1={PAD} y2={PAD + PLOT_H} />
        )}
        <rect
          x={L.gutter}
          y={PAD}
          width={Math.max(0, last * scale)}
          height={PLOT_H}
          fill="transparent"
          pointerEvents="all"
          onPointerMove={track}
          onPointerDown={track}
          onPointerLeave={() => setAt(null)}
        />
      </svg>
    </section>
  );
}
