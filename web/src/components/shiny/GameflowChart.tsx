"use client";
import { Fragment, useId, useState, type PointerEvent } from "react";
import type { GameflowPayload } from "@/server/gameflowRepo";
import {
  clock,
  laneKey,
  marginPath,
  marginScale,
  pmLabel,
  shortLabels,
  scoreAt,
  windowDetail,
  windowSegments,
  type Bar,
  type Window,
} from "@/rules/gameflow";
import geometry from "@/data/gameflowLayout.json";
export type BarWindow = { bar: Bar; start: number; end: number };
export default function GameflowChart({
  data,
  compact = false,
  range,
  selected,
  highlight,
  onPlayer,
  onBar,
  onHover,
  active,
  lineupIndex,
}: {
  data: GameflowPayload;
  compact?: boolean;
  range?: Window;
  selected: string[];
  highlight: Window[];
  onPlayer: (bar: Bar) => void;
  onBar: (window: BarWindow) => void;
  onHover: (window: BarWindow | null) => void;
  active: BarWindow | null;
  lineupIndex?: number;
}) {
  const id = useId().replace(/:/g, ""),
    L = compact ? geometry.compact : geometry.desktop,
    total = data.bounds[data.bounds.length - 1],
    start = range?.start ?? 0,
    end = range?.end ?? total,
    scale = (L.width - L.gutter) / total,
    width = L.gutter + (end - start) * scale;
  const bars = data.bars.filter(
      (b) => b.end_elapsed > start && b.start_elapsed < end,
    ),
    players = (["own", "opp"] as const).flatMap((side) => {
      const first = new Map<string, Bar>();
      for (const b of bars.filter((b) => b.side === side))
        if (!first.has(laneKey(b))) first.set(laneKey(b), b);
      return [...first.values()].sort((a, b) => a.lane_index - b.lane_index);
    });
  const own = players.filter((p) => p.side === "own"),
    opp = players.filter((p) => p.side === "opp"),
    pitch = L.lane_height + L.lane_gap,
    ownH = Math.max(0, own.length * pitch - L.lane_gap),
    marginTop = geometry.padTop + geometry.header + ownH + geometry.bandGap,
    oppTop = marginTop + L.margin_height + geometry.bandGap,
    totalH = oppTop + Math.max(0, opp.length * pitch - L.lane_gap),
    height = totalH + 12 + geometry.padBottom;
  const y = (b: Bar) =>
    (b.side === "own" ? geometry.padTop + geometry.header : oppTop) +
    (b.side === "own" ? own : opp).findIndex(
      (p) => p.player_key === b.player_key,
    ) *
      pitch;
  const x = (time: number) => L.gutter + (time - start) * scale;
  const labels = shortLabels(
    players.map((p) => p.player_label),
    players.map((p) => p.side),
  );
  const scaleInfo = marginScale(data.margin),
    marginY = (v: number) =>
      marginTop +
      L.margin_height / 2 -
      ((v / scaleInfo.maxAbs) * L.margin_height) / 2;
  let margin = data.margin.filter(
    (r) => r.elapsed >= start && r.elapsed <= end,
  );
  const previous = [...data.margin].reverse().find((r) => r.elapsed < start);
  if (previous && data.margin[data.margin.length - 1].elapsed >= start)
    margin = [{ elapsed: start, margin: previous.margin }, ...margin];
  const after = data.margin.find((r) => r.elapsed > end),
    last = margin[margin.length - 1];
  if (after && last && last.elapsed < end)
    margin = [...margin, { elapsed: end, margin: last.margin }];
  const path = marginPath(
    margin.map((r) => ({ ...r, elapsed: r.elapsed - start })),
    end - start,
    width,
    marginTop,
    L.margin_height,
    L.gutter,
    scaleInfo.maxAbs,
  );
  const [scoreTip, setScoreTip] = useState<{
    x: number;
    y: number;
    elapsed: number;
  } | null>(null);
  const showScore = (event: PointerEvent<SVGRectElement>) => {
    const rect = event.currentTarget.getBoundingClientRect(),
      elapsed =
        start +
        Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width)) *
          (Math.min(end, data.margin[data.margin.length - 1]?.elapsed ?? end) -
            start);
    setScoreTip({
      x: Math.min(innerWidth - 270, Math.max(8, event.clientX + 12)),
      y: Math.max(8, event.clientY - 70),
      elapsed,
    });
  };
  const clipped = (windows: Window[]) =>
    windows.flatMap((w) =>
      w.end > start && w.start < end
        ? [{ start: Math.max(start, w.start), end: Math.min(end, w.end) }]
        : [],
    );
  const focused = selected.length > 0 || active != null;
  const pinned = compact && !range;
  const gutter = (
    <>
      {players.map((b, i) => {
        const key = laneKey(b),
          stats = data.totals.find((r) => laneKey(r) === key),
          label = compact ? labels[i] : b.player_label,
          chosen = selected.includes(key);
        return (
          <g key={key} className={chosen ? "is-pair-selected" : ""}>
            <rect
              className={`ibpl-ribbon-row-btn${chosen ? " is-pair-selected" : ""}`}
              x={2}
              y={y(b)}
              width={L.gutter - 6}
              height={L.lane_height}
              rx={3}
              onClick={() => onPlayer(b)}
            />
            <text
              className={`ibpl-ribbon-name${chosen ? " is-pair-selected" : ""}`}
              x={range ? L.gutter - 6 : L.name_x}
              y={y(b) + L.lane_height / 2 + 4}
              textAnchor="end"
              role="button"
              tabIndex={0}
              aria-pressed={chosen}
              aria-label={`${b.player_label}${b.is_starter ? ", starter" : ""}, ${clock(stats?.secs ?? 0)} on the floor, plus-minus ${pmLabel(stats?.pm ?? null)}`}
              onClick={() => onPlayer(b)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onPlayer(b);
                }
              }}
            >
              {label}
            </text>
            {!compact && (
              <text
                className="ibpl-ribbon-min"
                x={geometry.desktop.min_x}
                y={y(b) + L.lane_height / 2 + 4}
                textAnchor="end"
              >
                {clock(stats?.secs ?? 0)}
              </text>
            )}
            {!range && (
              <text
                className="ibpl-ribbon-pm"
                x={L.pm_x}
                y={y(b) + L.lane_height / 2 + 4}
                textAnchor="end"
              >
                {pmLabel(stats?.pm ?? null)}
              </text>
            )}
          </g>
        );
      })}
      <text className="ibpl-ribbon-team" x={0} y={geometry.padTop + 10}>
        {data.meta.ownTeam}
      </text>
      {!compact && (
        <>
          <text
            className="ibpl-ribbon-col-head"
            x={geometry.desktop.min_x}
            y={geometry.padTop + 10}
            textAnchor="end"
          >
            MIN
          </text>
          <text
            className="ibpl-ribbon-col-head"
            x={L.pm_x}
            y={geometry.padTop + 10}
            textAnchor="end"
          >
            +/-
          </text>
        </>
      )}
      <text
        className="ibpl-ribbon-team"
        x={0}
        y={oppTop - geometry.header + 10}
      >
        {data.meta.oppTeam}
      </text>
      <text
        className="ibpl-ribbon-zero-label"
        x={L.gutter - 8}
        y={marginY(0) + 3}
        textAnchor="end"
      >
        tied
      </text>
      {scaleInfo.ticks.map((v) => (
        <Fragment key={v}>
          {" "}
          <text
            className="ibpl-ribbon-scale-label"
            x={L.gutter - 8}
            y={marginY(v) + 3}
            textAnchor="end"
          >
            {Math.abs(v)}
          </text>
        </Fragment>
      ))}
    </>
  );
  return (
    <>
      {pinned && (
        <svg
          className="ibpl-gameflow-gutter ibpl-ribbon is-compact"
          viewBox={`0 0 ${L.gutter} ${height}`}
          width={L.gutter}
          height={height}
          aria-label="Players and margin scale"
        >
          <rect
            className="ibpl-ribbon-pin-bg"
            width={L.gutter}
            height={height}
          />
          {gutter}
        </svg>
      )}
      <svg
        className={`ibpl-ribbon${compact ? " is-compact" : ""}${selected.length ? " is-pair-focused" : active ? " is-focused" : ""}${lineupIndex != null ? " is-lineup-focused" : ""}`}
        viewBox={`0 0 ${width} ${height}`}
        width={compact ? width : undefined}
        role="img"
        aria-label={`${data.meta.ownTeam} vs ${data.meta.oppTeam}${range ? `, ${start < 2400 ? "Q" + (Math.floor(start / 600) + 1) : "OT" + (Math.floor((start - 2400) / 300) + 1)}` : ""}`}
        data-base-height={height}
      >
        <defs>
          <clipPath id={`${id}-focus`}>
            {clipped(highlight).map((w, i) => (
              <rect
                key={i}
                x={x(w.start)}
                y={marginTop}
                width={(w.end - w.start) * scale}
                height={L.margin_height}
              />
            ))}
          </clipPath>
        </defs>
        {active &&
          clipped([{ start: active.start, end: active.end }]).map((w, i) => (
            <rect
              key={i}
              className="ibpl-ribbon-hover-band"
              x={x(w.start)}
              y={geometry.padTop}
              width={(w.end - w.start) * scale}
              height={totalH - geometry.padTop}
            />
          ))}
        {!pinned && gutter}
        <line
          className="ibpl-ribbon-zero"
          x1={L.gutter}
          x2={width}
          y1={marginY(0)}
          y2={marginY(0)}
        />
        {scaleInfo.ticks.map((v) => (
          <g key={v}>
            <line
              className="ibpl-ribbon-scale"
              x1={L.gutter}
              x2={width}
              y1={marginY(v)}
              y2={marginY(v)}
            />
          </g>
        ))}
        <path className="ibpl-ribbon-margin-base" d={path} />
        <path
          className="ibpl-ribbon-margin-focus"
          d={path}
          clipPath={focused ? `url(#${id}-focus)` : undefined}
        />
        {bars.map((b, i) => {
          const from = Math.max(start, b.start_elapsed),
            to = Math.min(end, b.end_elapsed),
            detail = windowDetail(
              data.lanes,
              data.steps,
              data.dict,
              b,
              from,
              to,
            ),
            w = Math.max(0.75, (to - from) * scale),
            key = laneKey(b),
            chosen = selected.includes(key),
            window = { bar: b, start: from, end: to };
          const segments =
            chosen || active?.bar === b || lineupIndex != null
              ? windowSegments(
                  data.lanes,
                  data.steps,
                  data.dict,
                  b.side,
                  b.player_key,
                  from,
                  to,
                )
              : [];
          return (
            <g
              key={`${key}:${b.start_elapsed}:${i}`}
              className={`ibpl-ribbon-lane is-${b.side}${chosen ? " is-pair-selected" : ""}${active?.bar === b ? " is-active" : ""}`}
              data-player-key={key}
              data-start={from}
              data-end={to}
              tabIndex={0}
              role="button"
              aria-pressed={chosen}
              aria-label={detail.label}
              onClick={() => onBar(window)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onBar(window);
                }
              }}
              onPointerEnter={() => onHover(window)}
              onPointerLeave={() => onHover(null)}
              onFocus={() => onHover(window)}
              onBlur={() => onHover(null)}
            >
              <rect
                x={x(from)}
                y={y(b)}
                width={w}
                height={L.lane_height}
                rx={2}
              />
              {!segments.length &&
                detail.pm &&
                w >=
                  detail.pm.length *
                    geometry.numberAdvance *
                    geometry.numberFont +
                    geometry.numberPad && (
                  <text
                    className="ibpl-ribbon-num"
                    x={x(from) + w / 2}
                    y={y(b) + L.lane_height / 2 + 3}
                    textAnchor="middle"
                  >
                    {detail.pm}
                  </text>
                )}
              {segments.length > 0 && (
                <g className="ibpl-ribbon-selection-overlay">
                  {segments.map((s, j) => {
                    const sw = (s.end - s.start) * scale,
                      text = pmLabel(s.pm),
                      focus = lineupIndex === s.dictIndex;
                    return (
                      <Fragment key={j}>
                        {focus && (
                          <rect
                            className="ibpl-ribbon-lineup-mark is-primary"
                            x={x(s.start)}
                            y={y(b)}
                            width={sw}
                            height={L.lane_height}
                          />
                        )}
                        {j > 0 && (
                          <line
                            className="ibpl-ribbon-segment-divider"
                            x1={x(s.start)}
                            x2={x(s.start)}
                            y1={y(b)}
                            y2={y(b) + L.lane_height}
                          />
                        )}{" "}
                        {text &&
                          sw >=
                            text.length *
                              geometry.numberAdvance *
                              geometry.numberFont +
                              geometry.numberPad && (
                            <text
                              className="ibpl-ribbon-segment-num"
                              x={x(s.start) + sw / 2}
                              y={y(b) + L.lane_height / 2 + 3}
                              textAnchor="middle"
                            >
                              {text}
                            </text>
                          )}
                      </Fragment>
                    );
                  })}
                </g>
              )}
            </g>
          );
        })}
        {data.bounds.map((bound, i) => {
          const opening = i ? data.bounds[i - 1] : 0;
          if (bound <= start || opening >= end) return null;
          const label = i < 4 ? `Q${i + 1}` : `OT${i - 3}`;
          return (
            <g key={bound}>
              {opening > start && (
                <line
                  className="ibpl-ribbon-period"
                  x1={x(opening)}
                  x2={x(opening)}
                  y1={geometry.padTop}
                  y2={totalH}
                />
              )}{" "}
              {(!compact || !range) && (
                <text
                  className="ibpl-ribbon-period-label"
                  x={x(Math.max(opening, start)) + 4}
                  y={geometry.padTop + 10}
                >
                  {label}
                </text>
              )}
              <text
                className="ibpl-ribbon-period-label"
                x={x(Math.max(opening, start)) + 4}
                y={marginTop + 11}
              >
                {label}
              </text>
              <text
                className="ibpl-ribbon-period-label"
                x={x(Math.max(opening, start)) + 4}
                y={totalH + 12}
              >
                {label}
              </text>
            </g>
          );
        })}
        {path && data.steps.some((s) => s.own != null) && (
          <rect
            className="ibpl-ribbon-margin-hit"
            x={L.gutter}
            y={marginTop}
            width={Math.max(
              0,
              (Math.min(end, data.margin[data.margin.length - 1].elapsed) -
                start) *
                scale,
            )}
            height={L.margin_height}
            fill="transparent"
            pointerEvents="all"
            aria-label="Hover or tap to see the score"
            onPointerMove={showScore}
            onPointerDown={showScore}
            onPointerLeave={() => setScoreTip(null)}
          />
        )}
      </svg>
      {scoreTip && (
        <div
          className="ibpl-ribbon-score-tooltip"
          role="tooltip"
          style={{ left: scoreTip.x, top: scoreTip.y }}
        >
          {(() => {
            const p = data.bounds.findIndex((b) => b > scoreTip.elapsed),
              period = p < 0 ? data.bounds.length - 1 : p;
            return `${period < 4 ? `Q${period + 1}` : `OT${period - 3}`} ${clock(Math.ceil(data.bounds[period] - scoreTip.elapsed))}\n${data.meta.ownTeam} ${scoreAt(data.steps, scoreTip.elapsed, "own")} – ${scoreAt(data.steps, scoreTip.elapsed, "own") - scoreAt(data.steps, scoreTip.elapsed)} ${data.meta.oppTeam}`;
          })()}
        </div>
      )}
    </>
  );
}
