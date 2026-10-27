"use client";
// mod_lineup_player_filter.R + app.js "Lineup player chips", driven by React.
import { useState } from "react";
import type { Row } from "@/rules/gameLogs";
import type { PlayerSelection } from "@/rules/lineups";
import { rRound } from "@/rules/round";
export default function LineupPlayers({
  roster,
  selection,
  onChange,
}: {
  roster: Row[];
  selection: PlayerSelection;
  onChange: (p: PlayerSelection) => void;
}) {
  const [mode, setMode] = useState<"on" | "any" | "off">("on");
  const [expanded, setExpanded] = useState(false);
  // app.js displayName: all-cap provider names become title case; names that
  // already have mixed case remain as supplied (including McRae etc.).
  const full = (r: Row) => {
    const name = String(r.name ?? "").trim();
    return name !== name.toUpperCase()
      ? name
      : name
          .toLowerCase()
          .replace(
            /(^|[\s\-'.])([a-z])/g,
            (_, p: string, c: string) => p + c.toUpperCase(),
          )
          .replace(/\b(Ii|Iii|Iv)\b/g, (m) => m.toUpperCase());
  };
  const last = (r: Row) =>
    full(r).includes(",")
      ? full(r).split(",")[0].trim()
      : full(r).trim().split(/\s+/).slice(1).join(" ") || String(r.name);
  const labels = roster.map((r) =>
    roster.filter((x) => last(x) === last(r)).length > 1
      ? `${(full(r).includes(",") ? full(r).split(",")[1].trim() : full(r))[0]}. ${last(r)}`
      : last(r),
  );
  const state = (id: number) =>
    selection.on.includes(id)
      ? "on"
      : selection.any.includes(id)
        ? "any"
        : selection.off.includes(id)
          ? "off"
          : "none";
  const selected =
    selection.on.length + selection.any.length + selection.off.length > 0;
  const k = Math.max(1, Math.min(selection.count, selection.any.length - 1));
  const names = (ids: number[]) =>
    roster.flatMap((r, i) =>
      ids.includes(Number(r.player_id)) ? [labels[i]] : [],
    );
  const hidden = roster.filter(
    (r, i) => i >= 12 && state(Number(r.player_id)) === "none",
  ).length;
  const tap = (id: number) => {
    const next = {
      ...selection,
      on: selection.on.filter((x) => x !== id),
      any: selection.any.filter((x) => x !== id),
      off: selection.off.filter((x) => x !== id),
    };
    if (state(id) !== mode) next[mode] = [...next[mode], id];
    next.count = Math.max(1, Math.min(k, next.any.length - 1));
    if (next.any.length < 2) next.exact = false;
    onChange(next);
  };
  return (
    <div
      className={`lineup-chips${roster.length ? "" : " is-empty"}`}
      data-mode={mode}
    >
      <div className="lineup-chips-head">
        <span className="lineup-chips-eyebrow" id="lineup-player-label">
          Players
        </span>
        <div
          className="lineup-chips-modes"
          role="radiogroup"
          aria-label="What tapping a player does"
        >
          {(["on", "any", "off"] as const).map((m) => (
            <button
              type="button"
              key={m}
              className="lineup-chips-mode"
              role="radio"
              aria-checked={mode === m}
              onClick={() => setMode(m)}
            >
              <i className={`lineup-chips-mark ${m}`} aria-hidden="true" />
              {m === "on" ? "On" : m === "any" ? "Any of" : "Off"}
            </button>
          ))}
        </div>
      </div>
      <div
        className="lineup-chips-list"
        role="group"
        aria-labelledby="lineup-player-label"
      >
        {roster.map((r, i) => {
          const id = Number(r.player_id),
            s = state(id);
          if (!expanded && i >= 12 && s === "none") return null;
          return (
            <button
              type="button"
              key={id}
              className="lineup-chip"
              data-id={id}
              data-state={s}
              aria-label={`${full(r)}, ${s === "none" ? "not selected" : s === "on" ? "must be on" : s === "any" ? "any of" : "must be off"}`}
              title={`${full(r)}${r.minutes == null ? "" : ` · ${rRound(Number(r.minutes), 0)} min this season`}`}
              onClick={() => tap(id)}
            >
              {labels[i]}
            </button>
          );
        })}
        {(hidden > 0 || (expanded && roster.length > 12)) && (
          <button
            type="button"
            className="lineup-chip lineup-chip-more"
            onClick={() => setExpanded(!expanded)}
          >
            {expanded ? "Show fewer" : `+${hidden} more`}
          </button>
        )}
      </div>
      <div className="lineup-chips-summary" aria-live="polite">
        {!roster.length ? (
          <span className="lineup-chips-hint">
            Pick a team to filter by player.
          </span>
        ) : !selected ? (
          <span className="lineup-chips-hint">
            All lineups. Pick a mode, then tap players.
          </span>
        ) : (
          <>
            <span className="lineup-chips-sentence">
              {!!selection.on.length && (
                <b className="lineup-chips-on">
                  {names(selection.on).join(" + ")}
                </b>
              )}
              {!!selection.any.length && (
                <>
                  <span className="lineup-chips-conn">
                    {selection.on.length
                      ? " · with "
                      : selection.any.length < 2
                        ? "including "
                        : ""}
                  </span>
                  {selection.any.length >= 2 && (
                    <>
                      <button
                        type="button"
                        className="lineup-chips-token lineup-chips-quant"
                        aria-label="Switch between at least and exactly"
                        onClick={() =>
                          onChange({ ...selection, exact: !selection.exact })
                        }
                      >
                        {selection.exact ? "exactly" : "at least"}
                      </button>{" "}
                      {selection.any.length < 3 ? (
                        "1"
                      ) : (
                        <button
                          type="button"
                          className="lineup-chips-token lineup-chips-count"
                          aria-label="Change any-of count"
                          onClick={() =>
                            onChange({
                              ...selection,
                              count: (k % (selection.any.length - 1)) + 1,
                            })
                          }
                        >
                          {k}
                        </button>
                      )}
                      {" of "}
                    </>
                  )}
                  <span className="lineup-chips-any">
                    {names(selection.any).join(", ")}
                  </span>
                </>
              )}
              {!!selection.off.length && (
                <>
                  <span className="lineup-chips-conn">
                    {selection.on.length || selection.any.length ? " · " : ""}
                    without{" "}
                  </span>
                  <span className="lineup-chips-off">
                    {names(selection.off).join(", ")}
                  </span>
                </>
              )}
            </span>
            <button
              type="button"
              className="lineup-chips-clear"
              onClick={() =>
                onChange({ on: [], any: [], off: [], count: 1, exact: false })
              }
            >
              Clear
            </button>
          </>
        )}
      </div>
    </div>
  );
}
