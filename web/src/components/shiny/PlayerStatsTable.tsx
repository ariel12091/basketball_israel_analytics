"use client";
import { useSyncExternalStore } from "react";
import type { Row, League } from "@/rules/gameLogs";
import {
  PLAYER_LABELS,
  playerPr,
  type PlayerMode,
} from "@/rules/playerStatsTable";
import DataTable, { formatNumber, heat, type Column } from "./DataTable";
import ColumnsMenu from "./ColumnsMenu";
import { useStoredList } from "@/lib/storedList";
const EMPTY: string[] = [];
const snapshots = new Map<string, { raw: string | null; value: string[] }>();
function read(key: string) {
  try {
    const raw = localStorage.getItem(key),
      old = snapshots.get(key);
    if (old?.raw === raw) return old.value;
    const parsed = JSON.parse(raw ?? "[]"),
      value = Array.isArray(parsed)
        ? [...new Set(parsed.filter((x): x is string => typeof x === "string"))]
        : EMPTY;
    snapshots.set(key, { raw, value });
    return value;
  } catch {
    return EMPTY;
  }
}
function subscribe(notify: () => void) {
  window.addEventListener("storage", notify);
  window.addEventListener("ibpl-player-columns", notify);
  return () => {
    window.removeEventListener("storage", notify);
    window.removeEventListener("ibpl-player-columns", notify);
  };
}
export default function PlayerStatsTable({
  rows,
  league,
  mode,
}: {
  rows: Row[];
  league: League;
  mode: PlayerMode;
}) {
  const key =
    league === "il"
      ? "onoff.ts.visible_col_order.v1"
      : "onoff.euro.ts.visible_col_order.v1";
  const saved = useSyncExternalStore(
      subscribe,
      () => read(key),
      () => EMPTY,
    ),
    [hidden, setHidden] = useStoredList(`${key}.hidden`);
  const columns: Column[] = [
    { id: "team_name", label: "Team" },
    { id: "Player", label: "Player" },
  ];
  const add = (id: string, label: string, ranked = false) =>
    columns.push({
      id,
      label,
      descFirst: false,
      render: (r) => {
        const text = formatNumber(r[id], id === "gp" ? 0 : 1);
        return {
          node: text,
          text,
          bg:
            r.is_multi_team_total === true &&
            ["team_name", "Player"].includes(id)
              ? "rgba(var(--ibpl-accent-rgb), 0.18)"
              : ranked
                ? heat(r[playerPr(label)], id === "tov")
                : undefined,
        };
      },
    });
  for (const [id, label] of Object.entries(PLAYER_LABELS)) {
    if (id === "dfl" && league !== "il") continue;
    add(id, label, !["gp", "minutes", "poss_on_floor"].includes(id));
    if (id === "poss_on_floor" && mode === "Per 60 Possessions")
      add("base_poss_pg", "Poss/G");
    if (id === "minutes" && mode === "Per 30 Minutes")
      add("base_min_pg", "Min/G");
  }
  for (const c of columns.slice(0, 2))
    c.render = (r) => ({
      node: String(r[c.id] ?? ""),
      text: String(r[c.id] ?? ""),
      bg:
        r.is_multi_team_total === true
          ? "rgba(var(--ibpl-accent-rgb), 0.18)"
          : undefined,
    });
  const ordered = [
    ...saved
      .map((id) => columns.find((c) => c.id === id))
      .filter((c): c is Column => !!c),
    ...columns.filter((c) => !saved.includes(c.id)),
  ];
  const move = (from: string, to: string) => {
    const ids = ordered.map((c) => c.id),
      a = ids.indexOf(from),
      b = ids.indexOf(to);
    if (a < 0 || b < 0 || a === b) return;
    ids.splice(a, 1);
    ids.splice(b, 0, from);
    try {
      localStorage.setItem(key, JSON.stringify(ids));
    } catch {}
    window.dispatchEvent(new Event("ibpl-player-columns"));
  };
  return (
    <>
      <DataTable
        rows={rows}
        columns={ordered.filter((c) => !hidden.includes(c.id))}
        defaultOrder={[]}
        onColumnMove={move}
        tools={
          <ColumnsMenu
            columns={ordered}
            hidden={hidden}
            locked={["team_name", "Player"]}
            onMove={move}
            onHidden={setHidden}
            onReset={() => {
              try {
                localStorage.removeItem(key);
              } catch {}
              window.dispatchEvent(new Event("ibpl-player-columns"));
              setHidden([]);
            }}
          />
        }
        pageLength={50}
        csvName={`${league === "il" ? "israeli_league" : "euroleague"}_player_stats`}
        nameColumns={["Player", "team_name"]}
        moreInfo
        rowProps={(r) => ({
          className: `${r.is_multi_team_total === true ? "ibpl-player-total " : ""}${r.rate_eligible === false ? "ibpl-player-ineligible" : ""}`,
        })}
      />
    </>
  );
}
