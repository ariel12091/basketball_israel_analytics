"use client";
import { useState } from "react";
import Link from "next/link";
import {
  homeDashboard,
  homeFixed,
  homeOrdinal,
  homeSprintf,
} from "@/rules/home";
import { homeTabHref, homeStoryHref } from "@/lib/homeQuery";
import ui from "@/data/homeUi.json";
import GameflowPanel from "./GameflowPanel";
type Dashboard = ReturnType<typeof homeDashboard>;

/** The always-visible part of the hub: load errors and the team ratings card. */
export function HubIdentity({
  season,
  dashboard,
  partial,
  loading,
  error,
  retry,
}: {
  season: string;
  dashboard: Dashboard | null;
  partial: boolean;
  loading: boolean;
  error: string;
  retry: () => void;
}) {
  const t = ui.text;
  return (
    <div className="home-identity" aria-busy={loading}>
      {error && (
        <div role="alert">
          {error}{" "}
          <button type="button" className="btn btn-default" onClick={retry}>
            Retry
          </button>
        </div>
      )}
      {partial && (
        <div role="alert">
          Some Home sections are unavailable.{" "}
          <button type="button" className="btn btn-default" onClick={retry}>
            Retry
          </button>
        </div>
      )}
      <div id="hub_identity">
        {dashboard?.identity ? (
          <Identity data={dashboard} season={season} />
        ) : (
          loading && <p role="status">{t.loading}</p>
        )}
      </div>
    </div>
  );
}

/** The rest of the hub, shown under "See more". */
export default function HomeHub({
  season,
  team,
  dashboard,
  loading,
  error,
}: {
  season: string;
  team: string;
  dashboard: Dashboard | null;
  loading: boolean;
  error: string;
}) {
  const [gameflow, openGameflow] = useState(false),
    t = ui.text;
  const g = dashboard?.lastGame;
  return (
    <div id="team_hub_section" aria-busy={loading}>
      <div id="hub_last_game">
        {g && (
          <div className="card bg-dark border-secondary mb-4 hub-card-static hub-last-game">
            <div className="card-body">
              <span className="hub-block-title">{t.lastGame}</span>
              <span className={g.row.has_won ? "hub-pos" : "hub-neg"}>
                {g.result}
              </span>
              <span className="hub-last-game-context">{g.context}</span>
              {g.row.has_gameflow === true && (
                <button
                  className="btn btn-link ibpl-home-gameflow"
                  type="button"
                  onClick={() => openGameflow(true)}
                >
                  {t.gameflow}
                </button>
              )}
            </div>
          </div>
        )}
      </div>
      <div id="hub_ribbon_inline_panel">
        {gameflow && g?.row.has_gameflow === true && (
          <GameflowPanel
            league="il"
            game={Number(g.row.game_id)}
            team={Number(g.row.team_id)}
            onClose={() => openGameflow(false)}
          />
        )}
      </div>
      <div className="row" style={{ alignItems: "stretch" }}>
        <div className="col-sm-6">
          <div id="hub_players" className="shiny-html-output">
            {dashboard && (dashboard.players || dashboard.scorer) && (
              <Link
                prefetch={false}
                href={homeTabHref("il", season, "go_onoff", team)}
                className="card bg-dark border-secondary mb-4 h-100 hub-card"
              >
                <div className="card-body">
                  <h6 className="hub-block-title">{t.players}</h6>
                  {dashboard.players && (
                    <ul className="hub-list">
                      {dashboard.players.map((p) => (
                        <li
                          key={`${p.team_id}:${p.player_id ?? `${p["First Name"]}:${p["Last Name"]}`}`}
                        >
                          <span className="hub-player-name">
                            {p.Player ?? `${p["First Name"]} ${p["Last Name"]}`}
                          </span>
                          <span
                            className={
                              Number(p["Net RTG Diff"]) >= 0
                                ? "hub-pos"
                                : "hub-neg"
                            }
                          >
                            {homeFixed(p["Net RTG Diff"], true)} / 100
                          </span>
                        </li>
                      ))}
                    </ul>
                  )}
                  {dashboard.scorer && (
                    <p className="hub-footnote">
                      {homeSprintf(
                        t.scorer,
                        ui.topScorerMinGp,
                        dashboard.scorer.player_name ?? dashboard.scorer.Player,
                        dashboard.scorer.ppg,
                      )}
                    </p>
                  )}
                  <div className="hub-card-go">{t.onoffLink}</div>
                </div>
              </Link>
            )}
          </div>
        </div>
        <div className="col-sm-6">
          <div id="hub_lineups" className="shiny-html-output">
            {dashboard?.lineups && (
              <Link
                prefetch={false}
                href={homeTabHref("il", season, "go_lineups", team)}
                className="card bg-dark border-secondary mb-4 h-100 hub-card"
              >
                <div className="card-body">
                  <h6 className="hub-block-title">{t.lineups}</h6>
                  {(["best", "worst"] as const).map((key) => {
                    const row = dashboard.lineups![key],
                      color = key === "best" ? "hub-pos" : "hub-neg";
                    return (
                      <div className="hub-lineup" key={key}>
                        <span className={`hub-lineup-tag ${color}`}>
                          {t[key]}
                        </span>
                        <span className="hub-lineup-players">
                          {row.player_names_str}
                        </span>
                        <span className={color}>
                          {homeFixed(row.net_rtg, true)} net,{" "}
                          {Math.trunc(Number(row.total_poss))} poss
                        </span>
                      </div>
                    );
                  })}
                  <div className="hub-card-go">{t.lineupsLink}</div>
                </div>
              </Link>
            )}
          </div>
        </div>
      </div>
      <div className="hub-storylines-shell">
        <div id="hub_storylines" className="hub-storylines-output">
          <div className="card bg-dark border-secondary mb-4 hub-card-static">
            <div className="card-body">
              <h6 className="hub-block-title">{t.storylines}</h6>
              {loading && !dashboard ? (
                <p role="status">{t.loading}</p>
              ) : dashboard?.storylines.length ? (
                dashboard.storylines.map((line) => (
                  <Link
                    prefetch={false}
                    className="hub-story-line"
                    key={line.id}
                    href={homeStoryHref(season, line.preset, team)}
                  >
                    {line.text}
                  </Link>
                ))
              ) : (
                !error && (
                  <p className="hub-storylines-empty mb-0">
                    {t.emptyStorylines}
                  </p>
                )
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
function Identity({
  data,
  season,
}: {
  data: ReturnType<typeof homeDashboard>;
  season: string;
}) {
  const info = data.identity!,
    row = info.row,
    t = ui.text;
  const stat = (
    label: string,
    value: unknown,
    rank: unknown,
    signed = false,
  ) => (
    <div className="hub-stat" key={label}>
      <div className="hub-stat-value">{homeFixed(value, signed)}</div>
      <div className="hub-stat-label">{label}</div>
      <div className="hub-stat-rank">
        {homeOrdinal(rank)} of {info.n_teams}
      </div>
    </div>
  );
  return (
    <Link
      prefetch={false}
      className="card bg-dark border-secondary mb-4 hub-card"
      href={homeTabHref("il", season, "go_team", "")}
    >
      <div className="card-body">
        <div className="d-flex justify-content-between align-items-baseline mb-2">
          <h5 className="card-title mb-0">{row.team_name}</h5>
          <span className="hub-record">
            {row.wins}-{row.losses}
          </span>
        </div>
        <div className="hub-stat-row">
          {stat(t.off, row.off_ppp, row.rank_off_ppp)}
          {stat(t.def, row.def_ppp, row.rank_def_ppp)}
          {stat(t.net, row.net_rtg, row.rank_net_rtg, true)}
        </div>
        {(data.offense || data.defense) && (
          <div className="hub-ff-groups">
            {(["offense", "defense"] as const).map(
              (side) =>
                data[side] && (
                  <div className="hub-ff-row" key={side}>
                    <span className="hub-ff-label">{t[side]}</span>
                    <div className="hub-ff-chips">
                      {data[side]!.map((v) => (
                        <span className="hub-ff-chip" key={v.label}>
                          {v.label} {homeFixed(v.value)} ({homeOrdinal(v.rank)})
                        </span>
                      ))}
                    </div>
                  </div>
                ),
            )}
          </div>
        )}
        <div className="hub-card-go">{t.teamLink}</div>
      </div>
    </Link>
  );
}
