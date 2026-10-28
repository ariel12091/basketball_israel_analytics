"use client";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import Link from "next/link";
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from "@tanstack/react-query";
import type { League } from "@/rules/gameLogs";
import type { HomePayload } from "@/server/homeRepo";
import ui from "@/data/homeUi.json";
import shiny from "@/data/shinyUi.json";
import { homeTeams, homePath, homeTabHref } from "@/lib/homeQuery";
import { TabFrame, useTabQuery, useVersion } from "./shiny/TabKit";
import Selectize from "./shiny/Selectize";
import HomeHub, { HubIdentity } from "./shiny/HomeHub";
import HomeAsk from "./shiny/HomeAsk";
import { homeDashboard } from "@/rules/home";
import { noteVersion, withVersion } from "@/lib/dataVersion";
type Props = {
  league: League;
  initialSeason: string;
  initialTeam: string;
  initial: { url: string | null; data: HomePayload | null };
};
const listeners = new Set<() => void>();
function savedTeam() {
  try {
    return localStorage.getItem(ui.storage.enabled) === "1"
      ? (localStorage.getItem(ui.storage.team) ?? "")
      : "";
  } catch {
    return "";
  }
}
function subscribeSaved(notify: () => void) {
  listeners.add(notify);
  window.addEventListener("storage", notify);
  return () => {
    listeners.delete(notify);
    window.removeEventListener("storage", notify);
  };
}
function storeTeam(team: string) {
  try {
    if (team) {
      localStorage.setItem(ui.storage.team, team);
      localStorage.setItem(ui.storage.enabled, "1");
    } else {
      localStorage.removeItem(ui.storage.team);
      localStorage.removeItem(ui.storage.enabled);
    }
  } catch {}
  listeners.forEach((fn) => fn());
}
export default function HomeClient(props: Props) {
  const [client] = useState(() => new QueryClient());
  return (
    <QueryClientProvider client={client}>
      <HomeContent {...props} />
    </QueryClientProvider>
  );
}
function HomeContent({ league, initialSeason, initialTeam, initial }: Props) {
  const { query: q, set } = useTabQuery(league, { team_id: "" });
  const teams = homeTeams(q.season),
    saved = useSyncExternalStore(subscribeSaved, savedTeam, () => ""),
    [sessionSaved, setSessionSaved] = useState<string | null>(null),
    [setThisSession, markSet] = useState(false),
    [touched, touch] = useState(false);
  const remembered = sessionSaved ?? saved,
    valid = (id: string) => teams.some((t) => String(t.id) === id);
  const team =
    q.team_id === "all"
      ? ""
      : valid(q.team_id)
        ? q.team_id
        : !touched && valid(remembered)
          ? remembered
          : q.season === initialSeason && valid(initialTeam)
            ? initialTeam
            : String(teams[0]?.id ?? "");
  const url = league === "il" && team ? homePath(q.season, team) : null;
  const state = useQuery<HomePayload>({
    queryKey: ["home", url],
    enabled: !!url,
    queryFn: async ({ signal }) => {
      const res = await fetch(withVersion(url!), { signal });
      const data = await res.json();
      noteVersion("il", data.meta?.version);
      if (!res.ok) throw Error(data.error ?? "Unable to load Home");
      if (data.meta?.season !== q.season || data.meta?.teamId !== team)
        throw Error("Home response does not match the selected team");
      return data;
    },
    initialData: initial.url === url && initial.data ? initial.data : undefined,
    staleTime: 60000,
    retry: false,
    refetchOnWindowFocus: false,
  });
  const data =
    state.data?.meta.season === q.season && state.data?.meta.teamId === team
      ? state.data
      : null;
  const version = useRef(data?.meta.version ?? "");
  useEffect(() => {
    if (data) version.current = data.meta.version;
  }, [data]);
  const { refetch } = state;
  const refresh = useCallback(() => {
    if (url) void refetch();
  }, [url, refetch]);
  const updated = useVersion(league, version, refresh);
  const choose = (id: string) => {
    touch(true);
    set({ team_id: id || "all" });
  };
  const remember = (checked: boolean) => {
    if (checked && team) {
      setSessionSaved(team);
      markSet(true);
      storeTeam(team);
    } else if (team === remembered) {
      setSessionSaved("");
      markSet(false);
      storeTeam("");
    }
  };
  const dashboard = useMemo(
    () => (data ? homeDashboard(data.frames, team) : null),
    [data, team],
  );
  const [more, showMore] = useState(false);
  const euro = league !== "il",
    t = ui.text;
  const cards = <HomeCards league={league} season={q.season} team={team} />;
  return (
    <TabFrame
      league={league}
      tab="home"
      paneValue="home"
      season={q.season}
      seasons={euro ? shiny.euroStaticSeasons : shiny.ilSeasons}
      onSeason={(season) =>
        set({
          season,
          team_id: homeTeams(season).some((t) => String(t.id) === team)
            ? team
            : null,
        })
      }
      views={[]}
      view=""
      onView={() => {}}
      updated={updated}
      filtersId="home-filters"
      sidebar={null}
      mainOnly
      main={
        <div className="ibpl-home home-compact">
          <HomeAsk league={league} />
          {!euro && (
            <div className="home-team-row">
              <div className="home-team-select">
                <Selectize
                  id="home_team"
                  label={<span className="visually-hidden">Team</span>}
                  choices={teams.map((t) => ({
                    value: String(t.id),
                    label: t.name,
                  }))}
                  value={team ? [team] : []}
                  onChange={(v) => choose(v[0] ?? "")}
                  emptyLabel={t.allTeams}
                  placeholder={t.allTeams}
                />
              </div>
              <div className="home-team-default" title={t.defaultTitle}>
                <label className="form-check">
                  <input
                    className="form-check-input"
                    type="checkbox"
                    disabled={!team}
                    checked={!!team && setThisSession && remembered === team}
                    onChange={(e) => remember(e.target.checked)}
                  />
                  <span className="form-check-label">{t.defaultLabel}</span>
                </label>
              </div>
            </div>
          )}
          <div
            className={`home-boxes${!euro && team ? " home-boxes-pair" : ""}`}
          >
            {!euro && team && (
              <HubIdentity
                key={`${q.season}:${team}`}
                season={q.season}
                dashboard={dashboard}
                partial={!!data?.meta.errors.length}
                loading={state.isFetching}
                error={state.error?.message ?? ""}
                retry={refresh}
              />
            )}
            <nav className="home-links" aria-labelledby="home-links-title">
              <div className="hub-block-title" id="home-links-title">
                {t.investigate}
              </div>
              {cards}
            </nav>
          </div>
          {!euro && team && (
            <>
              <button
                type="button"
                className="btn home-more-toggle"
                aria-expanded={more}
                aria-controls="home-more"
                onClick={() => showMore((v) => !v)}
              >
                {more ? t.seeLess : t.seeMore}
                {!more && <span className="home-more-sub">{t.seeMoreSub}</span>}
              </button>
              {more && (
                <div id="home-more">
                  <HomeHub
                    key={`${q.season}:${team}`}
                    season={q.season}
                    team={team}
                    dashboard={dashboard}
                    loading={state.isFetching}
                    error={state.error?.message ?? ""}
                  />
                </div>
              )}
            </>
          )}
          {euro && (
            <p className="home-euro-note">
              {t.euroNote}
              {t.euroNoteEnd}
            </p>
          )}
        </div>
      }
    />
  );
}
function HomeCards({
  league,
  season,
  team,
}: {
  league: League;
  season: string;
  team: string;
}) {
  const cards = league === "il" ? ui.cards.il : ui.cards.euro;
  return (
    <ul className="home-links-list">
      {cards.map((c) => (
        <li key={c.input_id}>
          <Link
            prefetch={false}
            className="home-nav-card"
            href={homeTabHref(league, season, c.input_id, team)}
          >
            <span className="home-nav-card-head">
              <i className={`bi ${c.icon}`} aria-hidden="true" />
              <span className="home-nav-card-title">{c.title}</span>
            </span>
            <span className="home-nav-card-sub">{c.sub}</span>
          </Link>
        </li>
      ))}
    </ul>
  );
}
