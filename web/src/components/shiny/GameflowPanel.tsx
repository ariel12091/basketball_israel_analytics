"use client";
import {
  Fragment,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useReducer,
  useRef,
  useState,
  useSyncExternalStore,
  type CSSProperties,
  type ReactNode,
  type RefObject,
} from "react";
import type { League } from "@/rules/gameLogs";
import type { GameflowPayload } from "@/server/gameflowRepo";
import {
  aggregateSegments,
  clock,
  fitGameflowLayout,
  gameLineups,
  laneKey,
  lineupsRoom,
  marginArea,
  marginPath,
  pmLabel,
  RATING_WINDOW,
  ratingAt,
  ratingLabel,
  type RatingMode,
  scoreAt,
  sharedLineups,
  sharedWindows,
  windowDetail,
  windowSegments,
  windowsSummary,
  type Bar,
} from "@/rules/gameflow";
import type { Side, Window as Span } from "@/rules/gameflow";
import {
  gameRating,
  netLabel,
  ratingTone,
  stintRatings,
  windowsRatings,
  type StintRatings,
} from "@/rules/gameflowRatings";
import { useTabData } from "./TabKit";
import LineupNames from "./LineupNames";
import { NARROW } from "./layoutClasses";
import GameflowChart, { type RunTrigger } from "./GameflowChart";
import {
  periodClock,
  runLineupGroups,
  windowLabel,
  type Run,
} from "@/rules/gameflowRuns";
import type { ExplainedRun } from "@/rules/gameflowSummary";
import { paletteVars, type TeamIdentity } from "@/rules/teamColors";
import {
  EMPTY_SELECTION,
  selectionReducer,
  stintOf,
  type BarWindow,
  type SelectionAction,
} from "@/rules/gameflowSelection";
import { text as labels } from "@/data/gameflowLayout.json";
function subscribeNarrow(notify: () => void) {
  const q = matchMedia(NARROW);
  q.addEventListener("change", notify);
  return () => q.removeEventListener("change", notify);
}
export function useNarrow() {
  return useSyncExternalStore(
    subscribeNarrow,
    () => matchMedia(NARROW).matches,
    () => true,
  );
}
export function gameflowPath(league: League, game: number, team: number) {
  return `/api/v1/${league}/gameflow?game_id=${game}&team_id=${team}`;
}
/** Room left under the chart: the panel's padding and a little air. */
const BELOW_CHART = 21;
/** .gf-details' margins, top and bottom. */
const DETAILS_MARGINS = 12;
/**
 * The space the wide chart may fill in this window, measured once and on
 * resize. What it is then drawn to depends on whether the lineups are open.
 */
function useChartSpace(
  compact: boolean,
  box: RefObject<HTMLDivElement | null>,
) {
  const [size, setSize] = useState<{ width: number; height: number } | null>(
    null,
  );
  useLayoutEffect(() => {
    if (compact) return;
    const measure = () => {
      const el = box.current;
      if (!el) return;
      // Position in the page, not the window: where the chart sits unscrolled.
      const top = el.getBoundingClientRect().top + scrollY,
        next = {
          width: el.clientWidth,
          height: Math.max(0, innerHeight - top - BELOW_CHART),
        };
      setSize((s) =>
        s?.width === next.width && s.height === next.height ? s : next,
      );
    };
    measure();
    // A web font arriving late can rewrap the navbar and move the chart.
    void document.fonts?.ready.then(measure);
    addEventListener("resize", measure);
    return () => removeEventListener("resize", measure);
  }, [compact, box]);
  return compact ? null : size;
}
export function GameflowView({
  data,
  compact,
}: {
  data: GameflowPayload;
  compact: boolean;
}) {
  const [selection, dispatch] = useReducer(selectionReducer, EMPTY_SELECTION),
    { selected, active, hover, focus, preview, run } = selection,
    gameEnd = data.bounds[data.bounds.length - 1],
    [lineupsUsed, setLineupsUsed] = useState<number | null>(null),
    [nameHover, setNameHover] = useState<Bar | null>(null),
    [full, setFull] = useState(false),
    [quarter, setQuarter] = useState(1),
    [score, setScore] = useState(""),
    // Each team's ORtg in the score readouts (user 2026-11-07): since tip-off
    // by default, or over its last RATING_WINDOW possessions.
    [ratingMode, setRatingMode] = useState<RatingMode | "off">("game"),
    // Phones (user 2026-11-11, from the mock-up): one quarter at a time, one
    // bottom panel every tap reports to, the runs listed in it, and the
    // display options behind a menu.
    [panelOpen, setPanelOpen] = useState(false),
    [panelRuns, setPanelRuns] = useState(false),
    [menuOpen, setMenuOpen] = useState(false),
    // Wide: the default top lineups fold away under their heading.
    [topOpen, setTopOpen] = useState(true),
    // A swipe across the quarter, and a tap that only closes the run
    // details, must not also count as a tap on the bar under them.
    swallowTap = useRef(false),
    // Scoring runs (shared_definitions.md, Gameflow run) and the open tip.
    runs = data.runs,
    [runTip, setRunTip] = useState<{
      run: ExplainedRun;
      box: DOMRect;
      pinned: boolean;
    } | null>(null);
  const detail = useRef<HTMLElement>(null),
    chartBox = useRef<HTMLDivElement>(null),
    lineupsBox = useRef<HTMLDivElement>(null),
    space =useChartSpace(compact, chartBox),
    // The click that opened the chart leaves the pointer resting on a bar. That
    // is not a hover: acting on it would cover the first look with a tooltip.
    pointerMoved = useRef(false),
    restingOn = useRef<{ window: BarWindow; box: DOMRect } | null>(null);
  const windows = sharedWindows(data.bars, selected),
    total = windows.reduce((sum, w) => sum + w.end - w.start, 0);
  // Before any click, each team's most-played lineups (user 2026-11-15); a
  // player, stint or run replaces them.
  const top = useMemo(
      () =>
        (["own", "opp"] as const).flatMap((side) =>
          gameLineups(data, side, TOP_LINEUPS),
        ),
      [data],
    ),
    idle = !run && !selected.length && !active;
  // A clicked run's band: both teams' lineups during it (user 2026-11-11).
  const groups = run
    ? runLineupGroups(data, run)
    : selected.length > 1
      ? sharedLineups(data, selected, windows)
      : active
        ? stintLineups(data, active)
        : top;
  // A click on a bar is about that stint, a click on a name the whole game.
  const stint = stintOf(selection, gameEnd);
  const groupKey = (g: (typeof groups)[number]) => `${g.side}:${g.dictIndex}`;
  const focused = groups.find((g) => groupKey(g) === focus);
  const echo = focused
    ? data.lanes
        .filter(
          (l) =>
            l.side === focused.side &&
            data.dict[focused.dictIndex]?.lineup_key === l.lineup_key,
        )
        .map((l) => ({ start: l.start_elapsed, end: l.end_elapsed }))
    : null;
  // A hovered lineup row, then (wide) a hovered bar, win over what is
  // selected: hovering marks without clicking (user 2026-11-18).
  const highlight =
    preview?.windows ??
    (!compact && hover
      ? [{ start: hover.start, end: hover.end }]
      : focused
      ? selected.length > 1 || run
        ? focused.windows
        : echo!
      : run
        ? [{ start: run.start, end: run.end }]
        : stint
        ? [{ start: stint.start, end: stint.end }]
        : selected.length
          ? windows
          : hover
            ? [{ start: hover.start, end: hover.end }]
            : []);
  const toggle = (bar: Bar, window?: BarWindow) => {
    if (compact) setPanelRuns(false);
    dispatch({ type: "toggle", bar, window, bars: data.bars, end: gameEnd });
  };
  const clear = () => dispatch({ type: "clear" });
  // The next click is swallowed; a gesture that never ends in one (a touch
  // swipe) lets the flag lapse instead of eating a later, real tap.
  const skipNextTap = () => {
    swallowTap.current = true;
    setTimeout(() => (swallowTap.current = false), 500);
  };
  const quarterOf = (t: number) =>
    Math.max(1, data.bounds.findIndex((b) => t < b) + 1 || data.bounds.length);
  const selectRun = (r: Run) => {
    dispatch({ type: "run", run: r });
    if (compact) {
      setPanelRuns(false);
      setPanelOpen(true);
    }
  };
  // Wide: hovering moves nothing (it made bars slide out from under the pointer
  // on the way to a click, user 2026-11-04), so it shows a tooltip, and only a
  // click opens the details and shrinks the chart. Narrow keeps its inline card.
  const activeWindow = compact ? (hover ?? active) : active;
  const showStint =
    !!activeWindow &&
    selected.length < 2 &&
    !(activeWindow.start === 0 && activeWindow.end === gameEnd);
  const lanes = new Set(data.bars.map(laneKey)).size,
    lineupsOpen = showStint || selected.length > 0 || groups.length > 0,
    // Folded top lineups keep only their heading: the chart is fitted as if
    // nothing were open, under it.
    folded = idle && !topOpen,
    room = space && lineupsRoom(lanes, space),
    // The lineups take only what they fill, up to the room; the chart the rest.
    taken = room != null && lineupsUsed != null ? Math.min(room, lineupsUsed) : room,
    layout = space
      ? fitGameflowLayout(
          lanes,
          lineupsOpen && taken != null
            ? { ...space, height: space.height - taken }
            : space,
          lineupsOpen && !folded,
        )
      : undefined;
  // Phones: a tap outside the panel closes a run's details or the runs list
  // (user 2026-11-12). A tap on the chart that does so does nothing else.
  const detailsOpen = compact && panelOpen && (run != null || panelRuns);
  // Phones: what the selection is, so the panel's button can pulse when it
  // changes (user 2026-11-12: the ▲ alone was easy to miss).
  const selectionKey = run
    ? `run:${run.side}:${run.start}`
    : `${selected.join(",")}|${active ? `${active.start}-${active.end}` : ""}`;
  useEffect(() => {
    if (!detailsOpen) return;
    const close = (e: PointerEvent) => {
      const t = e.target as Element;
      if (t.closest?.(".gf-m-panel")) return;
      dispatch({ type: "clear" });
      setPanelRuns(false);
      setPanelOpen(false);
      if (t.closest?.(".ibpl-ribbon-quarters")) skipNextTap();
    };
    addEventListener("pointerdown", close, true);
    return () => removeEventListener("pointerdown", close, true);
  }, [detailsOpen]);
  // A tapped run tip is placed once: touching anywhere else (which is also
  // how a touch scroll starts) closes it. Not on scroll events: layout can
  // scroll the page right after the tap.
  const pinnedTip = runTip?.pinned ?? false;
  useEffect(() => {
    if (!pinnedTip) return;
    const close = (e: PointerEvent) => {
      if ((e.target as Element).closest?.(".gf-run-chip, .gf-run-tip")) return;
      setRunTip(null);
    };
    addEventListener("pointerdown", close);
    return () => removeEventListener("pointerdown", close);
  }, [pinnedTip]);
  // Before paint, whenever what is listed can change.
  useLayoutEffect(() => {
    const el = lineupsBox.current;
    if (el) setLineupsUsed(el.offsetHeight + DETAILS_MARGINS);
  }, [active, selected, room, run, topOpen]);
  const common = {
    data,
    compact,
    selected,
    highlight,
    onPlayer: (bar: Bar) => toggle(bar),
    onBar: (window: BarWindow) => toggle(window.bar, window),
    onHover: (window: BarWindow | null, box: DOMRect | null) => {
      restingOn.current = window && box && { window, box };
      if (window && !pointerMoved.current) return;
      dispatch({ type: "hover", window, box });
    },
    onNameHover: (bar: Bar | null) => {
      if (bar && !pointerMoved.current) return;
      setNameHover(bar);
    },
    active: hover ?? active,
    stintBar: stint?.bar,
    lineupIndex: preview?.lineup ?? focused?.dictIndex,
    previewing: preview != null,
    layout,
    ratingMode,
    runs,
    selectedRun: run,
    onRunSelect: (r: Run) => selectRun(r),
    openRun: runTip?.run ?? null,
    // The chart hands back the run objects it was given (data.runs).
    onRun: (r: Run | null, box: DOMRect | null, how: RunTrigger) => {
      // Phones explain a run in the bottom panel, never in a floating tip.
      if (compact) {
        if (how === "tap" && r) selectRun(r);
        return;
      }
      const tipRun = r as ExplainedRun | null;
      // A tap on a chip also lists the run's lineups, as a click on its band
      // does (user 2026-11-17), and pins its tip; a second tap closes both.
      if (how === "tap") {
        const open =
          tipRun != null &&
          !(run && run.side === tipRun.side && run.start === tipRun.start);
        if (tipRun) selectRun(tipRun);
        else if (run) clear();
        setRunTip(open ? { run: tipRun, box: box!, pinned: true } : null);
        return;
      }
      // A pointer shows the tip while resting and leaves a pinned one alone.
      setRunTip((tip) => {
        if (tip?.pinned) return tip;
        return tipRun && box ? { run: tipRun, box, pinned: false } : null;
      });
    },
  };
  const prompt = run
    ? `${(run as ExplainedRun).lines?.[0] ?? "Run"}. Select the run again to close it.`
    : stint
    ? `${stint.bar.player_label} · ${clock(stint.end - stint.start)} in this stint, ${clock(total)} in the game. Select the stint again to remove it.`
    : selected.length
      ? `${selected.map((key) => data.bars.find((b) => laneKey(b) === key)?.player_label ?? key).join(" + ")} · ${total ? clock(total) + (selected.length > 1 ? " on court together" : " on court") : "No shared court time"}. Select a player again to remove them.`
      : labels.prompt;
  // A hovered bar, else a hovered name (user 2026-11-18).
  const hoverLine =
    compact || runTip
      ? null
      : hover
        ? stintHoverLine(data, hover)
        : nameHover
          ? playerHoverLine(data, nameHover)
          : null;
  // Which team the selection belongs to: details sit next to its lanes.
  const sideOf = (key: string) =>
    data.bars.find((b) => laneKey(b) === key)?.side;
  const upper =
    selected.length > 0 && selected.every((k) => sideOf(k) === "own");
  const list = (
    <GameflowDetails
      data={data}
      activeWindow={activeWindow}
      showStint={showStint}
      selected={selected}
      groups={groups}
      groupKey={groupKey}
      focus={focus}
      run={run}
      top={idle}
      folded={folded}
      onFold={() => {
        // A highlighted lineup would stay lit with its row folded away.
        if (topOpen) clear();
        setTopOpen(!topOpen);
      }}
      dispatch={(a: SelectionAction) => {
        dispatch(a);
        // Phones show one quarter: a lineup picked in the panel brings up the
        // quarter where it first played, or its marks could be off screen.
        if (compact && !full && a.type === "focus" && a.key !== focus) {
          const g = groups.find((x) => groupKey(x) === a.key);
          if (g?.windows.length) setQuarter(quarterOf(g.windows[0].start));
        }
      }}
      detail={detail}
      compact={compact}
    />
  );
  // The margins are part of the room the chart gave up.
  const details =
    room != null && lineupsOpen ? (
      <div
        className="gf-details"
        ref={lineupsBox}
        style={{ maxHeight: room - DETAILS_MARGINS }}
      >
        {list}
      </div>
    ) : (
      list
    );
  const meta = data.meta as typeof data.meta & { ownIdentity?: TeamIdentity; oppIdentity?: TeamIdentity },
    teamColours = useMemo(() => paletteVars(meta.ownIdentity, meta.oppIdentity), [meta.ownIdentity, meta.oppIdentity]);
  const ortgToggle = data.ratings && (
    <div
      className="ibpl-gameflow-rtg-toggle"
      role="group"
      aria-label="ORtg in the score readout"
    >
      <div className="ibpl-gameflow-rtg-row">
        <span className="ibpl-gameflow-rtg-title">
          Offensive rating (ORtg)
        </span>
        <span className="gf-rtg-switch">
          {(
            [
              ["off", "Off"],
              ["game", "Entire game"],
              ["window", `Last ${RATING_WINDOW} poss`],
            ] as const
          ).map(([mode, label]) => (
            <button
              key={mode}
              type="button"
              aria-pressed={ratingMode === mode}
              className={ratingMode === mode ? "is-on" : undefined}
              onClick={() => setRatingMode(mode)}
            >
              {label}
            </button>
          ))}
        </span>
      </div>
      {ratingMode !== "off" && (
        <p className="ibpl-gameflow-rtg-hint">
          {ratingMode === "game"
            ? "Points per 100 possessions since tip-off."
            : `Counts only each team's last ${RATING_WINDOW} possessions, so a recent run stands out instead of being averaged into the whole game.`}
        </p>
      )}
    </div>
  );
  return (
    <div
      className={`ibpl-ribbon-game ibpl-react-gameflow${compact ? " gf-m" : ""}`}
      // Each side in its own club's colours, both themes (web.css picks one).
      style={teamColours as CSSProperties}
      onPointerMove={(e) => {
        if (pointerMoved.current || !(e.movementX || e.movementY)) return;
        pointerMoved.current = true;
        // It really is hovering the bar it was resting on, now.
        if (restingOn.current) {
          dispatch({ type: "hover", ...restingOn.current });
        }
      }}
      onKeyDown={() => {
        // Keyboard users never move the pointer: their focus is a real hover.
        pointerMoved.current = true;
      }}
    >
      {runTip && <GameflowRunTip run={runTip.run} box={runTip.box} />}
      {data.health && (
        <div className="alert alert-warning py-2 px-3 mb-2" role="note">
          {data.health}
        </div>
      )}
      {!compact && (
      <div className="ibpl-ribbon-pair-controls">
        <button
          type="button"
          className="ibpl-ribbon-pair-clear"
          onClick={clear}
        >
          {labels.clear}
        </button>
        <div
          className="ibpl-ribbon-pair-status"
          role="status"
          aria-live="polite"
        >
          {/* A hovered stint reads here, over the prompt, instead of in a
              tip that covered the bars (user 2026-11-17). The prompt stays
              for screen readers; the readout is not announced. */}
          <span className={hoverLine ? "is-covered" : undefined}>
            {prompt}
          </span>
          {hoverLine && (
            <span className="gf-hover-readout" aria-hidden="true">
              <b>{hoverLine.head}</b> · {hoverLine.stats}{" "}
              <span className="gf-hover-hint">{hoverLine.hint}</span>
            </span>
          )}
        </div>
        {ortgToggle}
      </div>
      )}
      {compact && (
        <div className="gf-m-head">
          <div className="ibpl-ribbon-mobile-overview">
            <svg
              className="ibpl-ribbon-overview-svg"
              viewBox="0 0 320 56"
              role="img"
              aria-label={labels.overviewAria}
            >
              <line
                className="ibpl-ribbon-overview-zero"
                x1={0}
                x2={320}
                y1={26}
                y2={26}
              />
              {data.bounds.slice(0, -1).map((b) => (
                <line
                  key={b}
                  className="ibpl-ribbon-overview-period"
                  x1={(b / data.bounds.at(-1)!) * 320}
                  x2={(b / data.bounds.at(-1)!) * 320}
                  y1={0}
                  y2={56}
                />
              ))}
              {runs.map((r) => (
                <rect
                  key={`${r.side}:${r.start}`}
                  className={`gf-run-band is-${r.side}${r.defining ? " is-defining" : ""}${run && run.start === r.start && run.side === r.side ? " is-open" : ""}`}
                  data-start={r.start}
                  x={(r.start / data.bounds.at(-1)!) * 320}
                  y={0}
                  width={Math.max(2, ((r.end - r.start) / data.bounds.at(-1)!) * 320)}
                  height={56}
                />
              ))}
              <defs>
                <clipPath id="gf-m-strip-up">
                  <rect x={0} y={0} width={320} height={26} />
                </clipPath>
                <clipPath id="gf-m-strip-down">
                  <rect x={0} y={26} width={320} height={30} />
                </clipPath>
              </defs>
              {(() => {
                const line = marginPath(data.margin, data.bounds.at(-1)!, 320, 4, 44, 0),
                  area = marginArea(line, 26);
                return (
                  <>
                    <path className="gf-m-lead is-own" d={area} clipPath="url(#gf-m-strip-up)" />
                    <path className="gf-m-lead is-opp" d={area} clipPath="url(#gf-m-strip-down)" />
                    <path className="ibpl-ribbon-overview-line" d={line} />
                  </>
                );
              })()}
              {/* Which way is which (user 2026-11-12): the team whose Gameflow
                  this is on top, the other below, in the lanes' colours. */}
              <text className="gf-m-side is-own" x={4} y={9}>
                {data.meta.ownTeam}
              </text>
              <text className="gf-m-side is-opp" x={4} y={54}>
                {data.meta.oppTeam}
              </text>
              <rect
                x={0}
                y={4}
                width={
                  (data.margin.at(-1)!.elapsed / data.bounds.at(-1)!) * 320
                }
                height={44}
                fill="transparent"
                aria-label="Hover or tap to see the score"
                onPointerMove={(e) => {
                  const r = e.currentTarget.getBoundingClientRect(),
                    elapsed =
                      ((e.clientX - r.left) / r.width) *
                      data.margin.at(-1)!.elapsed;
                  const rtg =
                    ratingMode !== "off" && data.ratings
                      ? ratingAt(data.ratings[ratingMode], elapsed)
                      : null;
                  setScore(
                    `${periodClock(elapsed, data.bounds)} · ${data.meta.ownTeam} ${scoreAt(data.steps, elapsed, "own")} – ${scoreAt(data.steps, elapsed, "own") - scoreAt(data.steps, elapsed)} ${data.meta.oppTeam}${rtg ? ` · ORtg${ratingMode === "window" ? ` (last ${RATING_WINDOW})` : ""} ${ratingLabel(rtg.own)} – ${ratingLabel(rtg.opp)}` : ""}`,
                  );
                }}
                onPointerLeave={() => setScore("")}
                // A tap opens the run under it (user 2026-11-12), or else
                // shows the quarter tapped.
                onClick={(e) => {
                  const r = e.currentTarget.getBoundingClientRect(),
                    t =
                      ((e.clientX - r.left) / r.width) *
                      data.margin.at(-1)!.elapsed;
                  const hit = runs.find((x) => t >= x.start && t <= x.end);
                  setFull(false);
                  if (hit) {
                    setQuarter(quarterOf(hit.start));
                    if (!(run && run.start === hit.start && run.side === hit.side))
                      selectRun(hit);
                  } else {
                    // An open run would be off screen: close it (a player
                    // selection stays).
                    setQuarter(quarterOf(t));
                    if (run) clear();
                  }
                }}
              />
            </svg>
            {score && <output role="tooltip">{score}</output>}
          </div>
          <div className="gf-m-tabs" role="tablist" aria-label="Quarter">
            {data.bounds.map((_, i) => (
              <button
                key={i}
                type="button"
                role="tab"
                aria-selected={!full && quarter === i + 1}
                className={`gf-m-tab${!full && quarter === i + 1 ? " is-active" : ""}`}
                onClick={() => {
                  setFull(false);
                  setQuarter(i + 1);
                }}
              >
                {i < 4 ? `Q${i + 1}` : `OT${i - 3}`}
              </button>
            ))}
            {runs.length > 0 && (
              <button
                type="button"
                className={`gf-m-runs${panelRuns && panelOpen ? " is-active" : ""}`}
                onClick={() => {
                  clear();
                  setPanelRuns(true);
                  setPanelOpen(true);
                }}
              >
                Runs {runs.length} ›
              </button>
            )}
            <button
              type="button"
              className="gf-m-more"
              aria-label="More options"
              aria-expanded={menuOpen}
              onClick={() => setMenuOpen(!menuOpen)}
            >
              {/* Drawn: the font has no usable ellipsis glyph. */}
              <svg viewBox="0 0 18 4" width="18" height="4" aria-hidden="true">
                <circle cx="2" cy="2" r="2" />
                <circle cx="9" cy="2" r="2" />
                <circle cx="16" cy="2" r="2" />
              </svg>
            </button>
          </div>
          {menuOpen && (
            <div className="gf-m-menu">
              {ortgToggle}
              <button
                type="button"
                className="ibpl-ribbon-full-toggle"
                aria-expanded={full}
                onClick={() => {
                  setFull(!full);
                  setMenuOpen(false);
                  dispatch({ type: "hover", window: null, box: null });
                }}
              >
                {full ? labels.quarters : labels.full}
              </button>
              <p className="ibpl-ribbon-inline-hint">{PHONE_HINT}</p>
            </div>
          )}
        </div>
      )}
      {!compact || full ? (
        upper ? (
          // Top team selected: its lanes, then the lineups, then the margin
          // chart and the other team.
          <>
            <div
              className="ibpl-ribbon-inline-scroll ibpl-gameflow-full"
              ref={chartBox}
            >
              <GameflowChart {...common} crop="top" />
            </div>
            {details}
            <div className="ibpl-ribbon-inline-scroll ibpl-gameflow-full">
              <GameflowChart {...common} crop="bottom" />
            </div>
          </>
        ) : (
          <>
            <div
              className="ibpl-ribbon-inline-scroll ibpl-gameflow-full"
              ref={chartBox}
            >
              <GameflowChart {...common} />
            </div>
            {details}
          </>
        )
      ) : (
        <>
          {/* One quarter at a time (phones): the tabs above or a swipe
              choose it, and whatever a tap selects reports in the panel. */}
          <div
            className="ibpl-ribbon-quarters"
            onPointerDown={(e) => {
              // The finger may lift past the card's edge: read the release
              // wherever it happens.
              const from = { x: e.clientX, y: e.clientY };
              const end = (ev: PointerEvent) => {
                removeEventListener("pointerup", end);
                removeEventListener("pointercancel", end);
                if (ev.type !== "pointerup") return;
                const dx = ev.clientX - from.x,
                  dy = ev.clientY - from.y;
                if (Math.abs(dx) < SWIPE_MIN || Math.abs(dx) < 1.5 * Math.abs(dy))
                  return;
                skipNextTap();
                setQuarter((q) =>
                  Math.min(data.bounds.length, Math.max(1, q + (dx < 0 ? 1 : -1))),
                );
              };
              addEventListener("pointerup", end);
              addEventListener("pointercancel", end);
            }}
            onClickCapture={(e) => {
              if (!swallowTap.current) return;
              swallowTap.current = false;
              e.stopPropagation();
              e.preventDefault();
            }}
          >
            {data.bounds.map((end, i) =>
              quarter !== i + 1 ? null : (
                <section
                  key={end}
                  className="ibpl-ribbon-quarter-card"
                  data-quarter={i + 1}
                >
                  <h4 className="ibpl-ribbon-quarter-heading">
                    {i < 4 ? `Q${i + 1}` : `OT${i - 3}`}
                  </h4>
                  <div className="ibpl-ribbon-quarter-frame">
                    <GameflowChart
                      {...common}
                      range={{ start: i ? data.bounds[i - 1] : 0, end }}
                    />
                  </div>
                </section>
              ),
            )}
          </div>
        </>
      )}
      {compact && (
        <PhonePanel
          data={data}
          open={panelOpen}
          setOpen={setPanelOpen}
          showRuns={panelRuns}
          run={run as ExplainedRun | null}
          selected={selected}
          active={active}
          windows={windows}
          prompt={prompt}
          list={details}
          lineups={groups.length}
          selectionKey={selectionKey}
          onRun={(r) => {
            selectRun(r);
            setQuarter(quarterOf(r.start));
            setFull(false);
          }}
          onAllRuns={() => {
            clear();
            setPanelRuns(true);
          }}
          onShow={(r) => {
            setQuarter(quarterOf(r.start));
            setFull(false);
            setPanelOpen(false);
          }}
          onClear={() => {
            clear();
            setPanelOpen(false);
          }}
        />
      )}
    </div>
  );
}
const stintLineups = (data: GameflowPayload, w: BarWindow) =>
  aggregateSegments(
    windowSegments(
      data.lanes,
      data.steps,
      data.dict,
      w.bar.side,
      w.bar.player_key,
      w.start,
      w.end,
    ),
  );
const TIP_WIDTH = 400;
const LINEUPS_SHORT = 3;
/** The default list: each team's most-played lineups (user 2026-11-15). */
const TOP_LINEUPS = 5;
/** A horizontal move at least this long (px) is a swipe, not a tap. */
const SWIPE_MIN = 60;
const PHONE_HINT =
  "Swipe to change quarter. Tap a player's name for his game, a bar for one stint, two or more names for their minutes together, or a shaded run for its lineups.";
/** The one-line summary of several selected players: their minutes
 * together (one team) or head to head (both). */
function togetherText(data: GameflowPayload, selected: string[], windows: Span[]) {
  const sides = [
    ...new Set(
      selected.map((k) => data.bars.find((b) => laneKey(b) === k)?.side),
    ),
  ].filter((s): s is Side => s != null);
  const secs = windows.reduce((t, w) => t + w.end - w.start, 0);
  if (sides.length === 1) {
    const d = windowsSummary(sides[0], windows, data.steps),
      r = data.events.length
        ? stintRatings(
            data.events,
            sides[0],
            selected.map((k) => k.slice(k.indexOf(":") + 1)),
          )
        : null;
    return `Together ${clock(secs)} · +/- ${d.pm}${r ? ` · Net ${netLabel(r.net)}` : ""}`;
  }
  const d = windowsSummary("own", windows, data.steps);
  return `Head to head ${clock(secs)}${d.pf == null ? "" : ` · ${data.meta.ownTeam} ${d.pf} – ${d.pa} ${data.meta.oppTeam}`}`;
}
/**
 * Phones: the one place every tap reports (user 2026-11-11 mock-up). Fixed to
 * the bottom of the screen; collapsed it is a summary line, open it holds the
 * cards and lineups, a run's explanation and lineups, or the list of runs.
 */
function PhonePanel({
  data,
  open,
  setOpen,
  showRuns,
  run,
  selected,
  active,
  windows,
  prompt,
  list,
  lineups,
  selectionKey,
  onRun,
  onAllRuns,
  onShow,
  onClear,
}: {
  /** How many lineups the open panel lists, named on its button. */
  lineups: number;
  /** Changes with the selection; the button pulses when it does. */
  selectionKey: string;
  data: GameflowPayload;
  open: boolean;
  setOpen: (open: boolean) => void;
  showRuns: boolean;
  run: ExplainedRun | null;
  selected: string[];
  active: BarWindow | null;
  windows: Span[];
  prompt: string;
  list: ReactNode;
  onRun: (r: ExplainedRun) => void;
  onAllRuns: () => void;
  onShow: (r: ExplainedRun) => void;
  onClear: () => void;
}) {
  const team = (r: ExplainedRun) =>
    r.side === "own" ? data.meta.ownTeam : data.meta.oppTeam;
  const window = (r: ExplainedRun) =>
    `${periodClock(r.start, data.bounds)} – ${periodClock(r.end, data.bounds)}`; // a run starts and ends on scores
  // The Ask button sits above the bar, never on its button: the bar's
  // height goes to the page as --gf-m-bar-h.
  const bar = useRef<HTMLButtonElement>(null);
  useLayoutEffect(() => {
    const el = bar.current;
    if (!el) return;
    const root = document.documentElement.style,
      measure = () => root.setProperty("--gf-m-bar-h", `${el.offsetHeight}px`);
    measure();
    const watch = new ResizeObserver(measure);
    watch.observe(el);
    return () => {
      watch.disconnect();
      root.removeProperty("--gf-m-bar-h");
    };
  }, []);
  let title: string, sub: string, body: ReactNode;
  if (showRuns) {
    title = "Runs in this game";
    sub = `${data.runs.length} · tap one to see why it happened`;
    body = data.runs.map((r) => (
      <button
        key={`${r.side}:${r.start}`}
        type="button"
        className={`gf-m-runrow is-${r.side}${r.defining ? " is-defining" : ""}`}
        onClick={() => onRun(r)}
      >
        <span className="gf-m-dot" aria-hidden="true" />
        <b>
          {team(r)} {r.pts}-{r.oppPts}
          {r.defining ? " · defining" : ""}
        </b>
        <span className="gf-m-when">{window(r)}</span>
      </button>
    ));
  } else if (run) {
    title = `${team(run)} ${run.pts}-${run.oppPts} run${run.defining ? " · defining stretch" : ""}`;
    // The headline line's window and swing ("Q3 2:25 – Q4 2:31 · from down 20
    // to up 12") sit under the title; the body starts at the lineups.
    sub = run.lines[0]?.split(" · ").slice(1).join(" · ") || window(run);
    body = (
      <>
        <div className="gf-m-links">
          <button type="button" className="gf-m-link" onClick={onAllRuns}>
            ‹ All runs
          </button>
          <button type="button" className="gf-m-link" onClick={() => onShow(run)}>
            Show on timeline ›
          </button>
        </div>
        {run.lines.slice(1).map((line, i, rest) => {
          // "Team: what happened" / "Estimated cause: ...": the part before
          // the colon is set apart; the text itself is unchanged.
          const cut = line.indexOf(": "),
            cause = i === rest.length - 1;
          return (
            <p
              key={line}
              className={`gf-run-tip-line${cause ? " is-cause" : ""}`}
            >
              {cut > 0 ? (
                <>
                  <b className="gf-run-tip-lead">{line.slice(0, cut)}</b>
                  {line.slice(cut)}
                </>
              ) : (
                line
              )}
            </p>
          );
        })}
        {list}
      </>
    );
  } else if (selected.length || active) {
    const label = (k: string) =>
      data.bars.find((b) => laneKey(b) === k)?.player_label ?? k;
    title =
      selected.length > 1
        ? togetherText(data, selected, windows)
        : `${label(selected[0])} · ${active && !(active.start === 0 && active.end === data.bounds.at(-1)) ? "stint" : "game"}`;
    sub = selected.length > 1 ? selected.map(label).join(" + ") : prompt;
    body = (
      <>
        {list}
        <button type="button" className="gf-m-link" onClick={onClear}>
          Clear players
        </button>
      </>
    );
  } else if (lineups) {
    // Nothing tapped yet: each team's most-played lineups (user 2026-11-15).
    title = `Top ${TOP_LINEUPS} lineups per team`;
    sub = "Tap a player, a bar or a shaded run for more";
    body = (
      <>
        {list}
        <p className="ibpl-ribbon-inline-hint">{PHONE_HINT}</p>
      </>
    );
  } else {
    title = "Nothing selected";
    sub = "Tap a player, a bar or a shaded run";
    body = <p className="ibpl-ribbon-inline-hint">{PHONE_HINT}</p>;
  }
  return (
    <section className={`gf-m-panel${open ? " is-open" : ""}`} aria-label="Selection">
      <button
        ref={bar}
        type="button"
        className="gf-m-panel-bar"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      >
        <span className="gf-m-sum">
          {title}
          <span className="gf-m-sub">{sub}</span>
        </span>
        {/* What a tap opens, named (user 2026-11-12), pulsing once on each
            new selection; the plain arrow alone was easy to miss. */}
        {(open || showRuns || run || selected.length > 0 || active || lineups > 0) && (
          <span
            className={`gf-m-open${open ? " is-open" : ""}`}
            key={open ? "open" : selectionKey}
          >
            {open
              ? "Hide ▼"
              : showRuns
                ? "Runs ▲"
                : lineups
                  ? `Lineups (${lineups}) ▲`
                  : "Details ▲"}
          </span>
        )}
      </button>
      {open && <div className="gf-m-panel-body">{body}</div>}
    </section>
  );
}
/** A listed lineup's ratings over its windows, from the five's credited
 * events; null when the reader returned none. */
function lineupRatings(
  data: GameflowPayload,
  g: ReturnType<typeof aggregateSegments>[number],
) {
  const key = data.dict[g.dictIndex]?.lineup_key;
  if (!data.events.length || key == null) return null;
  const five = [
    ...new Set(
      data.lanes
        .filter((l) => l.side === g.side && l.lineup_key === key)
        .map((l) => l.player_key),
    ),
  ];
  return windowsRatings(data.events, g.side, five, g.windows);
}
/** " · +/- +6 (20–14)" for a listed lineup: its raw points for and against
 * beside the +/-, their difference (user 2026-11-12). Floor-based, as the
 * +/- is, so the two always agree. */
function lineupPmText(
  data: GameflowPayload,
  g: ReturnType<typeof aggregateSegments>[number],
) {
  if (!g.hasPm) return "";
  const d = windowsSummary(g.side, g.windows, data.steps);
  return d.pf == null
    ? ` · +/- ${pmLabel(g.pm)}`
    : ` · +/- ${d.pm || "0"} (${d.pf}–${d.pa})`;
}
/** The game's average rating, once per payload: the tones' midpoint. */
const gameBase = new WeakMap<GameflowPayload["events"], number | null>();
function baseOf(data: GameflowPayload) {
  if (!gameBase.has(data.events)) gameBase.set(data.events, gameRating(data.events));
  return gameBase.get(data.events)!;
}
/** A tone (-1 red .. +1 green) as a tint behind the number; none near 0. */
function toneStyle(t: number | null): CSSProperties | undefined {
  if (t == null || Math.abs(t) < 0.05) return undefined;
  return {
    backgroundColor: `rgba(${t > 0 ? "var(--gf-good-rgb)" : "var(--gf-bad-rgb)"}, ${(0.12 + 0.43 * Math.abs(t)).toFixed(2)})`,
  };
}
/** " · Net +12.3 · ORtg 110.0 · DRtg 97.7" for a listed lineup (user
 * 2026-11-12: Net with both ratings), or nothing without events. Each number
 * is tinted green or red by how it compares with the game (user 2026-11-15). */
function lineupRatingText(
  data: GameflowPayload,
  g: ReturnType<typeof aggregateSegments>[number],
) {
  const r = lineupRatings(data, g);
  if (!r) return null;
  const base = baseOf(data);
  const parts = [
    ["Net", "net", netLabel(r.net), r.net],
    ["ORtg", "ortg", ratingLabel(r.ortg), r.ortg],
    ["DRtg", "drtg", ratingLabel(r.drtg), r.drtg],
  ] as const;
  return parts.map(([label, kind, text, v]) => (
    <Fragment key={kind}>
      {" · "}
      <span className="gf-rtg" style={toneStyle(ratingTone(kind, v, base))}>
        {label} {text}
      </span>
    </Fragment>
  ));
}
/** A hovered stint in one line, for the status line above the chart: the
 * player, the window, time on the floor, +/-, Net and points. Its lineups are
 * a click away. */
function stintHoverLine(data: GameflowPayload, window: BarWindow) {
  const d = windowDetail(
      data.lanes,
      data.steps,
      data.dict,
      window.bar,
      window.start,
      window.end,
    ),
    rtg = data.events.length
      ? stintRatings(data.events, window.bar.side, [window.bar.player_key], window)
      : null;
  return {
    head: `${window.bar.player_label} · ${windowLabel(window, data.bounds)}`,
    stats:
      `${clock(d.end - d.start)} on the floor · +/- ${d.pm}` +
      (rtg ? ` · Net ${netLabel(rtg.net)}` : "") +
      (d.pf !== "" ? ` · ${d.pf} for, ${d.pa} against` : ""),
    hint: "Click the bar for its lineups ›",
  };
}
/** A hovered name in one line: the player's whole game -- time on the floor,
 * +/-, points for and against while on it, Net, ORtg and DRtg. Its lineups
 * are a click away (user 2026-11-18). */
function playerHoverLine(data: GameflowPayload, bar: Bar) {
  const key = laneKey(bar),
    stints = data.bars.filter((b) => laneKey(b) === key),
    total = data.totals.find((r) => laneKey(r) === key),
    known = stints.every((b) => b.pf != null && b.pa != null),
    pf = stints.reduce((s, b) => s + (b.pf ?? 0), 0),
    pa = stints.reduce((s, b) => s + (b.pa ?? 0), 0),
    rtg = data.events.length
      ? stintRatings(data.events, bar.side, [bar.player_key])
      : null;
  return {
    head: bar.player_label,
    stats:
      `${clock(total?.secs ?? 0)} on the floor · +/- ${pmLabel(total?.pm ?? null)}` +
      (known && stints.length ? ` · ${pf} for, ${pa} against` : "") +
      (rtg
        ? ` · Net ${netLabel(rtg.net)} · ORtg ${ratingLabel(rtg.ortg)} · DRtg ${ratingLabel(rtg.drtg)}`
        : ""),
    hint: "Click the name for their lineups ›",
  };
}
/** A run explained (the answer's runs[].lines): window, lineups, results,
 * cause. */
function GameflowRunTip({
  run,
  box,
}: {
  run: ExplainedRun;
  box: DOMRect;
}) {
  const lines = run.lines,
    width = Math.min(TIP_WIDTH, innerWidth - 16),
    above = box.top > innerHeight / 2;
  return (
    <div
      className="gf-hover-tip gf-run-tip"
      role="tooltip"
      style={{
        left: Math.max(8, Math.min(innerWidth - width - 8, box.left)),
        width,
        ...(above
          ? { bottom: innerHeight - box.top + 6 }
          : { top: box.bottom + 6 }),
      }}
    >
      {lines.map((line, i) => (
        <div
          key={line}
          className={`gf-run-tip-line${i === 0 ? " ibpl-ribbon-detail-head" : ""}`}
        >
          {line}
        </div>
      ))}
    </div>
  );
}
type CardStats = {
  secs: number;
  pm: string;
  pf: number | null;
  pa: number | null;
  /** Head to head: the score from both sides, in place of +/- and points. */
  score?: string;
  /** Credited-lineup ratings (shared_definitions.md, Gameflow stint rating). */
  ratings?: StintRatings;
  /** Head to head over the shared minutes: the +/- and Net once, signed for
   * the game log's team, and one ORtg per team. */
  h2h?: { team: string; pm: string; net: number | null; ortg: { name: string; v: number | null }[] };
};
// Ratings only, no possession count (user 2026-11-11).
const RATING_ROWS = [
  { dt: "Net", value: (r: StintRatings) => netLabel(r.net) },
  { dt: "ORtg", value: (r: StintRatings) => ratingLabel(r.ortg) },
  { dt: "DRtg", value: (r: StintRatings) => ratingLabel(r.drtg) },
];
function StintStats({ d }: { d: CardStats }) {
  if (d.score != null)
    return (
      <>
        <dl className="gf-stint-stats">
          <div>
            <dt>On the floor together</dt>
            <dd>{clock(d.secs)}</dd>
          </div>
        </dl>
        {/* A scoreboard (user 2026-11-12): each team with its ORtg under it,
            the score with the game log's +/- in the middle, Net under it. */}
        {d.h2h && d.pf != null && (
          <div className="gf-h2h-board">
            <span className="is-team">{d.h2h.ortg[0].name}</span>
            <span className="is-mid">
              {d.pf} – {d.pa} ({d.h2h.pm})
            </span>
            <span className="is-team">{d.h2h.ortg[1].name}</span>
            {d.h2h.ortg.some((o) => o.v != null) && (
              <>
                <span>ORtg {ratingLabel(d.h2h.ortg[0].v)}</span>
                <span className="is-mid">Net {netLabel(d.h2h.net)}</span>
                <span>ORtg {ratingLabel(d.h2h.ortg[1].v)}</span>
              </>
            )}
          </div>
        )}
      </>
    );
  return (
    <dl className="gf-stint-stats">
      <div>
        <dt>On the floor</dt>
        <dd>{clock(d.secs)}</dd>
      </div>
      <div>
        <dt>+/-</dt>
        <dd>{d.pm}</dd>
      </div>
      {d.pf != null && (
        <div>
          <dt>Points</dt>
          <dd>
            {d.pf} for, {d.pa} against
          </dd>
        </div>
      )}
      {d.ratings &&
        RATING_ROWS.map((row) => (
          <div key={row.dt}>
            <dt>{row.dt}</dt>
            <dd>{row.value(d.ratings!)}</dd>
          </div>
        ))}
    </dl>
  );
}
// D39: EuroLeague credits a trip to the lineup of its last free throw (D1).
const ratingsNote = (league: League) =>
  `Ratings count free throws for ${league === "il" ? "the lineup that drew the foul" : "the lineup on the floor for the last free throw"}, as On/Off does, so they can differ slightly from the +/- beside them.`;

function GameflowDetails({
  data,
  activeWindow,
  showStint,
  selected,
  groups,
  groupKey,
  focus,
  dispatch,
  detail,
  compact = false,
  run = null,
  top = false,
  folded = false,
  onFold,
}: {
  /** Nothing selected: the groups are each team's top lineups. */
  top?: boolean;
  /** Wide: the top lineups folded under their heading. */
  folded?: boolean;
  onFold?: () => void;
  compact?: boolean;
  /** A clicked run: the list holds its lineups. */
  run?: Run | null;
  data: GameflowPayload;
  activeWindow: BarWindow | null;
  showStint: boolean;
  selected: string[];
  groups: ReturnType<typeof aggregateSegments>;
  groupKey: (g: ReturnType<typeof aggregateSegments>[number]) => string;
  focus: string | null;
  dispatch: (a: SelectionAction) => void;
  detail: React.RefObject<HTMLElement | null>;
}) {
  // A card per click (user 2026-11-07): the stint clicked, else each selected
  // player's whole game, plus the minutes they shared when there are several.
  const bars = (key: string) => data.bars.filter((b) => laneKey(b) === key);
  // Phones list each team's top LINEUPS_SHORT lineups, the rest one tap away, so the
  // cards above the chart stay short (user 2026-11-11, minimal scrolling).
  const [showAll, setShowAll] = useState(false),
    // Per team, so a run's list keeps both teams.
    shown =
      compact && !showAll && !top
        ? groups.filter(
            (g, i) =>
              groups.slice(0, i).filter((h) => h.side === g.side).length <
              LINEUPS_SHORT,
          )
        : groups;
  // Credited-lineup ratings, when the reader returned events.
  const rate = (side: Side, keys: string[], window?: Span, windows?: Span[]) =>
    data.events.length
      ? windowsRatings(
          data.events,
          side,
          keys.map((k) => k.slice(k.indexOf(":") + 1)),
          windows ?? (window ? [window] : null),
        )
      : undefined;
  const cards: { key: string; title: string; aria: string; stats: CardStats }[] =
    [];
  if (activeWindow && showStint) {
    const d = windowDetail(
      data.lanes,
      data.steps,
      data.dict,
      activeWindow.bar,
      activeWindow.start,
      activeWindow.end,
    );
    cards.push({
      key: "stint",
      title: `${activeWindow.bar.player_label} · ${windowLabel(activeWindow, data.bounds)}`,
      aria: "Stint detail",
      stats: {
        secs: d.end - d.start,
        pm: d.pm,
        pf: d.pf === "" ? null : Number(d.pf),
        pa: d.pa === "" ? null : Number(d.pa),
        ratings: rate(activeWindow.bar.side, [laneKey(activeWindow.bar)], {
          start: activeWindow.start,
          end: activeWindow.end,
        }),
      },
    });
  } else {
    for (const key of selected) {
      const own = bars(key);
      if (!own.length) continue;
      cards.push({
        key,
        title: `${own[0].player_label} · game`,
        aria: `${own[0].player_label} game detail`,
        stats: {
          ...windowsSummary(
            own[0].side,
            own.map((b) => ({ start: b.start_elapsed, end: b.end_elapsed })),
            data.steps,
          ),
          ratings: rate(own[0].side, [key]),
        },
      });
    }
    const sides = new Set(selected.map((k) => bars(k)[0]?.side));
    if (selected.length > 1) {
      const shared = sharedWindows(data.bars, selected);
      if (sides.size === 1) {
        cards.push({
          key: "together",
          title: "Together",
          aria: "Shared minutes detail",
          stats: {
            ...windowsSummary([...sides][0]!, shared, data.steps),
            ratings: rate([...sides][0]!, selected),
          },
        });
      } else {
        // Both teams: the score from both, then over the shared minutes one
        // ORtg per team (its selected players as the credited filter) and
        // the +/- and Net once, signed for the game log's team (user
        // 2026-11-12: both teams' full lines were redundant). Net is the gap
        // between the two ORtgs, so the card adds up.
        const d = windowsSummary("own", shared, data.steps);
        const ortg = (["own", "opp"] as const).map((side) => ({
          name: side === "own" ? data.meta.ownTeam : data.meta.oppTeam,
          v:
            rate(
              side,
              selected.filter((k) => bars(k)[0]?.side === side),
              undefined,
              shared,
            )?.ortg ?? null,
        }));
        const h2h = {
          team: data.meta.ownTeam,
          pm: d.pf == null ? "" : pmLabel(d.pf - d.pa!) || "0",
          net: ortg[0].v == null || ortg[1].v == null ? null : ortg[0].v - ortg[1].v,
          ortg,
        };
        cards.push({
          key: "h2h",
          title: "Head to head",
          aria: "Head to head detail",
          stats: {
            ...d,
            h2h,
            score:
              d.pf == null
                ? ""
                : `${data.meta.ownTeam} ${d.pf} – ${d.pa} ${data.meta.oppTeam}`,
          },
        });
      }
      // What the players did together is the point of selecting several
      // (user 2026-11-11): its card leads, the single-player cards follow.
      cards.unshift(cards.pop()!);
    }
  }
  return (
    <>
      {cards.length > 0 && (
        <div className={`gf-player-cards${cards.length > 1 ? " is-multi" : ""}`}>
          {cards.map((c) => (
            <section
              className="ibpl-ribbon-detail is-card"
              aria-label={c.aria}
              key={c.key}
            >
              <div className="ibpl-ribbon-detail-head">{c.title}</div>
              {/* Numbers only: the lineups are listed, one per row, beside
                  (user 2026-10-28: the one-paragraph list was unreadable). */}
              <StintStats d={c.stats} />
            </section>
          ))}
          {cards.some((c) => c.stats.ratings) && (
            <p className="ibpl-ribbon-detail-meta gf-ratings-note">
              {ratingsNote(data.meta.league)}
            </p>
          )}
        </div>
      )}
      {groups.length > 0 && (
        <section
          className="ibpl-ribbon-detail ibpl-ribbon-shared-lineups"
          ref={detail}
          aria-label={top ? "Top lineups" : run ? "Run lineups" : selected.length > 1 ? "Shared lineups" : "Stint lineups"}
        >
          {top && !compact ? (
            <button
              type="button"
              className="ibpl-ribbon-detail-head gf-top-toggle"
              aria-expanded={!folded}
              onClick={onFold}
            >
              <span>
                Top {TOP_LINEUPS} lineups per team, by minutes
                {!folded && " · select a lineup to highlight its minutes"}
              </span>
              <span className="gf-top-fold">{folded ? "Show" : "Hide"}</span>
            </button>
          ) : top ? null : (
          <div className="ibpl-ribbon-detail-head">
            {run
              ? "Lineups in the run · time inside it only · select a lineup to highlight it"
              : selected.length > 1
              ? "Shared lineups · select a lineup to highlight its shared minutes"
              : "Lineups · select a lineup to highlight its minutes"}
          </div>
          )}
          {/* Sides in the groups' order: a run lists its team first. */}
          {!folded && [...new Set(shown.map((g) => g.side))].map(
            (side) =>
              shown.some((g) => g.side === side) && (
                <FragmentSide
                  key={side}
                  side={side}
                  title={side === "own" ? data.meta.ownTeam : data.meta.oppTeam}
                >
                  {shown
                    .filter((g) => g.side === side)
                    .map((g) => (
                      <button
                        type="button"
                        className={`ibpl-ribbon-detail-row ibpl-ribbon-shared-lineup${focus === groupKey(g) ? " is-active" : ""}`}
                        key={groupKey(g)}
                        aria-pressed={focus === groupKey(g)}
                        onClick={() => {
                          dispatch({ type: "focus", key: groupKey(g) });
                        }}
                        onPointerEnter={() =>
                          dispatch({
                            type: "preview",
                            preview: { windows: g.windows, lineup: g.dictIndex },
                          })
                        }
                        onPointerLeave={() =>
                          dispatch({ type: "preview", preview: null })
                        }
                      >
                        <span className="ibpl-ribbon-detail-members">
                          {data.dict[g.dictIndex] ? (
                            <LineupNames
                              value={data.dict[g.dictIndex].members
                                .split(" | ")
                                .join(", ")}
                            />
                          ) : (
                            "Lineup unavailable"
                          )}
                        </span>
                        {top ? (
                          <TopLineupMeta data={data} g={g} />
                        ) : (
                        <span className="ibpl-ribbon-detail-meta">
                          {clock(g.duration)}{" "}
                          {run ? "in the run" : selected.length > 1 ? "shared" : "total"}
                          {lineupPmText(data, g)}
                          {lineupRatingText(data, g)}{" "}
                          ·{" "}
                          {g.windows
                            .map((w) => windowLabel(w, data.bounds))
                            .join(", ")}
                        </span>
                        )}
                      </button>
                    ))}
                </FragmentSide>
              ),
          )}
          {shown.length < groups.length && (
            <button
              type="button"
              className="btn btn-default btn-sm gf-lineups-more"
              onClick={() => setShowAll(true)}
            >
              Show all {groups.length} lineups
            </button>
          )}
        </section>
      )}
    </>
  );
}
/** A top lineup's line: its minutes, then its own scoreboard -- points for
 * and against with the +/- -- then its ratings. No windows: a whole game's
 * list of them is long, and selecting the row draws them on the chart. */
function TopLineupMeta({
  data,
  g,
}: {
  data: GameflowPayload;
  g: ReturnType<typeof aggregateSegments>[number];
}) {
  const d = windowsSummary(g.side, g.windows, data.steps);
  return (
    <span className="ibpl-ribbon-detail-meta">
      {clock(g.duration)}
      {d.pf == null ? (
        lineupPmText(data, g)
      ) : (
        <>
          {" · "}
          <b className="gf-top-score">
            {d.pf}–{d.pa}
          </b>{" "}
          ({d.pm || "0"})
        </>
      )}
      {lineupRatingText(data, g)}
    </span>
  );
}
function FragmentSide({
  title,
  side,
  children,
}: {
  title: string;
  side: string;
  children: ReactNode;
}) {
  return (
    <>
      <div className="ibpl-ribbon-detail-head gf-side-head" data-side={side}>
        {title}
      </div>
      <div className="ibpl-ribbon-detail-list" data-side={side}>
        {children}
      </div>
    </>
  );
}
type PanelProps = {
  league: League;
  game: number;
  team: number;
  onClose: () => void;
  initial?: { url: string; data: GameflowPayload | null };
  standalone?: boolean;
  /** The Back bar's text, e.g. "Back to games". */
  backLabel?: string;
};
export default function GameflowPanel(props: PanelProps) {
  const state = useTabData<GameflowPayload>(
    gameflowPath(props.league, props.game, props.team),
    props.initial,
  );
  return <GameflowSurface {...props} {...state} />;
}
export function GameflowSurface({
  onClose,
  standalone = false,
  backLabel = "Back",
  backLink,
  data,
  loading,
  error,
  retry,
}: Pick<PanelProps, "onClose" | "standalone" | "backLabel"> & {
  /** The direct page's way back, on the title's row. */
  backLink?: ReactNode;
} & Pick<
    ReturnType<typeof useTabData<GameflowPayload>>,
    "data" | "loading" | "error" | "retry"
  >) {
  const narrow = useNarrow(),
    panel = useRef<HTMLElement>(null),
    back = useRef<HTMLButtonElement>(null);
  const close = useCallback(() => onClose(), [onClose]);
  useLayoutEffect(() => {
    if (standalone) return;
    // Note the link that opened Gameflow before moving focus to Back.
    const opener = document.activeElement as HTMLElement | null;
    back.current?.focus({ preventScroll: true });
    panel.current?.scrollIntoView({ block: "start" });
    // Closing returns focus to the link that opened it.
    return () => queueMicrotask(() => opener?.focus({ preventScroll: true }));
  }, [standalone]);
  const title = data
    ? `${data.meta.ownTeam} vs ${data.meta.oppTeam}`
    : "Gameflow";
  const content = loading ? (
    <p role="status">Loading gameflow…</p>
  ) : error ? (
    <div role="alert">
      {error}{" "}
      <button type="button" className="btn btn-default" onClick={retry}>
        Retry
      </button>
    </div>
  ) : !data?.lanes.length ? (
    <p>{labels.empty}</p>
  ) : (
    <GameflowView data={data} compact={narrow} />
  );
  // Always in the page, never a dialog (user 2026-10-28): the opener shows
  // it in place of its own content, with a Back bar on top.
  return (
    <section
      className="ibpl-ribbon-inline-panel ibpl-react-gameflow-panel"
      ref={panel}
      aria-label="Gameflow"
      onKeyDown={(e) => {
        if (e.key === "Escape" && !standalone) close();
      }}
    >
      <div className="ibpl-ribbon-inline-head">
        {!standalone && (
          <button
            type="button"
            className="ibpl-gameflow-back"
            aria-label="Close gameflow"
            onClick={close}
            ref={back}
          >
            <span aria-hidden="true">←</span> {backLabel}
          </button>
        )}
        {backLink}
        <strong>{title}</strong>
      </div>
      <div className="ibpl-ribbon-inline-result">{content}</div>
    </section>
  );
}
