"use client";
import {
  Fragment,
  useCallback,
  useEffect,
  useLayoutEffect,
  useReducer,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
  type RefObject,
} from "react";
import type { League } from "@/rules/gameLogs";
import type { GameflowPayload } from "@/server/gameflowRepo";
import {
  aggregateSegments,
  clock,
  fitGameflowLayout,
  laneKey,
  lineupsRoom,
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
  netLabel,
  stintRatings,
  windowsRatings,
  type StintRatings,
} from "@/rules/gameflowRatings";
import { useTabData } from "./TabKit";
import { NARROW } from "./layoutClasses";
import GameflowChart, { type RunTrigger } from "./GameflowChart";
import {
  periodClock,
  runLineupGroups,
  windowLabel,
  type Run,
} from "@/rules/gameflowRuns";
import type { ExplainedRun } from "@/rules/gameflowSummary";
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
    { selected, active, hover, hoverBox, focus, preview, run } = selection,
    gameEnd = data.bounds[data.bounds.length - 1],
    [lineupsUsed, setLineupsUsed] = useState<number | null>(null),
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
  // A clicked run's band: both teams' lineups during it (user 2026-11-11).
  const groups = run
    ? runLineupGroups(data, run)
    : selected.length > 1
      ? sharedLineups(data, selected, windows)
      : active
        ? stintLineups(data, active)
        : [];
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
  const highlight =
    preview ??
    (focused
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
    room = space && lineupsRoom(lanes, space),
    // The lineups take only what they fill, up to the room; the chart the rest.
    taken = room != null && lineupsUsed != null ? Math.min(room, lineupsUsed) : room,
    layout = space
      ? fitGameflowLayout(
          lanes,
          lineupsOpen && taken != null
            ? { ...space, height: space.height - taken }
            : space,
          lineupsOpen,
        )
      : undefined;
  // Phones: a tap outside the panel closes a run's details or the runs list
  // (user 2026-11-12). A tap on the chart that does so does nothing else.
  const detailsOpen = compact && panelOpen && (run != null || panelRuns);
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
  }, [active, selected, room]);
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
    active: hover ?? active,
    stintBar: stint?.bar,
    lineupIndex: focused?.dictIndex,
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
      const run = r as ExplainedRun | null;
      setRunTip((tip) => {
        // A tap pins the tip (touch has no hover) and a second tap closes it;
        // a pointer shows it while resting and leaves a pinned one alone.
        if (how === "tap")
          return run && !(tip?.pinned && tip.run === run)
            ? { run, box: box!, pinned: true }
            : null;
        if (tip?.pinned) return tip;
        return run && box ? { run, box, pinned: false } : null;
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
      {!compact && hover && hoverBox && !runTip && (
        <GameflowHoverTip data={data} window={hover} box={hoverBox} />
      )}
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
          {prompt}
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
              <path
                className="ibpl-ribbon-overview-line"
                d={marginPath(data.margin, data.bounds.at(-1)!, 320, 4, 44, 0)}
              />
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
const TIP_WIDTH = 400,
  // At most two lineups in a bar's tip (user 2026-11-11); the rest are a
  // click away.
  TIP_ROWS = 2;
const LINEUPS_SHORT = 3;
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
  onRun,
  onAllRuns,
  onShow,
  onClear,
}: {
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
        {run.lines.slice(1).map((line, i, rest) => (
          <p
            key={line}
            className={`gf-run-tip-line${i === rest.length - 1 ? " is-cause" : ""}`}
          >
            {line}
          </p>
        ))}
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
  } else {
    title = "Nothing selected";
    sub = "Tap a player, a bar or a shaded run";
    body = <p className="ibpl-ribbon-inline-hint">{PHONE_HINT}</p>;
  }
  return (
    <section className="gf-m-panel" aria-label="Selection">
      <button
        type="button"
        className="gf-m-panel-bar"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      >
        <span className="gf-m-sum">
          {title}
          <span className="gf-m-sub">{sub}</span>
        </span>
        <span className="gf-m-chev" aria-hidden="true">
          {open ? "▼" : "▲"}
        </span>
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
/** " · Net +12.3 · ORtg 110.0 · DRtg 97.7" for a listed lineup (user
 * 2026-11-12: Net with both ratings), or "" without events. */
function lineupRatingText(
  data: GameflowPayload,
  g: ReturnType<typeof aggregateSegments>[number],
) {
  const r = lineupRatings(data, g);
  return r
    ? ` · Net ${netLabel(r.net)} · ORtg ${ratingLabel(r.ortg)} · DRtg ${ratingLabel(r.drtg)}`
    : "";
}
/** What a hovered stint holds, floating clear of its bar: nothing moves. */
function GameflowHoverTip({
  data,
  window,
  box,
}: {
  data: GameflowPayload;
  window: BarWindow;
  box: DOMRect;
}) {
  const d = windowDetail(
      data.lanes,
      data.steps,
      data.dict,
      window.bar,
      window.start,
      window.end,
    ),
    lineups = stintLineups(data, window),
    rtg = data.events.length
      ? stintRatings(data.events, window.bar.side, [window.bar.player_key], window)
      : null,
    above = box.top > innerHeight / 2;
  return (
    <div
      className="gf-hover-tip"
      role="tooltip"
      style={{
        left: Math.max(
          8,
          Math.min(
            innerWidth - TIP_WIDTH - 8,
            box.left + box.width / 2 - TIP_WIDTH / 2,
          ),
        ),
        width: TIP_WIDTH,
        ...(above
          ? { bottom: innerHeight - box.top + 6 }
          : { top: box.bottom + 6 }),
      }}
    >
      <div className="ibpl-ribbon-detail-head">
        {window.bar.player_label} · {windowLabel(window, data.bounds)}
      </div>
      <div className="gf-hover-tip-stats">
        {clock(d.end - d.start)} on the floor · +/- {d.pm}
        {rtg && ` · Net ${netLabel(rtg.net)}`}
        {d.pf !== "" && ` · ${d.pf} for, ${d.pa} against`}
      </div>
      {lineups.slice(0, TIP_ROWS).map((g) => (
        <div className="gf-hover-tip-row" key={g.dictIndex}>
          <div>
            {data.dict[g.dictIndex]?.members.split(" | ").join(" · ") ??
              "Lineup unavailable"}
          </div>
          <div className="ibpl-ribbon-detail-meta">
            {clock(g.duration)}
            {g.hasPm ? ` · +/- ${pmLabel(g.pm)}` : ""}
            {lineupRatingText(data, g)}
          </div>
        </div>
      ))}
      {lineups.length > TIP_ROWS && (
        <div className="ibpl-ribbon-detail-meta">
          +{lineups.length - TIP_ROWS} more. Click the bar to see them all.
        </div>
      )}
    </div>
  );
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
  /** Head to head: each team over the shared minutes, its +/- and ratings. */
  teams?: { name: string; pm: string; ratings?: StintRatings }[];
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
      <dl className="gf-stint-stats is-h2h">
        <div>
          <dt>On the floor together</dt>
          <dd>{clock(d.secs)}</dd>
        </div>
        {d.score !== "" && (
          <div>
            <dt>Score</dt>
            <dd className="is-score">{d.score}</dd>
          </div>
        )}
        {d.teams?.map((t) => (
          <div className="gf-h2h-team" key={t.name}>
            <dt>{t.name}</dt>
            <dd>
              +/- {t.pm}
              {t.ratings &&
                ` · Net ${netLabel(t.ratings.net)} · ORtg ${ratingLabel(t.ratings.ortg)} · DRtg ${ratingLabel(t.ratings.drtg)}`}
            </dd>
          </div>
        ))}
      </dl>
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
}: {
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
      compact && !showAll
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
        // Both teams: no side to sign a +/- from, so the score from both, then
        // each team over the shared minutes with its own +/- and ratings
        // (user 2026-11-12), its selected players as the credited filter.
        const d = windowsSummary("own", shared, data.steps);
        const teams = (["own", "opp"] as const).map((side) => {
          const mine = selected.filter((k) => bars(k)[0]?.side === side),
            pm = d.pf == null ? null : side === "own" ? d.pf - d.pa! : d.pa! - d.pf;
          return {
            name: side === "own" ? data.meta.ownTeam : data.meta.oppTeam,
            pm: pmLabel(pm) || "0",
            ratings: rate(side, mine, undefined, shared),
          };
        });
        cards.push({
          key: "h2h",
          title: "Head to head",
          aria: "Head to head detail",
          stats: {
            ...d,
            teams,
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
          aria-label={run ? "Run lineups" : selected.length > 1 ? "Shared lineups" : "Stint lineups"}
        >
          <div className="ibpl-ribbon-detail-head">
            {run
              ? "Lineups in the run · time inside it only · select a lineup to highlight it"
              : selected.length > 1
              ? "Shared lineups · select a lineup to highlight its shared minutes"
              : "Lineups · select a lineup to highlight its minutes"}
          </div>
          {/* Sides in the groups' order: a run lists its team first. */}
          {[...new Set(shown.map((g) => g.side))].map(
            (side) =>
              shown.some((g) => g.side === side) && (
                <FragmentSide
                  key={side}
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
                          dispatch({ type: "preview", windows: g.windows })
                        }
                        onPointerLeave={() =>
                          dispatch({ type: "preview", windows: null })
                        }
                      >
                        <span className="ibpl-ribbon-detail-members">
                          {data.dict[g.dictIndex]?.members
                            .split(" | ")
                            .join(" · ") ?? "Lineup unavailable"}
                        </span>
                        <span className="ibpl-ribbon-detail-meta">
                          {clock(g.duration)}{" "}
                          {run ? "in the run" : selected.length > 1 ? "shared" : "total"}
                          {g.hasPm ? ` · +/- ${pmLabel(g.pm)}` : ""}
                          {lineupRatingText(data, g)}{" "}
                          ·{" "}
                          {g.windows
                            .map((w) => windowLabel(w, data.bounds))
                            .join(", ")}
                        </span>
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
function FragmentSide({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <>
      <div className="ibpl-ribbon-detail-head">{title}</div>
      <div className="ibpl-ribbon-detail-list">{children}</div>
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
