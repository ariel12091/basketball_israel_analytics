"use client";
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
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
import { detectRuns, runExplanation, type Run } from "@/rules/gameflowRuns";
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
    { selected, active, hover, hoverBox, focus, preview } = selection,
    gameEnd = data.bounds[data.bounds.length - 1],
    [lineupsUsed, setLineupsUsed] = useState<number | null>(null),
    [full, setFull] = useState(false),
    [quarter, setQuarter] = useState(1),
    [score, setScore] = useState(""),
    // Each team's ORtg in the score readouts (user 2026-11-07): since tip-off
    // by default, or over its last RATING_WINDOW possessions.
    [ratingMode, setRatingMode] = useState<RatingMode | "off">("game"),
    // Scoring runs (shared_definitions.md, Gameflow run) and the open tip.
    runs = useMemo(() => detectRuns(data.steps), [data.steps]),
    [runTip, setRunTip] = useState<{
      run: Run;
      box: DOMRect;
      pinned: boolean;
    } | null>(null);
  const cards = useRef(new Map<number, HTMLElement>()),
    detail = useRef<HTMLElement>(null),
    chartBox = useRef<HTMLDivElement>(null),
    lineupsBox = useRef<HTMLDivElement>(null),
    space =useChartSpace(compact, chartBox),
    // The click that opened the chart leaves the pointer resting on a bar. That
    // is not a hover: acting on it would cover the first look with a tooltip.
    pointerMoved = useRef(false),
    restingOn = useRef<{ window: BarWindow; box: DOMRect } | null>(null);
  const windows = sharedWindows(data.bars, selected),
    total = windows.reduce((sum, w) => sum + w.end - w.start, 0);
  const groups =
    selected.length > 1
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
      ? selected.length > 1
        ? focused.windows
        : echo!
      : stint
        ? [{ start: stint.start, end: stint.end }]
        : selected.length
          ? windows
          : hover
            ? [{ start: hover.start, end: hover.end }]
            : []);
  const toggle = (bar: Bar, window?: BarWindow) =>
    dispatch({ type: "toggle", bar, window, bars: data.bars, end: gameEnd });
  const clear = () => dispatch({ type: "clear" });
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
    openRun: runTip?.run ?? null,
    onRun: (run: Run | null, box: DOMRect | null, how: RunTrigger) =>
      setRunTip((tip) => {
        // A tap pins the tip (touch has no hover) and a second tap closes it;
        // a pointer shows it while resting and leaves a pinned one alone.
        if (how === "tap")
          return run && !(tip?.pinned && tip.run === run)
            ? { run, box: box!, pinned: true }
            : null;
        if (tip?.pinned) return tip;
        return run && box ? { run, box, pinned: false } : null;
      }),
  };
  const prompt = stint
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
      dispatch={dispatch}
      detail={detail}
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
  return (
    <div
      className="ibpl-ribbon-game ibpl-react-gameflow"
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
      {runTip && <GameflowRunTip data={data} run={runTip.run} box={runTip.box} />}
      {data.health && (
        <div className="alert alert-warning py-2 px-3 mb-2" role="note">
          {data.health}
        </div>
      )}
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
        {data.ratings && (
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
        )}
      </div>
      {compact && (
        <>
          <div className="ibpl-ribbon-inline-hint">{labels.hint}</div>
          <div className="ibpl-ribbon-mobile-overview">
            <div className="ibpl-ribbon-overview-title">{labels.overview}</div>
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
              <path
                className="ibpl-ribbon-overview-line"
                d={marginPath(data.margin, data.bounds.at(-1)!, 320, 4, 44, 0)}
              />
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
                    `${clock(elapsed)} · ${data.meta.ownTeam} ${scoreAt(data.steps, elapsed, "own")} – ${scoreAt(data.steps, elapsed, "own") - scoreAt(data.steps, elapsed)} ${data.meta.oppTeam}${rtg ? ` · ORtg${ratingMode === "window" ? ` (last ${RATING_WINDOW})` : ""} ${ratingLabel(rtg.own)} – ${ratingLabel(rtg.opp)}` : ""}`,
                  );
                }}
                onPointerLeave={() => setScore("")}
              />
            </svg>
            {score && <output role="tooltip">{score}</output>}
            <div
              className="ibpl-ribbon-quarter-nav"
              role="group"
              aria-label="Jump to quarter"
            >
              {data.bounds.map((_, i) => (
                <button
                  key={i}
                  type="button"
                  className={`ibpl-ribbon-quarter-jump${quarter === i + 1 ? " is-active" : ""}`}
                  aria-label={`Jump to ${i < 4 ? `Q${i + 1}` : `OT${i - 3}`}`}
                  onClick={() => {
                    setFull(false);
                    setQuarter(i + 1);
                    requestAnimationFrame(() =>
                      cards.current.get(i + 1)?.scrollIntoView({
                        block: "start",
                        behavior: "smooth",
                      }),
                    );
                  }}
                >
                  {i < 4 ? `Q${i + 1}` : `OT${i - 3}`}
                </button>
              ))}
            </div>
          </div>
          <button
            type="button"
            className="ibpl-ribbon-full-toggle"
            aria-expanded={full}
            onClick={() => {
              setFull(!full);
              dispatch({ type: "hover", window: null, box: null });
            }}
          >
            {full ? labels.quarters : labels.full}
          </button>
        </>
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
          {/* Quarter cards (phones): the lineups sit right above them. */}
          {details}
          <div className="ibpl-ribbon-quarters">
            {data.bounds.map((end, i) => (
              <section
                key={end}
                className="ibpl-ribbon-quarter-card"
                data-quarter={i + 1}
                ref={(el) => {
                  if (el) cards.current.set(i + 1, el);
                  else cards.current.delete(i + 1);
                }}
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
            ))}
          </div>
        </>
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
/** A listed lineup's Net over its windows, from the five's credited events. */
function lineupNet(
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
  return windowsRatings(data.events, g.side, five, g.windows).net;
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
        {window.bar.player_label} · {clock(window.start)}–{clock(window.end)}
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
/** A run explained (runExplanation()): window, lineups, results, cause. */
function GameflowRunTip({
  data,
  run,
  box,
}: {
  data: GameflowPayload;
  run: Run;
  box: DOMRect;
}) {
  const lines = runExplanation(data, run, {
      own: data.meta.ownTeam,
      opp: data.meta.oppTeam,
    }),
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
      <dl className="gf-stint-stats">
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
// D38: EuroLeague credits a trip to the lineup of its last free throw (D1).
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
}: {
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
  // Credited-lineup ratings, when the reader returned events.
  const rate = (side: Side, keys: string[], window?: Span) =>
    data.events.length
      ? stintRatings(
          data.events,
          side,
          keys.map((k) => k.slice(k.indexOf(":") + 1)),
          window,
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
      title: `${activeWindow.bar.player_label} · ${clock(activeWindow.start)}–${clock(activeWindow.end)}`,
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
        // Both teams: no side to sign a +/- from, so the score from both.
        const d = windowsSummary("own", shared, data.steps);
        cards.push({
          key: "h2h",
          title: "Head to head",
          aria: "Head to head detail",
          stats: {
            ...d,
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
          aria-label={selected.length > 1 ? "Shared lineups" : "Stint lineups"}
        >
          <div className="ibpl-ribbon-detail-head">
            {selected.length > 1
              ? "Shared lineups · select a lineup to highlight its shared minutes"
              : "Lineups · select a lineup to highlight its minutes"}
          </div>
          {(["own", "opp"] as const).map(
            (side) =>
              groups.some((g) => g.side === side) && (
                <FragmentSide
                  key={side}
                  title={side === "own" ? data.meta.ownTeam : data.meta.oppTeam}
                >
                  {groups
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
                          {selected.length > 1 ? "shared" : "total"}
                          {g.hasPm ? ` · +/- ${pmLabel(g.pm)}` : ""}
                          {data.events.length
                            ? ` · Net ${netLabel(lineupNet(data, g))}`
                            : ""}{" "}
                          ·{" "}
                          {g.windows
                            .map((w) => `${clock(w.start)}–${clock(w.end)}`)
                            .join(", ")}
                        </span>
                      </button>
                    ))}
                </FragmentSide>
              ),
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
