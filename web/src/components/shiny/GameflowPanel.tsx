"use client";
import {
  useCallback,
  useLayoutEffect,
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
  type Window,
} from "@/rules/gameflow";
import { useTabData } from "./TabKit";
import { NARROW } from "./layoutClasses";
import GameflowChart, { type BarWindow } from "./GameflowChart";
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
  const [selected, setSelected] = useState<string[]>([]),
    [active, setActive] = useState<BarWindow | null>(null),
    [hover, setHover] = useState<BarWindow | null>(null),
    [hoverBox, setHoverBox] = useState<DOMRect | null>(null),
    [lineupsUsed, setLineupsUsed] = useState<number | null>(null),
    [focus, setFocus] = useState<string | null>(null),
    [preview, setPreview] = useState<Window[] | null>(null),
    [full, setFull] = useState(false),
    [quarter, setQuarter] = useState(1),
    [score, setScore] = useState(""),
    // Each team's ORtg in the score readouts (user 2026-11-07): since tip-off
    // by default, or over its last RATING_WINDOW possessions.
    [ratingMode, setRatingMode] = useState<RatingMode | "off">("game");
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
  const stint =
    selected.length === 1 &&
    active &&
    !(active.start === 0 && active.end === data.bounds[data.bounds.length - 1])
      ? active
      : null;
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
  const toggle = (b: Bar, window?: BarWindow) => {
    const key = laneKey(b),
      next = selected.includes(key)
        ? selected.filter((p) => p !== key)
        : [...selected, key];
    setFocus(null);
    setPreview(null);
    setHover(null);
    // Another stint of the player on show moves to it, rather than deselecting.
    if (
      window &&
      stint &&
      laneKey(stint.bar) === key &&
      stint.start !== window.start
    ) {
      setActive(window);
      return;
    }
    setSelected(next);
    if (next.length === 1) {
      const bar =
        next[0] === key ? b : data.bars.find((b) => laneKey(b) === next[0])!;
      // A name selects the player's whole game (every lineup he was part
      // of); a bar selects just that stint (user 2026-10-28).
      setActive(
        next[0] === key && window
          ? window
          : { bar, start: 0, end: data.bounds[data.bounds.length - 1] },
      );
    } else setActive(null);
  };
  const clear = () => {
    setSelected([]);
    setActive(null);
    setHover(null);
    setFocus(null);
    setPreview(null);
  };
  // Wide: hovering moves nothing (it made bars slide out from under the pointer
  // on the way to a click, user 2026-11-04), so it shows a tooltip, and only a
  // click opens the details and shrinks the chart. Narrow keeps its inline card.
  const activeWindow = compact ? (hover ?? active) : active;
  const showStint =
    !!activeWindow &&
    selected.length < 2 &&
    !(
      activeWindow.start === 0 &&
      activeWindow.end === data.bounds[data.bounds.length - 1]
    );
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
      setHover(window);
      setHoverBox(box);
    },
    active: hover ?? active,
    stintBar: stint?.bar,
    lineupIndex: focused?.dictIndex,
    layout,
    ratingMode,
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
      setFocus={setFocus}
      setPreview={setPreview}
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
          setHover(restingOn.current.window);
          setHoverBox(restingOn.current.box);
        }
      }}
      onKeyDown={() => {
        // Keyboard users never move the pointer: their focus is a real hover.
        pointerMoved.current = true;
      }}
    >
      {!compact && hover && hoverBox && (
        <GameflowHoverTip data={data} window={hover} box={hoverBox} />
      )}
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
            <span className="ibpl-gameflow-rtg-title">
              Offensive rating (ORtg)
              <small>points per 100 possessions</small>
            </span>
            {(
              [
                ["off", "Off"],
                ["game", "So far"],
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
              setHover(null);
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
  TIP_ROWS = 6;
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
type CardStats = {
  secs: number;
  pm: string;
  pf: number | null;
  pa: number | null;
  /** Head to head: the score from both sides, in place of +/- and points. */
  score?: string;
};
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
    </dl>
  );
}

function GameflowDetails({
  data,
  activeWindow,
  showStint,
  selected,
  groups,
  groupKey,
  focus,
  setFocus,
  setPreview,
  detail,
}: {
  data: GameflowPayload;
  activeWindow: BarWindow | null;
  showStint: boolean;
  selected: string[];
  groups: ReturnType<typeof aggregateSegments>;
  groupKey: (g: ReturnType<typeof aggregateSegments>[number]) => string;
  focus: string | null;
  setFocus: (k: string | null) => void;
  setPreview: (w: Window[] | null) => void;
  detail: React.RefObject<HTMLElement | null>;
}) {
  // A card per click (user 2026-11-07): the stint clicked, else each selected
  // player's whole game, plus the minutes they shared when there are several.
  const bars = (key: string) => data.bars.filter((b) => laneKey(b) === key);
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
        stats: windowsSummary(
          own[0].side,
          own.map((b) => ({ start: b.start_elapsed, end: b.end_elapsed })),
          data.steps,
        ),
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
          stats: windowsSummary([...sides][0]!, shared, data.steps),
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
    }
  }
  return (
    <>
      {cards.length > 0 && (
        <div className="gf-player-cards">
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
                          setFocus(focus === groupKey(g) ? null : groupKey(g));
                          setPreview(null);
                        }}
                        onPointerEnter={() => setPreview(g.windows)}
                        onPointerLeave={() => setPreview(null)}
                      >
                        <span className="ibpl-ribbon-detail-members">
                          {data.dict[g.dictIndex]?.members
                            .split(" | ")
                            .join(" · ") ?? "Lineup unavailable"}
                        </span>
                        <span className="ibpl-ribbon-detail-meta">
                          {clock(g.duration)}{" "}
                          {selected.length > 1 ? "shared" : "total"}
                          {g.hasPm ? ` · +/- ${pmLabel(g.pm)}` : ""} ·{" "}
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
