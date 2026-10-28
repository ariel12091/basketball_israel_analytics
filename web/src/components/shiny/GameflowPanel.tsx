"use client";
import {
  useCallback,
  useLayoutEffect,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import type { League } from "@/rules/gameLogs";
import type { GameflowPayload } from "@/server/gameflowRepo";
import {
  aggregateSegments,
  clock,
  laneKey,
  marginPath,
  pmLabel,
  scoreAt,
  sharedLineups,
  sharedWindows,
  windowDetail,
  windowSegments,
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
    [focus, setFocus] = useState<string | null>(null),
    [preview, setPreview] = useState<Window[] | null>(null),
    [full, setFull] = useState(false),
    [quarter, setQuarter] = useState(1),
    [score, setScore] = useState("");
  const cards = useRef(new Map<number, HTMLElement>()),
    detail = useRef<HTMLElement>(null);
  const windows = sharedWindows(data.bars, selected),
    total = windows.reduce((sum, w) => sum + w.end - w.start, 0);
  const groups =
    selected.length > 1
      ? sharedLineups(data, selected, windows)
      : active
        ? aggregateSegments(
            windowSegments(
              data.lanes,
              data.steps,
              data.dict,
              active.bar.side,
              active.bar.player_key,
              active.start,
              active.end,
            ),
          )
        : [];
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
    setSelected(next);
    setFocus(null);
    setPreview(null);
    setHover(null);
    if (next.length === 1) {
      const bar =
        next[0] === key ? b : data.bars.find((b) => laneKey(b) === next[0])!;
      setActive(
        next[0] === key && window
          ? window
          : { bar, start: bar.start_elapsed, end: bar.end_elapsed },
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
  const common = {
    data,
    compact,
    selected,
    highlight,
    onPlayer: (bar: Bar) => toggle(bar),
    onBar: (window: BarWindow) => toggle(window.bar, window),
    onHover: setHover,
    active: hover ?? active,
    lineupIndex: focused?.dictIndex,
  };
  const prompt = selected.length
    ? `${selected.map((key) => data.bars.find((b) => laneKey(b) === key)?.player_label ?? key).join(" + ")} · ${total ? clock(total) + (selected.length > 1 ? " on court together" : " on court") : "No shared court time"}. Select a player again to remove them.`
    : labels.prompt;
  const activeWindow = hover ?? active;
  return (
    <div className="ibpl-ribbon-game ibpl-react-gameflow">
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
                  setScore(
                    `${clock(elapsed)} · ${data.meta.ownTeam} ${scoreAt(data.steps, elapsed, "own")} – ${scoreAt(data.steps, elapsed, "own") - scoreAt(data.steps, elapsed)} ${data.meta.oppTeam}`,
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
        <div className="ibpl-ribbon-inline-scroll ibpl-gameflow-full">
          <GameflowChart {...common} />
        </div>
      ) : (
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
      )}
      {activeWindow && selected.length < 2 && (
        <section
          className="ibpl-ribbon-detail is-card"
          aria-label="Stint detail"
        >
          <div className="ibpl-ribbon-detail-head">
            {activeWindow.bar.player_label} · {clock(activeWindow.start)}–
            {clock(activeWindow.end)}
          </div>
          <div>
            {
              windowDetail(
                data.lanes,
                data.steps,
                data.dict,
                activeWindow.bar,
                activeWindow.start,
                activeWindow.end,
              ).label
            }
          </div>
        </section>
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
    </div>
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
  data,
  loading,
  error,
  retry,
}: Pick<PanelProps, "onClose" | "standalone" | "backLabel"> &
  Pick<
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
        <strong>{title}</strong>
      </div>
      <div className="ibpl-ribbon-inline-result">{content}</div>
    </section>
  );
}
