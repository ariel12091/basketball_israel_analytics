"use client";
// One short key line directly above a stats table (design 2026-10-28 § 4),
// replacing the big legend boxes: only the parts that apply to the columns on
// screen, swatches drawn in the real colours, and "About this view" for the
// explainer and example that used to sit above the table.
import type { ReactNode } from "react";
import text from "@/data/layoutUi.json";
import { MUTED_HEAT } from "./palette";
import MoreInfo from "./MoreInfo";
import { ViewAbout, type Block } from "./Blocks";
import { useFrame } from "./TabKit";

const SWATCHES = [0, 5, 10, 15, 19].map((i) => MUTED_HEAT[i]);

export default function TableKey({
  shading,
  shots = false,
  rank = false,
  rankNote = "",
  extra,
  about,
}: {
  shading: boolean;
  /** Off/Def Shot cells are on screen. */
  shots?: boolean;
  /** Four Factors on/off detail is on (On/Off rank bars). */
  rank?: boolean;
  /** Who is ranked, e.g. "(Ranked Players: > 100 poss)". */
  rankNote?: string;
  extra?: ReactNode;
  about?: Block[];
}) {
  const t = text.key;
  const { openGlossary } = useFrame();
  const parts: ReactNode[] = [];
  if (shading)
    parts.push(
      <span key="shading" className="table-key-part">
        <b>{t.shading}</b> {t.low}
        <span className="table-key-scale" aria-hidden="true">
          {SWATCHES.map((c) => (
            <i key={c} style={{ background: c }} />
          ))}
        </span>
        {t.high}
      </span>,
    );
  if (shots)
    parts.push(
      <span key="shots" className="table-key-part">
        <b>{t.shots}</b> {t.accuracy} · {t.frequency}{" "}
        <i className="table-key-bar table-key-2pt" aria-hidden="true" />
        {t.two} <i className="table-key-bar table-key-3pt" aria-hidden="true" />
        {t.three}
      </span>,
    );
  if (rank)
    parts.push(
      <span key="rank" className="table-key-part">
        <b>{t.rank}</b> <i className="legend-icon-on" aria-hidden="true" />{" "}
        {t.on} <i className="legend-icon-off" aria-hidden="true" /> {t.off} ·{" "}
        {t.rankBar}
        {rankNote && <span className="table-key-note"> {rankNote}</span>}
      </span>,
    );
  if (extra)
    parts.push(
      <span key="extra" className="table-key-part">
        {extra}
      </span>,
    );
  if (about?.length)
    parts.push(
      <MoreInfo key="about" label={t.about} className="table-key-about">
        <ViewAbout blocks={about} onGlossary={openGlossary} />
      </MoreInfo>,
    );
  if (!parts.length) return null;
  return (
    <div className="table-key">
      {parts.map((p, i) => (
        <span key={i} className="table-key-item">
          {i > 0 && (
            <span className="table-key-sep" aria-hidden="true">
              ·
            </span>
          )}
          {p}
        </span>
      ))}
    </div>
  );
}

/** The key's parts from a view's exported blocks (the legends they replace):
 * shot_splits -> shots, rank -> rank (+ its note), explainer/example -> about. */
export function keyFromBlocks(blocks: Block[]) {
  const legend = (v: string) =>
    blocks.find(
      (b): b is Extract<Block, { kind: "legend" }> =>
        b.kind === "legend" && b.variant === v,
    );
  const rank = legend("rank");
  return {
    shots: !!legend("shot_splits"),
    rank: !!rank,
    rankNote: typeof rank?.note === "string" ? rank.note : "",
    about: blocks.filter((b) => b.kind === "explainer" || b.kind === "example"),
  };
}

/** table_hint()'s sentences for a MoreInfo table: on touch, definitions open
 * from the dashed-underlined header itself, not from an info button. */
export function moreInfoHint(hint: { pointer: string; touch: string }) {
  const touch = hint.touch
    .split(" · ")
    .map((part) =>
      /info button/i.test(part) ? text.hint.touchDefinition : part,
    )
    .join(" · ");
  return { ...hint, touch };
}
