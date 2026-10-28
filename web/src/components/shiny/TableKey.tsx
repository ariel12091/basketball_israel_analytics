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
  extra,
  about,
}: {
  shading: boolean;
  /** Off/Def Shot cells are on screen. */
  shots?: boolean;
  /** Four Factors on/off detail is on (On/Off rank bars). */
  rank?: boolean;
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
      </span>,
    );
  if (extra) parts.push(<span key="extra" className="table-key-part">{extra}</span>);
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
          {i > 0 && <span className="table-key-sep" aria-hidden="true">·</span>}
          {p}
        </span>
      ))}
    </div>
  );
}
