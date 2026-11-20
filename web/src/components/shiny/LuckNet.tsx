"use client";
import type { ReactNode } from "react";
import MoreInfo from "./MoreInfo";
import { luckSide, luckTag } from "@/rules/luck";

const signed = (v: number) => (v > 0 ? "+" : "") + v.toFixed(1);

/** Which way the luck went, in words (Team Ratings, Lineups and On/Off). */
export function luckNote(luck: number): string {
  return luckSide(luck) === "better"
    ? "Unlucky: without it the net would be higher."
    : "Lucky: without it the net would be lower.";
}

/** A Net cell with its luck tag ("▲ Unlucky" / "▼ Lucky", rules/luck); the
 * tag opens what the luck added, its parts, the net without it and which
 * way it went (the On/Off tag's popover sections). */
export default function LuckNet({
  value,
  title,
  parts,
  withoutLabel,
  without,
}: {
  value: ReactNode;
  /** What was measured: "Shooting luck" or "3PT luck". */
  title: string;
  parts: { label: string; value: number }[];
  withoutLabel: string;
  without: number;
}) {
  const luck = parts.reduce((s, p) => s + p.value, 0),
    tag = luckTag(luck);
  return (
    <span className="onoff-net-tip">
      {value}{" "}
      <MoreInfo label={tag} ariaLabel={`${tag}: ${title}`} className="onoff-luck-tag">
        <div className="onoff-tip-pop" data-kind="luck">
          <div className="onoff-tip-section is-luck">
            <div className="onoff-tip-title">{tag}</div>
            <div className="onoff-tip-row">
              <span>{title} per 100</span>
              <b className="onoff-tip-val">{signed(luck)}</b>
            </div>
            <div className="onoff-tip-sub">
              {parts.map((p) => `${p.label} ${signed(p.value)}`).join(" · ")}
            </div>
            <div className="onoff-tip-row">
              <span>{withoutLabel}</span>
              <b className="onoff-tip-val">{signed(without)}</b>
            </div>
            <div className="onoff-tip-note">{luckNote(luck)}</div>
          </div>
        </div>
      </MoreInfo>
    </span>
  );
}
