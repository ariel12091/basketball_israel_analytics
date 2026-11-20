"use client";
import type { ReactNode } from "react";
import MoreInfo from "./MoreInfo";

const signed = (v: number) => (v > 0 ? "+" : "") + v.toFixed(1);

/** A Net cell with its luck tag (Lineups' "3PT luck", Team Ratings'
 * "Shooting luck"); the tag opens what the luck added, its parts, and the
 * net without it (the On/Off tag's popover sections). */
export default function LuckNet({
  value,
  label,
  addedLabel,
  parts,
  withoutLabel,
  without,
}: {
  value: ReactNode;
  label: string;
  addedLabel: string;
  parts: { label: string; value: number }[];
  withoutLabel: string;
  without: number;
}) {
  const added = parts.reduce((s, p) => s + p.value, 0);
  return (
    <span className="onoff-net-tip">
      {value}{" "}
      <MoreInfo label={label} className="onoff-luck-tag">
        <div className="onoff-tip-pop" data-kind="luck">
          <div className="onoff-tip-section is-luck">
            <div className="onoff-tip-title">{label}</div>
            <div className="onoff-tip-row">
              <span>{addedLabel}</span>
              <b className="onoff-tip-val">{signed(added)}</b>
            </div>
            <div className="onoff-tip-sub">
              {parts.map((p) => `${p.label} ${signed(p.value)}`).join(" · ")}
            </div>
            <div className="onoff-tip-row">
              <span>{withoutLabel}</span>
              <b className="onoff-tip-val">{signed(without)}</b>
            </div>
          </div>
        </div>
      </MoreInfo>
    </span>
  );
}
