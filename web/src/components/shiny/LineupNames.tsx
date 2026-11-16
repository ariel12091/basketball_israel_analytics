// A lineup's players as one line, names split by middle dots (user
// 2026-11-16). The readers return "A, B, C"; the dots are presentation only,
// so CSV export and sorting keep the reader's string. Each name stays on one
// line; the line wraps between names.
import { Fragment } from "react";

export function lineupNameList(value: unknown): string[] {
  return String(value ?? "")
    .split(/\s*,\s*/)
    .filter(Boolean);
}

export function lineupDotted(value: unknown): string {
  return lineupNameList(value).join(" · ");
}

export default function LineupNames({ value }: { value: unknown }) {
  const names = lineupNameList(value);
  return (
    <span className="lineup-names">
      {names.map((n, i) => (
        <Fragment key={i}>
          {i > 0 && (
            <span className="lineup-names-sep" aria-hidden="true">
              {" · "}
            </span>
          )}
          <span className="lineup-names-name">{n}</span>
        </Fragment>
      ))}
    </span>
  );
}
