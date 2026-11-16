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

/** "JIMMY CLARK III, GABRIEL "IFFE" LUNDBERG" -> "CLARK III · LUNDBERG": the
 * family names only, for the label that floats over a row scrolled sideways
 * (user 2026-11-16). A quoted nickname is dropped; two players of one lineup
 * with the same family name keep their first initial. */
export function lineupSurnames(value: unknown): string {
  const split = lineupNameList(value).map((name) => {
    const words = name.split(/\s+/).filter((w) => !/^["“'].*["”']$/.test(w));
    return {
      given: words.length > 1 ? words[0] : "",
      family: (words.length > 1 ? words.slice(1) : words).join(" "),
    };
  });
  return split
    .map((s) =>
      s.given && split.filter((t) => t.family === s.family).length > 1
        ? `${s.given[0]}. ${s.family}`
        : s.family,
    )
    .join(" · ");
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
