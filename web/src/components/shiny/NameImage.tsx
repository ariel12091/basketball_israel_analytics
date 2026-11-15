"use client";
// PREVIEW PROTOTYPE (react/logos): a small logo or headshot in front of a
// team or player name. Decorative -- the name beside it says the same -- so
// alt="" keeps screen readers on the name. Served through Next's image
// optimiser (a ~1 KB WebP instead of basket.co.il's 30-60 KB original),
// lazily, at a fixed size so rows never shift. A row with no image (no photo,
// a TOTAL row, a failed load) keeps an empty slot of the same size, so every
// name in the column starts at the same x.
import Image from "next/image";
import { useState } from "react";
import type { Row } from "@/rules/gameLogs";
import { playerPhoto, teamLogo, type ImageScope } from "@/lib/images";

const SIZE = 22;

export function NameImage({
  src,
  kind,
}: {
  src: string | null;
  kind: "logo" | "photo";
}) {
  const [failed, setFailed] = useState(false);
  if (!src || failed)
    return <span className="ibpl-name-img" aria-hidden="true" />;
  return (
    <Image
      src={src}
      alt=""
      width={SIZE}
      height={SIZE}
      loading="lazy"
      className={`ibpl-name-img ibpl-name-img-${kind}`}
      onError={() => setFailed(true)}
    />
  );
}

// Which name columns carry which image, and the row fields that key it.
const TEAM_COLUMNS: Record<string, string> = {
  team_name: "team_id",
  Team: "team_id",
  opp_team_name: "opp_team_id",
};
const PLAYER_COLUMNS = new Set(["Player", "player_name"]);

/** The image slot for one name cell: null when the column carries no images
 * (not a team/player name column, EuroLeague); src null when this row has
 * none (TOTAL row, missing ids, no photo) but the column does. */
export function nameCellImage(
  scope: ImageScope,
  column: string,
  row: Row,
): { src: string | null; kind: "logo" | "photo" } | null {
  if (!scope || scope.league !== "il") return null;
  const total = Boolean(row.is_total || row.is_multi_team_total);
  if (column in TEAM_COLUMNS)
    return {
      src: total ? null : teamLogo(scope, row[TEAM_COLUMNS[column]]),
      kind: "logo",
    };
  if (PLAYER_COLUMNS.has(column))
    return {
      src: total ? null : playerPhoto(scope, row.team_id, row.player_id),
      kind: "photo",
    };
  return null;
}
