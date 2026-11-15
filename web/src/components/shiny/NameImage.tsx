"use client";
// PREVIEW PROTOTYPE (react/logos): a small logo or headshot in front of a
// team or player name. Decorative -- the name beside it says the same -- so
// alt="" keeps screen readers on the name. Served through Next's image
// optimiser (a ~1 KB WebP instead of basket.co.il's 30-60 KB original),
// lazily, at a fixed size so rows never shift. A failed load removes the
// image and leaves the name.
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
  if (!src || failed) return null;
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

/** The image for one name cell, or null (unknown column, TOTAL row, missing
 * ids, EuroLeague). */
export function nameCellImage(
  scope: ImageScope,
  column: string,
  row: Row,
): { src: string; kind: "logo" | "photo" } | null {
  if (!scope || row.is_total || row.is_multi_team_total) return null;
  if (column in TEAM_COLUMNS) {
    const src = teamLogo(scope, row[TEAM_COLUMNS[column]]);
    return src ? { src, kind: "logo" } : null;
  }
  if (PLAYER_COLUMNS.has(column)) {
    const src = playerPhoto(scope, row.team_id, row.player_id);
    return src ? { src, kind: "photo" } : null;
  }
  return null;
}
