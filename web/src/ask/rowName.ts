// The name the Ask answer's table shows for a quoted row: whose numbers they
// are. A player's or a lineup's name before its team's; the Hebrew name in a
// Hebrew answer when the page gave one.

const text = (cells: Record<string, unknown>, key: string) => {
  const v = cells[key];
  return v == null ? "" : String(v).trim();
};
const first = (cells: Record<string, unknown>, keys: string[]) =>
  keys.map((k) => text(cells, k)).find(Boolean) ?? "";

export function rowName(
  cells: Record<string, unknown>,
  lang: "he" | "en",
): string {
  const player =
    (lang === "he" ? first(cells, ["name_he"]) : "") ||
    first(cells, ["Player", "player_name"]) ||
    `${text(cells, "First Name")} ${text(cells, "Last Name")}`.trim();
  if (player) return player;
  const lineup =
    (lang === "he" ? first(cells, ["player_names_he_str"]) : "") ||
    first(cells, ["Players", "player_names_str"]);
  if (lineup) return lineup;
  return (
    (lang === "he" ? first(cells, ["team_name_he"]) : "") ||
    first(cells, ["team_name", "Team", "entity_name", "team"])
  );
}
