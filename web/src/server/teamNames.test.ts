import { describe, it, expect, vi } from "vitest";
import { applyTeamNamesHe, readTeamNamesHe, teamNamesFromRows } from "./teamNames";
import { displaySpec } from "../ask/display";

vi.mock("server-only", () => ({}));

const names = teamNamesFromRows([
  {
    team_id: 14,
    name_he: "מכבי תפוזינה ראשון לציון",
    team_name: "M. RISHON",
    team_name_schedule: "M. RISHON",
    team_name_rosters: "RISHON LEZION",
  },
  { team_id: 2, name_he: "מכבי Rapyd תל אביב", team_name: "MACCABI TEL AVIV", team_name_schedule: null, team_name_rosters: null },
  { team_id: 3, name_he: " ", team_name: "HAPOEL TEL AVIV", team_name_schedule: null, team_name_rosters: null },
]);

describe("Hebrew team names for the Ask chat", () => {
  it("matches a row by team_id first", () => {
    const [r] = applyTeamNamesHe([{ team_id: 14, team_name: "ANY", pts: 80 }], names);
    expect(r).toEqual({ team_id: 14, team_name: "ANY", pts: 80, team_name_he: "מכבי תפוזינה ראשון לציון" });
  });

  it("matches a name-only row by any of the three English spellings, case-insensitively", () => {
    const rows = applyTeamNamesHe(
      [{ team: "M. RISHON" }, { Team: "rishon lezion " }, { entity_name: "MACCABI TEL AVIV" }, { team: "Maccabi Tel-Aviv" }],
      names,
    );
    expect(rows.map((r) => r.team_name_he)).toEqual([
      "מכבי תפוזינה ראשון לציון",
      "מכבי תפוזינה ראשון לציון",
      "מכבי Rapyd תל אביב",
      "מכבי Rapyd תל אביב",
    ]);
  });

  it("adds the opponent's Hebrew name from opp_team_name", () => {
    const [r] = applyTeamNamesHe([{ opp_team_name: "MACCABI TEL AVIV", score_display: "80-75" }], names);
    expect(r.opp_team_name_he).toBe("מכבי Rapyd תל אביב");
    expect(r.team_name_he).toBeUndefined();
  });

  it("leaves rows without a known Hebrew name unchanged (blank names are unknown)", () => {
    const row = { team_id: 3, team_name: "HAPOEL TEL AVIV" };
    expect(applyTeamNamesHe([row], names)[0]).toBe(row);
    // A player's name never matches a team.
    expect(applyTeamNamesHe([{ name: "Some Player" }], names)[0].team_name_he).toBeUndefined();
  });

  it("is empty outside the Israeli league", async () => {
    const he = await readTeamNamesHe("euroleague", 2025);
    expect(he.byId.size + he.byName.size).toBe(0);
  });

  it("offers team_name_he / opp_team_name_he as chat-only columns in Israeli specs only", () => {
    expect(displaySpec("team_ratings", "il", null).team_name_he).toMatchObject({ chatOnly: true });
    expect(displaySpec("list_teams", "il", null).team_name_he).toMatchObject({ chatOnly: true });
    expect(displaySpec("gameflow", "il", null).team_name_he).toMatchObject({ chatOnly: true });
    // Gameflow's player rows carry the Hebrew player name too (gameflowRepo).
    expect(displaySpec("gameflow", "il", null).player_he).toMatchObject({ chatOnly: true });
    expect(displaySpec("game_logs", "il", null).opp_team_name_he).toMatchObject({ chatOnly: true });
    expect(displaySpec("team_ratings", "euroleague", null).team_name_he).toBeUndefined();
    // A player list without a team column gets none.
    expect(displaySpec("lineup_roster", "il", null).team_name_he).toBeUndefined();
  });
});
