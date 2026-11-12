import { describe, it, expect, vi } from "vitest";
import { applyPlayerNames, readPlayerNames } from "./playerNames";
import type { Query } from "./db";

vi.mock("server-only", () => ({}));

const names = new Map([
  ["1:10", { en: "Owner Ten", he: "בעלים עשר" }],
  ["1:11", { en: "Owner Eleven", he: null }],
]);

describe("Player display name owner (D27)", () => {
  it("reads player_display_names_v for the Israeli league only", async () => {
    const q = vi.fn(async (sql: string) => sql.includes("etl_full_last_success") ? [{ value: "v1" }] : [
      { team_id: 1, player_id: 10, name: "Owner Ten", name_he: "בעלים עשר", height_m: "2.03", position_en: "F" },
      { team_id: 1, player_id: 11, name: "Owner Eleven", name_he: " " },
      { team_id: 1, player_id: 12, name: null, name_he: null },
    ]) as unknown as Query & ReturnType<typeof vi.fn>;
    const m = await readPlayerNames(q as Query, "il", 2026);
    const nameCall = q.mock.calls.find((c) => String(c[0]).includes("player_display_names_v"))!;
    expect(nameCall[0]).toContain("basketball_test.player_display_names_v");
    expect(nameCall[1]).toEqual([2026]);
    expect([...m]).toEqual([
      // basket.co.il's height and position ride along (lineup order).
      ["1:10", { en: "Owner Ten", he: "בעלים עשר", height: 2.03, position: "F" }],
      ["1:11", { en: "Owner Eleven", he: null, height: null, position: null }],
    ]);
    // Cached per season: a second request does not re-read the view.
    await readPlayerNames(q as Query, "il", 2026);
    expect(q.mock.calls.filter((c) => String(c[0]).includes("player_display_names_v"))).toHaveLength(1);
    const e = vi.fn<Query>(async () => []);
    expect((await readPlayerNames(e as Query, "euroleague", 2025)).size).toBe(
      0,
    );
    expect(e).not.toHaveBeenCalled();
  });

  it("rebuilds lineup names in player_ids order, keeping unknown players", () => {
    const [r] = applyPlayerNames(
      [
        {
          team_id: 1,
          player_ids: "{11,99,10}",
          player_names: ["Roster Eleven", "Roster Ninety", "Roster Ten"],
          player_names_str: "Roster Eleven, Roster Ninety, Roster Ten",
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        } as any,
      ],
      names,
    );
    expect(r.player_names_str).toBe("Owner Eleven, Roster Ninety, Owner Ten");
    expect(r.player_names).toEqual([
      "Owner Eleven",
      "Roster Ninety",
      "Owner Ten",
    ]);
    // A Hebrew lineup name only when every player has one.
    expect(r.player_names_he_str).toBeUndefined();
  });

  it("gives a lineup its Hebrew names when all players have one", () => {
    const [r] = applyPlayerNames(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      [{ team_id: 1, player_ids: "{10}", player_names: ["R"], player_names_str: "R", Players: "R" } as any],
      names,
    );
    expect(r.player_names_he_str).toBe("בעלים עשר");
    // The Lineups table's own column follows too.
    expect(r.Players).toBe("Owner Ten");
  });

  it("names On/Off, Compare and picker rows by (team, player)", () => {
    const rows = applyPlayerNames(
      [
        { team_id: 1, player_id: 10, "First Name": "R", "Last Name": "Ten" },
        { team_id: 1, player_id: 11, player_name: "Roster Eleven" },
        { team_id: 1, player_id: 10, name: "R Ten" },
        { team_id: 2, player_id: 10, name: "Other team" },
        { team_id: 1, player_id: 10, ppg: 3 },
      ],
      names,
    );
    expect(rows[0].Player).toBe("Owner Ten");
    expect(rows[0].name_he).toBe("בעלים עשר");
    expect(rows[1].player_name).toBe("Owner Eleven");
    expect(rows[1].name_he).toBeUndefined();
    expect(rows[2].name).toBe("Owner Ten");
    expect(rows[3]).toEqual({ team_id: 2, player_id: 10, name: "Other team" });
    expect(rows[4]).toEqual({ team_id: 1, player_id: 10, ppg: 3, name_he: "בעלים עשר" });
  });
});
