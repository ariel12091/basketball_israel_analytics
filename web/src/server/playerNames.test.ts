import { describe, it, expect, vi } from "vitest";
import { applyPlayerNames, readPlayerNames } from "./playerNames";
import type { Query } from "./db";

vi.mock("server-only", () => ({}));

const names = new Map([
  ["1:10", "Owner Ten"],
  ["1:11", "Owner Eleven"],
]);

describe("Player display name owner (D27)", () => {
  it("reads resolved_player_identity_v for the Israeli league only", async () => {
    const q = vi.fn(async () => [
      { team_id: 1, player_id: 10, name: "Owner Ten" },
      { team_id: 1, player_id: 12, name: null },
    ]) as unknown as Query & ReturnType<typeof vi.fn>;
    const m = await readPlayerNames(q as Query, "il", 2026);
    expect(q.mock.calls[0][0]).toContain("resolved_player_identity_v");
    expect(q.mock.calls[0][0]).toContain(
      "HAVING COUNT(DISTINCT display_name)=1",
    );
    expect(q.mock.calls[0][1]).toEqual([2026]);
    expect([...m]).toEqual([["1:10", "Owner Ten"]]);
    // Cached per season: a second request does not re-read the view.
    await readPlayerNames(q as Query, "il", 2026);
    expect(q).toHaveBeenCalledTimes(1);
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
    expect(rows[1].player_name).toBe("Owner Eleven");
    expect(rows[2].name).toBe("Owner Ten");
    expect(rows[3].name).toBe("Other team");
    expect(rows[4]).toEqual({ team_id: 1, player_id: 10, ppg: 3 });
  });
});
