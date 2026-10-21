import { describe, it, expect } from "vitest";
import {
  minRanks,
  teamPercentiles,
  traditionalMode,
  teamPace,
  teamShotProfile,
  teamTableRows,
} from "./teamRatings";
import {
  teamPresets,
  loadTeam,
  teamOracleChecks,
} from "../../test/teamGoldens";
describe("Team Ratings R helpers", () => {
  it("min_rank leaves gaps, pr_vec averages ties and preserves NA", () => {
    expect(minRanks([10, 10, null, 30])).toEqual([2, 2, null, 1]);
    expect(teamPercentiles([10, 10, null, 30])).toEqual([0.25, 0.25, null, 1]);
    expect(teamPercentiles([10, 10, null, 30], true)).toEqual([
      0.75,
      0.75,
      null,
      0,
    ]);
    expect(teamPercentiles([null, 4])).toEqual([null, null]);
  });
  it("scales counting stats, floor possessions and minutes in each Traditional mode", () => {
    const raw = [
      { gp: 2, pts: 100, poss_on_floor: 200, minutes: 80, fg_pct: 50 },
    ];
    expect(traditionalMode(raw, "per_game")[0]).toMatchObject({
      pts: 50,
      poss_on_floor: 100,
      minutes: 40,
      fg_pct: 50,
    });
    expect(traditionalMode(raw, "per75")[0]).toMatchObject({
      pts: 37.5,
      poss_on_floor: 75,
      minutes: 30,
      fg_pct: 50,
    });
    expect(traditionalMode(raw, "per40")[0]).toMatchObject({
      pts: 50,
      poss_on_floor: 100,
      minutes: 80,
      fg_pct: 50,
    });
    expect(raw[0].pts).toBe(100);
    expect(
      traditionalMode([{ gp: 2, pts: null }], "per_game")[0].pts,
    ).toBeNull();
  });
  it("uses measured minutes and applies only the Israeli regulation fallback", () => {
    const raw = [{ team_id: 1, games_played: 2, off_poss: 160, def_poss: 150 }];
    expect(
      teamPace(raw, [{ team_id: 1, game_minutes: 85 }], true)[0].off_pace,
    ).toBe((160 / 85) * 40);
    expect(teamPace(raw, [], false)[0].off_pace).toBe(80);
    expect(teamPace(raw, [], true)[0].off_pace).toBeNull();
  });
  it("ranks current stat-filtered teams against the full previous population", () => {
    const now = [
      { team_id: 1, net_rtg: 10 },
      { team_id: 2, net_rtg: 20 },
      { team_id: 3, net_rtg: 30 },
    ];
    const prev = [
      { team_id: 3, net_rtg: 3 },
      { team_id: 1, net_rtg: 1 },
      { team_id: 2, net_rtg: 2 },
    ];
    const r = teamTableRows(
      now,
      prev,
      "summary",
      [{ col: "net_rtg", op: "ge", value: 20 }],
      true,
    );
    expect(r.map((r) => r.rank_net_rtg)).toEqual([2, 1]);
    expect(r.map((r) => r.delta_net_rtg)).toEqual([0, 0]);
    expect(
      teamTableRows(now, prev, "summary", [], false).every(
        (r) => r.delta_net_rtg == null,
      ),
    ).toBe(true);
  });
  it("preserves FF percentiles before stat filters and flips Traditional polarity", () => {
    const rows = [
      { team_id: 1, off_ppp: 10, pts: 10, tov: 1 },
      { team_id: 2, off_ppp: 20, pts: 20, tov: 2 },
      { team_id: 3, off_ppp: 30, pts: 30, tov: 3 },
    ];
    expect(
      teamTableRows(
        rows,
        [],
        "four_factors",
        [{ col: "off_ppp", op: "le", value: 20 }],
        false,
      )[1].pr_off_ppp,
    ).toBe(0.5);
    const def = teamTableRows(rows, [], "traditional", [], false, true);
    expect(def[0].rank_pts).toBe(1);
    expect(def[0].rank_tov).toBe(3);
  });
  it("derives shot diet from counts with unknown corner location remaining null", () => {
    const r = teamShotProfile(
      [
        {
          team_id: 1,
          off_fga: 10,
          off_layup_att: 2,
          off_dunk_att: 1,
          off_fg3_att: 4,
          off_c3_att: 0,
          off_c3_known_att: 0,
        },
      ],
      [{ team_id: 1, off_efg: 60 }],
    )[0];
    expect(r).toMatchObject({
      off_rim_share: 30,
      off_mid_share: 30,
      off_fg3_share: 40,
      off_c3_pct3: null,
      off_efg: 60,
    });
  });
});
describe("Team Ratings: recorded Shiny tables", () => {
  it("requires all 45 reference frames", () =>
    expect(teamPresets).toHaveLength(45));
  for (const file of teamPresets)
    it(file, () => {
      const { g, rows } = loadTeam(file),
        expected = g.expected.data;
      expect(rows).toHaveLength(expected.length);
      for (const e of expected) {
        const row = rows.find(
          (r) => String(r.team_name) === String(e.team_name ?? e.Team),
        );
        expect(row, `team ${e.team_name ?? e.Team}`).toBeDefined();
        for (const [label, got, want] of teamOracleChecks(e, row!, g.view)) {
          if (typeof got === "number" && typeof want === "number")
            expect(
              got,
              `${g.id} ${e.team_name ?? e.Team} ${label}`,
            ).toBeCloseTo(want, 8);
          else
            expect(got, `${g.id} ${e.team_name ?? e.Team} ${label}`).toEqual(
              want,
            );
        }
      }
    });
});
