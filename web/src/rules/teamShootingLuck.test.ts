import { describe, it, expect } from "vitest";
import { leagueShootingRates, teamShootingLuck } from "./teamRatings";

// docs/shared_definitions.md "Shooting luck (Team Ratings)": own and
// opponents' 3PT luck plus the opponents' FT luck, at league-average rates.
const rates = { r3: 0.35, rft: 0.75 };
const teamA = {
  team_id: 1,
  net_rtg: 2.0,
  off_poss: 1000,
  def_poss: 1000,
  off_fg3_made: 120,
  off_fg3_att: 300,
  def_fg3_made: 100,
  def_fg3_att: 300,
  def_ftm: 200,
  def_fta: 250,
};

describe("teamShootingLuck", () => {
  it("splits the luck into own 3PT, opponents' 3PT and opponents' FT, and tags a sign flip", () => {
    const [r] = teamShootingLuck([teamA], rates);
    expect(r.luck_3pt_ours as number).toBeCloseTo(4.5, 6);
    expect(r.luck_3pt_theirs as number).toBeCloseTo(1.5, 6);
    expect(r.luck_ft_theirs as number).toBeCloseTo(-1.25, 6);
    expect(r.net_without_luck as number).toBeCloseTo(-2.75, 6);
    expect(r.luck_flag).toBe(true);
    expect(r.luck_tag).toBe("▼ Lucky");
  });

  it("tags luck of half the Net or more without a sign flip, pointing up when it hurt", () => {
    // Galil Elion 26-27 shape: Net +2.8, luck -3.3 (119% of it), +6.1 without.
    const [r] = teamShootingLuck(
      [{ ...teamA, net_rtg: 2.8, off_fg3_made: 105, def_fg3_made: 116, def_ftm: 187.5 }],
      rates,
    );
    expect(r.net_without_luck as number).toBeCloseTo(6.1, 6);
    expect(r.luck_flag).toBe(true);
    expect(r.luck_tag).toBe("▲ Unlucky");
  });

  it("does not tag a Net Rtg that keeps its sign without the luck", () => {
    const [r] = teamShootingLuck([{ ...teamA, net_rtg: 10 }], rates);
    expect(r.net_without_luck as number).toBeCloseTo(5.25, 6);
    // luck 4.75 is 47.5% of the Net: under half, no flip
    expect(r.luck_flag).toBe(false);
    expect(r.luck_tag).toBeNull();
  });

  it("tags a Net Rtg of exactly zero only from the 1.5 floor", () => {
    expect(teamShootingLuck([{ ...teamA, net_rtg: 0 }], rates)[0].luck_tag).toBe("▼ Lucky");
    const small = { ...teamA, net_rtg: 0, off_fg3_made: 105, def_fg3_made: 105, def_ftm: 197.5 };
    expect(teamShootingLuck([small], rates)[0].luck_flag).toBe(false); // luck -1.0
  });

  it("gives a zero FT term when the opponents took no free throws", () => {
    const [r] = teamShootingLuck([{ ...teamA, def_ftm: 0, def_fta: 0 }], rates);
    expect(r.luck_ft_theirs).toBe(0);
    expect(r.net_without_luck as number).toBeCloseTo(-4, 6);
  });

  it("leaves the luck empty and untagged without defensive possessions", () => {
    const [r] = teamShootingLuck([{ ...teamA, def_poss: 0 }], rates);
    expect(r.luck_3pt_theirs).toBeNull();
    expect(r.luck_ft_theirs).toBeNull();
    expect(r.net_without_luck).toBeNull();
    expect(r.luck_flag).toBe(false);
  });

  it("returns rows without the counts unchanged", () => {
    const { def_ftm: _drop, ...noFt } = teamA;
    expect(teamShootingLuck([noFt], rates)).toEqual([noFt]);
  });

  it("defaults the rates to the rows' league average", () => {
    const b = { ...teamA, team_id: 2, off_ftm: 160, off_fta: 200 };
    const a = { ...teamA, off_ftm: 140, off_fta: 200 };
    const [r] = teamShootingLuck([a, b]);
    // r3 = 240 / 600 = 0.4, rft = 300 / 400 = 0.75
    expect(r.luck_3pt_ours as number).toBeCloseTo(0, 6);
    expect(r.luck_ft_theirs as number).toBeCloseTo(-1.25, 6);
  });
});

describe("leagueShootingRates", () => {
  it("is the league average over every team's own shots", () => {
    expect(
      leagueShootingRates([
        { off_fg3_made: 30, off_fg3_att: 100, off_ftm: 70, off_fta: 100, def_fg3_made: 99, def_fg3_att: 100 },
        { off_fg3_made: 40, off_fg3_att: 100, off_ftm: 80, off_fta: 100 },
      ]),
    ).toEqual({ r3: 0.35, rft: 0.75 });
  });

  it("is null for a rate with no attempts", () => {
    expect(leagueShootingRates([{ off_fg3_made: 0, off_fg3_att: 0, off_ftm: 0, off_fta: 0 }])).toEqual({
      r3: null,
      rft: null,
    });
  });
});
