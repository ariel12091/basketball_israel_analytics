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
  });

  it("does not tag a Net Rtg that keeps its sign without the luck", () => {
    const [r] = teamShootingLuck([{ ...teamA, net_rtg: 10 }], rates);
    expect(r.net_without_luck as number).toBeCloseTo(5.25, 6);
    expect(r.luck_flag).toBe(false);
  });

  it("never tags a Net Rtg of exactly zero", () => {
    expect(teamShootingLuck([{ ...teamA, net_rtg: 0 }], rates)[0].luck_flag).toBe(false);
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
