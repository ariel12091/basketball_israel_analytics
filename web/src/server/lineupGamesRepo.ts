import "server-only";
import { withSnapshot, Deadline } from "./db";
import type { League, Row } from "../rules/gameLogs";
import { rate, zero, pivotLineupGames } from "../rules/lineups";
import { rRound } from "../rules/round";
import { ParamError } from "./api";
/** The existing Tab 2 / Tab 10 modal queries. These are season game logs,
 * intentionally independent of the filters on the parent table, as in Shiny. */
export function lineupGames(league: League, s: URLSearchParams) {
  const season = Number(s.get("season")),
    team = Number(s.get("team")),
    hash = s.get("hash") ?? "",
    ff = league === "il" && s.get("view") === "four_factors";
  if (!Number.isInteger(season) || season < 2000 || season > 2100)
    throw new ParamError("season", "Invalid season");
  if (!Number.isSafeInteger(team) || team <= 0)
    throw new ParamError("team", "Invalid team");
  if (!/^[a-zA-Z0-9_,{}-]{1,500}$/.test(hash))
    throw new ParamError("hash", "Invalid lineup identity");
  return withSnapshot(new Deadline(25000, 20000), async (q) => {
    let rows: Row[];
    if (league !== "il") {
      rows = await q<Row>(
        `SELECT f.game_date,f.round_number,f.opp_team_name,f.is_home,
        sum(l.possessions) FILTER(WHERE l.type_lineup='offense') AS off_poss,
        sum(l.points) FILTER(WHERE l.type_lineup='offense') AS off_pts,
        sum(l.possessions) FILTER(WHERE l.type_lineup='defense') AS def_poss,
        sum(l.points) FILTER(WHERE l.type_lineup='defense') AS def_pts,
        ${["off", "def"].flatMap((side) => ["fg2_made", "fg2_att", "fg3_made", "fg3_att"].map((col, i) => `sum(l.${col}) FILTER(WHERE l.type_lineup='${side === "off" ? "offense" : "defense"}') AS ${side}_${["fg2m", "fg2a", "fg3m", "fg3a"][i]}`)).join(",")},
        round(sum(l.seconds) FILTER(WHERE l.type_lineup='offense')/60.0,1) AS minutes
        FROM euroleague.sub_lineups sl JOIN euroleague.lineup_totals_by_game l
          ON l.competition=sl.competition AND l.game_year=sl.game_year AND l.team_id=sl.team_id AND l.lineup_key=sl.lineup_key
        JOIN euroleague.final_schedule_mv f ON f.game_id=l.game_id AND f.team_id=l.team_id
        WHERE sl.competition=$1::text AND sl.game_year=$2::int4 AND sl.unit_key=$3::text
        GROUP BY f.game_date,f.round_number,f.opp_team_name,f.is_home ORDER BY f.game_date`,
        [league === "eurocup" ? "U" : "E", season, hash],
      );
    } else {
      const hashes = (
        await q<Row>(
          "SELECT DISTINCT lineup_hash FROM basketball_test.sub_lineups WHERE sub_lineup_hash=$1 AND team_id=$2 AND game_year=$3",
          [hash, team, season],
        )
      ).map((r) => r.lineup_hash);
      if (!hashes.length) hashes.push(hash);
      const schedule = await q<Row>(
        "SELECT game_id,gn,game_date,opp_team_name,team_score,opp_score,team_score>opp_score AS has_won FROM basketball_test.final_schedule_mv WHERE team_id=$1 AND game_year=$2",
        [team, season],
      );
      const raw = await q<Row>(
        ff
          ? `SELECT game_id,type_lineup,SUM(total_points) AS pts,SUM(total_poss) AS poss,
        SUM(ts_poss_count) AS ts_poss,SUM(oreb_count) AS oreb,SUM(oreb_opportunities) AS oreb_opp,SUM(tov_count) AS tov,
        SUM(total_ft_attempts) AS fta,SUM(total_fga) AS fga,SUM(total_fgm) AS fgm,SUM(total_fg3_made) AS fg3m,SUM(minutes) AS mins
        FROM basketball_test.lineup_four_factors_by_game WHERE lineup_hash=ANY($1::text[]) AND team_id=$2 AND game_year=$3 GROUP BY game_id,type_lineup`
          : `SELECT game_id,type_lineup,SUM(total_poss) AS poss,SUM(total_pts) AS pts,SUM(fg2_made) AS fg2m,SUM(fg2_att) AS fg2a,SUM(fg3_made) AS fg3m,SUM(fg3_att) AS fg3a,SUM(minutes) AS mins
        FROM basketball_test.mv_lineup_totals_by_day WHERE lineup_hash=ANY($1::text[]) AND team_id=$2 AND game_year=$3 GROUP BY game_id,type_lineup`,
        [hashes, team, season],
      );
      rows = pivotLineupGames(raw)
        .flatMap((r) => {
          const sched = schedule.find(
            (g) => String(g.game_id) === String(r.game_id),
          );
          if (!sched) return [];
          return [
            {
              ...r,
              ...sched,
              minutes: rRound(zero(r.off_mins), 1),
              result: sched.has_won ? "W" : "L",
              score_display: `${sched.team_score}-${sched.opp_score}`,
            } as Row,
          ];
        })
        .sort((a, b) => zero(a.gn) - zero(b.gn));
    }
    const roundRate = (num: unknown, den: unknown) => {
      const v = rate(num, den);
      return v == null ? null : rRound(v, 1);
    };
    rows = rows.map((r) => {
      const out: Row = {
        ...r,
        off_poss: zero(r.off_poss),
        def_poss: zero(r.def_poss),
        off_pts: zero(r.off_pts),
        def_pts: zero(r.def_pts),
        off_ppp: roundRate(r.off_pts, r.off_poss),
        def_ppp: roundRate(r.def_pts, r.def_poss),
        total_poss: zero(r.off_poss) + zero(r.def_poss),
        plus_minus: zero(r.off_pts) - zero(r.def_pts),
        venue: r.is_home ? "H" : "A",
      };
      out.net_rtg =
        league === "il"
          ? rRound(zero(out.off_ppp) - zero(out.def_ppp), 1)
          : out.off_ppp == null || out.def_ppp == null
            ? null
            : rRound(Number(out.off_ppp) - Number(out.def_ppp), 1);
      if (ff)
        for (const side of ["off", "def"]) {
          out[`${side}_efg`] = roundRate(
            zero(r[`${side}_fgm`]) + 0.5 * zero(r[`${side}_fg3m`]),
            r[`${side}_fga`],
          );
          for (const [metric, n, d] of [
            ["oreb_pct", "oreb", "oreb_opp"],
            ["tov_pct", "tov", "poss"],
            ["ftr", "fta", "fga"],
          ])
            out[`${side}_${metric}`] = roundRate(
              r[`${side}_${n}`],
              r[`${side}_${d}`],
            );
        }
      return out;
    });
    return {
      rows,
      meta: { version: "", view: ff ? "four_factors" : "summary" },
    };
  });
}
