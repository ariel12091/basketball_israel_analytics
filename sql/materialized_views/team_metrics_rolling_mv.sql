-- Draft: rolling team metrics built on team_metrics_by_game_mv.
-- Supports trends tab and per-team momentum deltas without per-request heavy scans.

CREATE MATERIALIZED VIEW basketball_test.team_metrics_rolling_mv
TABLESPACE pg_default
AS
WITH base AS (
  SELECT
    tm.*,
    ROW_NUMBER() OVER (
      PARTITION BY tm.game_year, tm.team_id
      ORDER BY tm.game_date, tm.game_id
    ) AS team_game_seq
  FROM basketball_test.team_metrics_by_game_mv tm
),
roll AS (
  SELECT
    b.*,
    AVG(b.off_ppp) OVER w3 AS off_ppp_r3,
    AVG(b.def_ppp) OVER w3 AS def_ppp_r3,
    AVG(b.net_rtg) OVER w3 AS net_rtg_r3,
    AVG(b.off_ts)  OVER w3 AS off_ts_r3,
    AVG(b.def_ts)  OVER w3 AS def_ts_r3,

    AVG(b.off_ppp) OVER w5 AS off_ppp_r5,
    AVG(b.def_ppp) OVER w5 AS def_ppp_r5,
    AVG(b.net_rtg) OVER w5 AS net_rtg_r5,
    AVG(b.off_ts)  OVER w5 AS off_ts_r5,
    AVG(b.def_ts)  OVER w5 AS def_ts_r5,

    AVG(b.off_ppp) OVER w10 AS off_ppp_r10,
    AVG(b.def_ppp) OVER w10 AS def_ppp_r10,
    AVG(b.net_rtg) OVER w10 AS net_rtg_r10,
    AVG(b.off_ts)  OVER w10 AS off_ts_r10,
    AVG(b.def_ts)  OVER w10 AS def_ts_r10
  FROM base b
  WINDOW
    w3 AS (PARTITION BY b.game_year, b.team_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
    w5 AS (PARTITION BY b.game_year, b.team_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
    w10 AS (PARTITION BY b.game_year, b.team_id ORDER BY b.game_date, b.game_id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
)
SELECT
  r.*,
  LAG(r.net_rtg_r5) OVER (PARTITION BY r.game_year, r.team_id ORDER BY r.game_date, r.game_id) AS prev_net_rtg_r5,
  (r.net_rtg_r5 - LAG(r.net_rtg_r5) OVER (PARTITION BY r.game_year, r.team_id ORDER BY r.game_date, r.game_id)) AS delta_net_rtg_r5
FROM roll r
WITH DATA;

CREATE UNIQUE INDEX team_metrics_rolling_mv_pk
  ON basketball_test.team_metrics_rolling_mv (game_year, game_id, team_id);

CREATE INDEX team_metrics_rolling_mv_gy_team_seq_idx
  ON basketball_test.team_metrics_rolling_mv (game_year, team_id, team_game_seq);

