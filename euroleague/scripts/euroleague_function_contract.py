"""Declared EuroLeague SQL-function reachability contract.

Keep this list explicit. App readers must be greppable and reviewable even when
the R router composes their names dynamically.
"""

from __future__ import annotations


DIRECT_APP_READERS = frozenset(
    {
        "onoff_compute",
        "four_factors_dashboard_compute",
        "get_team_ratings_pergame",
        "get_team_ratings_dynamic",
        "get_team_ratings_direct",
        "get_team_four_factors_pergame",
        "get_team_four_factors_dynamic",
        "get_team_four_factors_direct",
        "get_team_minutes_pergame",
        "get_team_minutes_dynamic",
        "get_team_minutes_direct",
        "get_team_dashboard_dynamic",
        "fetch_lineups_pergame",
        "fetch_lineups_dynamic",
        "fetch_lineups_direct",
        "get_player_traditional_pergame",
        "get_player_traditional_standard_clutch",
        "get_player_traditional_custom_clutch",
    }
)

# Migration 047 targets. They are retained in the contract so the applicator is
# idempotent and the audit reports them explicitly if they ever reappear.
PENDING_REMOVAL_FUNCTIONS = frozenset(
    {
        "get_player_traditional_clutch",
        "select_player_clutch_counts",
        "get_player_traditional_dynamic",
    }
)
PENDING_REMOVAL_VIEWS = frozenset(
    {"player_onoff_by_season", "player_four_factors_by_season"}
)

# This relation has a loader QA consumer and was incorrectly classified as an
# orphan in an earlier audit draft.
PROTECTED_RELATIONS = frozenset({"player_game_context"})


APP_READER_SMOKE = (
    ("onoff_compute", "'E',2025"),
    ("four_factors_dashboard_compute", "'E',2025"),
    # Reached by player_advanced_stats_mv rather than directly by the app.
    ("four_factors_compute", "'E',2025"),
    *((f"{base}_{kind}", "'E',2025")
      for base in ("get_team_ratings", "get_team_four_factors", "get_team_minutes")
      for kind in ("pergame", "dynamic", "direct")),
    ("get_team_dashboard_dynamic", "'E',2025,p_max_margin=>5,p_margin_status=>'all',p_max_time_remaining=>300"),
    *((f"fetch_lineups_{kind}", "'E',2025,p_last_n_games=>2,p_unit_size=>2")
      for kind in ("pergame", "dynamic", "direct")),
    ("get_player_traditional_pergame", "'E',2025"),
    ("get_player_traditional_standard_clutch", "'E',2025"),
    ("get_player_traditional_custom_clutch", "'E',2025"),
)
