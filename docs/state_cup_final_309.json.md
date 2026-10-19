# State Cup Final 2026 (game_id 309)

This is the normalized final-game JSON payload to use when `games_all.json` has not yet published `ExternalID=309`.

Important mapping:
- Raw stats/team endpoint IDs (`team_id_rosters`): `2` (Maccabi Tel-Aviv), `6` (Bnei Herzliya)
- Schedule-format IDs (`team_id_schedule` via `schedule_team_dict`): `1109`, `1118`

```json
{
  "ExternalID": "309",
  "game_type": 35,
  "team1": 1109,
  "team2": 1118,
  "team_name_1": "מכבי ת\"א",
  "team_name_2": "בני הרצליה",
  "team_name_eng_1": "Maccabi Tel-Aviv",
  "team_name_eng_2": "Bnei Herzliya",
  "game_date_txt": "19/02/2026",
  "game_year": 2026,
  "score_team1": 109,
  "score_team2": 90,
  "pbp_link": "https://stats.segevstats.com/realtimestat_heb/gameStats.php?game_id=309&lang=he"
}
```

