# Local data area

Downloaded EuroLeague datasets are intentionally not committed.

- `raw/`: immutable provider responses or per-game extracts
- `staging/`: normalized temporary files
- `exports/`: disposable audit outputs

Each persisted load should record competition, season, gamecode, retrieval
timestamp, package version, source endpoint, and a content checksum. The
100-game exploratory CSV used on 2026-08-05 is stored outside the repository at
`C:\tmp\euroleague_pbp_2025_100games.csv`.
