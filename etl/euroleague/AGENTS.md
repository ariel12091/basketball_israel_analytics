# EuroLeague ETL instructions

This directory inherits the repository-root `AGENTS.md`. It is also part of the
EuroLeague shadow sub-project: read and follow `../../euroleague/AGENTS.md` and
`../../euroleague/PROJECT.md` before changing these transformations or fixtures.

In particular, keep the R transformations deterministic and free of database
I/O, do not add game-specific exceptions, and add a labelled fixture for every
new grouping or possession rule.
