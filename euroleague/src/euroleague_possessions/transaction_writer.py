"""Driver-independent transactional orchestration for EuroLeague game loads."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

import pandas as pd


# Child/dependent tables are cleared before their parents. Source artifacts are
# immutable and schedule/dimension rows are upserted outside snapshot deletion.
DELETE_ORDER = (
    "reconciliation_metrics",
    "game_qa",
    "qa_incidents",
    "actions",
    "actions_raw",
    "team_boxscores",
    "full_rosters",
)

INSERT_ORDER = (
    "full_rosters",
    "team_boxscores",
    "actions_raw",
    "actions",
    "reconciliation_metrics",
    "game_qa",
    "qa_incidents",
)

REQUIRED_SNAPSHOT_TABLES = frozenset(
    {
        "full_rosters",
        "team_boxscores",
        "actions_raw",
        "actions",
        "reconciliation_metrics",
        "game_qa",
    }
)


@dataclass(frozen=True)
class NaturalGameKey:
    competition: str
    season: int
    gamecode: int


@dataclass(frozen=True)
class GameSnapshot:
    key: NaturalGameKey
    rows: Mapping[str, Sequence[Mapping[str, Any]]]

    def validate(self) -> None:
        unknown = set(self.rows).difference(INSERT_ORDER)
        if unknown:
            raise ValueError(f"unknown snapshot tables: {sorted(unknown)}")
        missing = REQUIRED_SNAPSHOT_TABLES.difference(self.rows)
        if missing:
            raise ValueError(f"missing required snapshot tables: {sorted(missing)}")
        if len(self.rows["game_qa"]) != 1:
            raise ValueError("game_qa must contain exactly one row")


class TransactionBackend(Protocol):
    """Interface implemented later by the PostgreSQL adapter."""

    def begin(self, key: NaturalGameKey) -> int:
        """Begin a transaction and resolve/upsert schedule; return game_id."""

    def delete_game_rows(self, table: str, game_id: int) -> None:
        """Delete one game's replaceable rows from a table."""

    def insert_rows(
        self,
        table: str,
        game_id: int,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        """Insert a staged table batch, resolving natural foreign keys."""

    def validate_game(self, game_id: int) -> None:
        """Run final database-side constraints/release checks."""

    def commit(self) -> None:
        """Commit the current game transaction."""

    def rollback(self) -> None:
        """Roll back the current game transaction."""


def write_game_snapshot(
    backend: TransactionBackend,
    snapshot: GameSnapshot,
) -> int:
    """Replace one game atomically, rolling back on any failure."""

    snapshot.validate()
    transaction_open = False
    try:
        game_id = backend.begin(snapshot.key)
        transaction_open = True
        for table in DELETE_ORDER:
            backend.delete_game_rows(table, game_id)
        for table in INSERT_ORDER:
            rows = snapshot.rows.get(table, ())
            if rows:
                backend.insert_rows(table, game_id, rows)
        backend.validate_game(game_id)
        backend.commit()
        transaction_open = False
        return game_id
    except Exception:
        if transaction_open:
            backend.rollback()
        raise


def dry_run_transactions(
    load_plan_games: pd.DataFrame,
    competition: str = "E",
) -> pd.DataFrame:
    """Expand load-plan counts into deterministic transaction operations."""

    missing_columns = {"season", "gamecode", "loadable"}.difference(
        load_plan_games.columns
    )
    if missing_columns:
        raise ValueError(
            f"load plan missing columns: {sorted(missing_columns)}"
        )

    operations: list[dict[str, Any]] = []
    for game in load_plan_games.sort_values(
        ["season", "gamecode"], kind="stable"
    ).itertuples(index=False):
        if not bool(game.loadable):
            continue
        base = {
            "competition": competition,
            "season": int(game.season),
            "gamecode": int(game.gamecode),
        }
        sequence = 1
        operations.append(
            {**base, "sequence": sequence, "operation": "begin", "table": "schedule", "rows": 1}
        )
        sequence += 1
        for table in DELETE_ORDER:
            operations.append(
                {**base, "sequence": sequence, "operation": "delete_game", "table": table, "rows": None}
            )
            sequence += 1
        for table in INSERT_ORDER:
            rows = int(getattr(game, table, 0))
            if rows:
                operations.append(
                    {**base, "sequence": sequence, "operation": "insert", "table": table, "rows": rows}
                )
                sequence += 1
        operations.append(
            {**base, "sequence": sequence, "operation": "validate", "table": "game_qa", "rows": 1}
        )
        sequence += 1
        operations.append(
            {**base, "sequence": sequence, "operation": "commit", "table": "", "rows": None}
        )
    return pd.DataFrame(operations)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a database-free transaction manifest from a load plan."
    )
    parser.add_argument("load_plan_games_csv", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--competition", default="E")
    args = parser.parse_args()

    operations = dry_run_transactions(
        pd.read_csv(args.load_plan_games_csv),
        competition=args.competition,
    )
    print(f"games={operations[['season', 'gamecode']].drop_duplicates().shape[0]}")
    print(f"operations={len(operations)}")
    print(f"commits={int(operations['operation'].eq('commit').sum())}")
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        operations.to_csv(args.output, index=False)


if __name__ == "__main__":
    main()
