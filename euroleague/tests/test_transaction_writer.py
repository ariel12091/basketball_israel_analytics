from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd
from pandas.testing import assert_frame_equal


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.transaction_writer import (  # noqa: E402
    DELETE_ORDER,
    INSERT_ORDER,
    REQUIRED_SNAPSHOT_TABLES,
    GameSnapshot,
    NaturalGameKey,
    dry_run_transactions,
    write_game_snapshot,
)


class FakeBackend:
    def __init__(
        self,
        fail_insert_table: str | None = None,
        fail_validation: bool = False,
    ) -> None:
        self.fail_insert_table = fail_insert_table
        self.fail_validation = fail_validation
        self.events: list[tuple[Any, ...]] = []

    def begin(self, key: NaturalGameKey) -> int:
        self.events.append(("begin", key))
        return 123

    def delete_game_rows(self, table: str, game_id: int) -> None:
        self.events.append(("delete", table, game_id))

    def insert_rows(
        self,
        table: str,
        game_id: int,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        self.events.append(("insert", table, game_id, tuple(rows)))
        if table == self.fail_insert_table:
            raise RuntimeError(f"failed to insert {table}")

    def validate_game(self, game_id: int) -> None:
        self.events.append(("validate", game_id))
        if self.fail_validation:
            raise RuntimeError("database validation failed")

    def commit(self) -> None:
        self.events.append(("commit",))

    def rollback(self) -> None:
        self.events.append(("rollback",))


def snapshot_rows() -> dict[str, Sequence[Mapping[str, Any]]]:
    rows: dict[str, Sequence[Mapping[str, Any]]] = {
        table: () for table in REQUIRED_SNAPSHOT_TABLES
    }
    rows["actions"] = ({"source_event_order": 1},)
    rows["game_qa"] = ({"publishable": True},)
    return rows


class TransactionWriterTest(unittest.TestCase):
    def test_complete_snapshot_commits_in_dependency_order(self) -> None:
        backend = FakeBackend()
        key = NaturalGameKey(competition="E", season=2025, gamecode=7)
        game_id = write_game_snapshot(
            backend,
            GameSnapshot(key=key, rows=snapshot_rows()),
        )

        self.assertEqual(game_id, 123)
        self.assertEqual(backend.events[0], ("begin", key))
        self.assertEqual(
            [event[1] for event in backend.events if event[0] == "delete"],
            list(DELETE_ORDER),
        )
        self.assertEqual(
            [event[1] for event in backend.events if event[0] == "insert"],
            ["actions", "game_qa"],
        )
        self.assertEqual(backend.events[-2:], [("validate", 123), ("commit",)])
        self.assertNotIn(("rollback",), backend.events)

    def test_insert_failure_rolls_back_without_commit(self) -> None:
        backend = FakeBackend(fail_insert_table="actions")
        snapshot = GameSnapshot(
            key=NaturalGameKey(competition="E", season=2025, gamecode=8),
            rows=snapshot_rows(),
        )

        with self.assertRaisesRegex(RuntimeError, "failed to insert actions"):
            write_game_snapshot(backend, snapshot)

        self.assertEqual(backend.events[-1], ("rollback",))
        self.assertNotIn(("commit",), backend.events)
        self.assertFalse(any(event[0] == "validate" for event in backend.events))

    def test_database_validation_failure_rolls_back_without_commit(self) -> None:
        backend = FakeBackend(fail_validation=True)
        snapshot = GameSnapshot(
            key=NaturalGameKey(competition="E", season=2025, gamecode=9),
            rows=snapshot_rows(),
        )

        with self.assertRaisesRegex(RuntimeError, "database validation failed"):
            write_game_snapshot(backend, snapshot)

        self.assertEqual(backend.events[-2:], [("validate", 123), ("rollback",)])
        self.assertNotIn(("commit",), backend.events)

    def test_invalid_snapshot_fails_before_opening_transaction(self) -> None:
        backend = FakeBackend()
        incomplete_rows = snapshot_rows()
        del incomplete_rows["actions"]

        with self.assertRaisesRegex(
            ValueError,
            "missing required snapshot tables",
        ):
            write_game_snapshot(
                backend,
                GameSnapshot(
                    key=NaturalGameKey(
                        competition="E",
                        season=2025,
                        gamecode=9,
                    ),
                    rows=incomplete_rows,
                ),
            )

        self.assertEqual(backend.events, [])

    def test_game_qa_requires_exactly_one_row(self) -> None:
        backend = FakeBackend()
        rows = snapshot_rows()
        rows["game_qa"] = ()

        with self.assertRaisesRegex(
            ValueError,
            "game_qa must contain exactly one row",
        ):
            write_game_snapshot(
                backend,
                GameSnapshot(
                    key=NaturalGameKey(
                        competition="E",
                        season=2025,
                        gamecode=10,
                    ),
                    rows=rows,
                ),
            )

        self.assertEqual(backend.events, [])

    def test_dry_run_is_sorted_deterministic_and_skips_unloadable_games(
        self,
    ) -> None:
        plan = pd.DataFrame(
            [
                {
                    "season": 2025,
                    "gamecode": 2,
                    "loadable": False,
                    "full_rosters": 27,
                    "game_qa": 1,
                },
                {
                    "season": 2025,
                    "gamecode": 1,
                    "loadable": True,
                    "full_rosters": 28,
                    "actions": 546,
                    "game_qa": 1,
                },
            ]
        )

        operations = dry_run_transactions(plan)
        assert_frame_equal(operations, dry_run_transactions(plan))

        self.assertEqual(operations["gamecode"].unique().tolist(), [1])
        self.assertEqual(
            operations["operation"].tolist(),
            ["begin"]
            + ["delete_game"] * len(DELETE_ORDER)
            + ["insert", "insert", "insert"]
            + ["validate", "commit"],
        )
        inserted = operations.loc[
            operations["operation"].eq("insert"),
            ["table", "rows"],
        ]
        self.assertEqual(
            list(inserted.itertuples(index=False, name=None)),
            [("full_rosters", 28.0), ("actions", 546.0), ("game_qa", 1.0)],
        )
        self.assertEqual(
            operations["sequence"].tolist(),
            list(range(1, len(operations) + 1)),
        )
        self.assertEqual(
            [table for table in INSERT_ORDER if table in set(inserted["table"])],
            inserted["table"].tolist(),
        )


if __name__ == "__main__":
    unittest.main()
