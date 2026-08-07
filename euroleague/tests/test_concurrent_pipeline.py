from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.batch_pipeline import (  # noqa: E402
    STAGE_FORMAT_VERSION,
    StageRecord,
    publish_staged_games,
    stage_games,
)
from euroleague_possessions.boxscore_collector import (  # noqa: E402
    GameKey,
    collect_boxscores,
)
from euroleague_possessions.concurrency import (  # noqa: E402
    AdaptiveRequestGate,
    run_bounded,
)
from euroleague_possessions.pbp_collector import (  # noqa: E402
    collect_play_by_play,
    read_cached_pbp,
)
from euroleague_possessions.staging import (  # noqa: E402
    GameBootstrap,
    StagedGame,
)
from euroleague_possessions.transaction_writer import (  # noqa: E402
    REQUIRED_SNAPSHOT_TABLES,
    GameSnapshot,
    NaturalGameKey,
)


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.headers: dict[str, str] = {}
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


class _ConcurrencyProbe:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active = 0
        self.maximum = 0
        self.calls: list[int] = []

    def enter(self, gamecode: int) -> None:
        with self._lock:
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            self.calls.append(gamecode)

    def leave(self) -> None:
        with self._lock:
            self.active -= 1


def _minimal_staged_game(
    competition: str,
    season: int,
    gamecode: int,
) -> StagedGame:
    key = NaturalGameKey(competition, season, gamecode)
    rows = {table: () for table in REQUIRED_SNAPSHOT_TABLES}
    rows["game_qa"] = ({"publication_status": "clear"},)
    snapshot = GameSnapshot(key=key, rows=rows)
    snapshot.validate()
    return StagedGame(
        bootstrap=GameBootstrap(
            key=key,
            package_version="0.1.1",
            collector_version="test",
            teams=(),
            players=(),
            schedule={},
            source_artifacts=(),
        ),
        snapshot=snapshot,
    )


class ConcurrentPipelineTest(unittest.TestCase):
    def test_request_gate_coordinates_spacing_and_global_cooldown(self) -> None:
        clock = _FakeClock()
        gate = AdaptiveRequestGate(
            2.0,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

        gate.wait()
        gate.wait()
        gate.penalize(5.0)
        gate.wait()

        self.assertEqual(clock.sleeps, [2.0, 5.0])
        self.assertEqual(clock.now, 7.0)

    def test_bounded_runner_is_concurrent_but_returns_input_order(self) -> None:
        probe = _ConcurrencyProbe()

        def worker(value: int) -> int:
            probe.enter(value)
            try:
                time.sleep((4 - value) * 0.01)
                return value * 10
            finally:
                probe.leave()

        outcomes = run_bounded([1, 2, 3], worker, max_workers=2)

        self.assertEqual([outcome.item for outcome in outcomes], [1, 2, 3])
        self.assertEqual([outcome.result for outcome in outcomes], [10, 20, 30])
        self.assertEqual(probe.maximum, 2)

    def test_bounded_runner_isolates_one_failure(self) -> None:
        def worker(value: int) -> int:
            if value == 2:
                raise ValueError("bad game")
            return value

        outcomes = run_bounded([1, 2, 3], worker, max_workers=2)

        self.assertIsNone(outcomes[0].error)
        self.assertIsInstance(outcomes[1].error, ValueError)
        self.assertIsNone(outcomes[2].error)

    def test_boxscore_collector_runs_concurrently_and_reuses_checkpoints(self) -> None:
        probe = _ConcurrencyProbe()

        def request_get(
            _url: str,
            *,
            params: dict[str, object],
            timeout: float,
        ) -> _FakeResponse:
            del timeout
            gamecode = int(params["gamecode"])
            probe.enter(gamecode)
            try:
                time.sleep(0.02)
                return _FakeResponse({"Stats": [{"gamecode": gamecode}]})
            finally:
                probe.leave()

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            records = collect_boxscores(
                [GameKey(2025, 2), GameKey(2025, 1), GameKey(2025, 2)],
                output_dir,
                throttle_seconds=0,
                max_workers=2,
                request_get=request_get,
            )

            self.assertEqual([record.gamecode for record in records], [1, 2])
            self.assertTrue(all(record.status == "fetched" for record in records))
            self.assertEqual(probe.maximum, 2)
            manifest = json.loads(
                (output_dir / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                [record["gamecode"] for record in manifest["records"]],
                [1, 2],
            )

            cached = collect_boxscores(
                [GameKey(2025, 2), GameKey(2025, 1)],
                output_dir,
                throttle_seconds=0,
                max_workers=2,
                request_get=lambda *_args, **_kwargs: self.fail(
                    "valid checkpoints must avoid network calls"
                ),
            )
            self.assertTrue(all(record.status == "cached" for record in cached))

    def test_package_pbp_collector_is_concurrent_ordered_and_restartable(self) -> None:
        probe = _ConcurrencyProbe()

        class FakePackage:
            def __init__(self, competition: str) -> None:
                self.competition = competition

            def get_game_play_by_play_data(
                self,
                season: int,
                gamecode: int,
                include_ishometeam: bool,
            ) -> pd.DataFrame:
                self.assert_inputs(season, include_ishometeam)
                probe.enter(gamecode)
                try:
                    time.sleep(0.02)
                    return pd.DataFrame(
                        {
                            "Season": [season],
                            "Gamecode": [gamecode],
                            "PERIOD": [1],
                            "TRUE_NUMBEROFPLAY": [0],
                            "PLAYTYPE": ["BP"],
                            "CODETEAM": ["AAA"],
                            "IsHomeTeam": [True],
                        }
                    )
                finally:
                    probe.leave()

            def assert_inputs(
                self,
                season: int,
                include_ishometeam: bool,
            ) -> None:
                if self.competition != "E" or season != 2025:
                    raise AssertionError("unexpected package inputs")
                if not include_ishometeam:
                    raise AssertionError("home-team context must be retained")

        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            records = collect_play_by_play(
                [GameKey(2025, 3), GameKey(2025, 1), GameKey(2025, 2)],
                output_dir,
                throttle_seconds=0,
                max_workers=2,
                package_factory=FakePackage,
            )

            self.assertEqual([record.gamecode for record in records], [1, 2, 3])
            self.assertEqual(probe.maximum, 2)
            game_two = read_cached_pbp(Path(records[1].path or ""))
            self.assertEqual(int(game_two.iloc[0]["Gamecode"]), 2)

            cached = collect_play_by_play(
                [GameKey(2025, 3), GameKey(2025, 1), GameKey(2025, 2)],
                output_dir,
                throttle_seconds=0,
                max_workers=3,
                package_factory=lambda **_kwargs: self.fail(
                    "valid checkpoints must avoid package calls"
                ),
            )
            self.assertTrue(all(record.status == "cached" for record in cached))

    def test_staging_checkpoints_are_ordered_and_resume_without_recompute(self) -> None:
        pbp = pd.DataFrame(
            {
                "Season": [2025, 2025, 2025],
                "Gamecode": [3, 1, 2],
                "TRUE_NUMBEROFPLAY": [0, 0, 0],
            }
        )
        probe = _ConcurrencyProbe()

        def fake_build(
            _game: pd.DataFrame,
            _boxscore_dir: Path,
            season: int,
            gamecode: int,
            competition: str,
            schedule_meta: dict | None = None,
        ) -> StagedGame:
            probe.enter(gamecode)
            try:
                time.sleep(0.02)
                return _minimal_staged_game(competition, season, gamecode)
            finally:
                probe.leave()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            boxscores = root / "boxscores"
            checkpoints = root / "checkpoints"
            boxscores.mkdir()
            for gamecode in (1, 2, 3):
                (boxscores / f"E2025_{gamecode}.json").write_text(
                    "{}", encoding="utf-8"
                )

            with patch(
                "euroleague_possessions.batch_pipeline.build_staged_game",
                side_effect=fake_build,
            ):
                first = stage_games(
                    pbp,
                    boxscores,
                    checkpoints,
                    max_workers=2,
                )

            self.assertEqual([record.key.gamecode for record in first], [1, 2, 3])
            self.assertTrue(all(record.status == "staged" for record in first))
            self.assertEqual(probe.maximum, 2)
            manifest = json.loads(
                (checkpoints / "stage_manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                [record["gamecode"] for record in manifest["records"]],
                [1, 2, 3],
            )

            with patch(
                "euroleague_possessions.batch_pipeline.build_staged_game",
                side_effect=AssertionError("checkpoint was not reused"),
            ):
                second = stage_games(
                    pbp,
                    boxscores,
                    checkpoints,
                    max_workers=3,
                )
            self.assertTrue(all(record.status == "cached" for record in second))

            stale_path = checkpoints / "E2025_1.stage.json"
            stale = json.loads(stale_path.read_text(encoding="utf-8"))
            stale["format_version"] = STAGE_FORMAT_VERSION - 1
            stale_path.write_text(json.dumps(stale), encoding="utf-8")
            with patch(
                "euroleague_possessions.batch_pipeline.build_staged_game",
                side_effect=fake_build,
            ):
                rebuilt = stage_games(
                    pbp,
                    boxscores,
                    checkpoints,
                    keys=[GameKey(2025, 1)],
                )
            self.assertEqual(rebuilt[0].status, "staged")
            refreshed = json.loads(stale_path.read_text(encoding="utf-8"))
            self.assertEqual(refreshed["format_version"], STAGE_FORMAT_VERSION)

    def test_batch_publication_shares_one_run_and_stays_sequential(self) -> None:
        records = [
            StageRecord(
                key=GameKey(2025, gamecode),
                status="staged",
                elapsed_seconds=0.1,
                checkpoint_path=f"game-{gamecode}.json",
                counts={},
                error=None,
                staged=_minimal_staged_game("E", 2025, gamecode),
            )
            for gamecode in (2, 1)
        ]
        connection = MagicMock()

        with (
            patch(
                "euroleague_possessions.batch_pipeline.connect_from_env_file",
                return_value=connection,
            ),
            patch(
                "euroleague_possessions.batch_pipeline.inspect_target",
                return_value={"server_port": 5432, "euroleague_schema": "euroleague"},
            ),
            patch(
                "euroleague_possessions.batch_pipeline.assert_shadow_schema_compatible"
            ),
            patch(
                "euroleague_possessions.batch_pipeline.start_load_run",
                return_value=71,
            ) as start_run,
            patch(
                "euroleague_possessions.batch_pipeline.bootstrap_game"
            ) as bootstrap,
            patch(
                "euroleague_possessions.batch_pipeline.PostgresTransactionBackend",
                side_effect=(MagicMock(name="backend1"), MagicMock(name="backend2")),
            ),
            patch(
                "euroleague_possessions.batch_pipeline.write_game_snapshot",
                side_effect=(101, 102),
            ) as write_snapshot,
            patch(
                "euroleague_possessions.batch_pipeline.finish_load_run"
            ) as finish_run,
        ):
            published = publish_staged_games(
                records,
                env_file=Path("unused.env"),
            )

        self.assertEqual([item.key.gamecode for item in published], [1, 2])
        self.assertEqual([item.game_id for item in published], [101, 102])
        start_run.assert_called_once()
        self.assertEqual(start_run.call_args.kwargs["requested_games"], 2)
        self.assertEqual(
            start_run.call_args.kwargs["request_parameters"]["gamecodes"],
            [1, 2],
        )
        self.assertEqual(
            bootstrap.call_args_list,
            [
                call(connection, records[1].staged.bootstrap, load_run_id=71),
                call(connection, records[0].staged.bootstrap, load_run_id=71),
            ],
        )
        self.assertEqual(write_snapshot.call_count, 2)
        finish_run.assert_called_once_with(
            connection,
            load_run_id=71,
            success=True,
            successful_games=2,
            failed_games=0,
            errors=[],
        )
        connection.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
