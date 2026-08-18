"""Deterministic EuroLeague event relationship and endpoint parser.

The parser intentionally mirrors the validated R reference rules while making
the per-event decision state and transition trace explicit. It performs no
network or database I/O.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from typing import Any, Optional

import pandas as pd

from .models import (
    EndpointReason,
    Event,
    EventDecision,
    GroupingStatus,
    PendingPenalty,
)


MADE_SHOTS = frozenset({"2FGM", "3FGM"})
MISSED_SHOTS = frozenset({"2FGA", "3FGA"})
SHOTS = MADE_SHOTS | MISSED_SHOTS
REBOUNDS = frozenset({"D", "O"})
COMMITTED_FOULS = frozenset({"CM", "CMT", "CMU", "B", "C", "CMTI", "OF"})
SPECIAL_FOULS = frozenset({"CMT", "CMU", "B", "C", "CMTI"})
FT_TYPES = frozenset({"FTA", "FTM"})
ADMINISTRATIVE = frozenset(
    {"IN", "OUT", "CCH", "TOUT", "TOUT_TV", "TIMEOUT", "TV", "BP"}
)
PERIOD_END = frozenset({"EP", "EG"})
LIVE_BOUNDARIES = SHOTS | REBOUNDS | frozenset({"TO", "OF"})


COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "season": ("season", "Season"),
    "gamecode": ("gamecode", "Gamecode"),
    "source_event_order": ("source_event_order", "TRUE_NUMBEROFPLAY"),
    "provider_number_of_play": ("provider_number_of_play", "NUMBEROFPLAY"),
    "period": ("period", "PERIOD"),
    "clock": ("clock", "MARKERTIME"),
    "team_code": ("team_code", "CODETEAM"),
    "play_type": ("play_type", "PLAYTYPE"),
    "player_id": ("player_id", "PLAYER_ID"),
    "player_name": ("player_name", "PLAYER"),
    "play_info": ("play_info", "PLAYINFO"),
    "score_a": ("score_a", "POINTS_A"),
    "score_b": ("score_b", "POINTS_B"),
}

REQUIRED_COLUMNS = frozenset(
    {"season", "gamecode", "source_event_order", "period", "play_type"}
)


def _source_column(frame: pd.DataFrame, normalized_name: str) -> Optional[str]:
    return next(
        (
            candidate
            for candidate in COLUMN_CANDIDATES[normalized_name]
            if candidate in frame.columns
        ),
        None,
    )


def _clean_optional_string(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    cleaned = str(value).strip()
    return None if cleaned in {"", "NA", "nan", "None"} else cleaned


def _same(left: Any, right: Any) -> bool:
    left_missing = left is None or pd.isna(left)
    right_missing = right is None or pd.isna(right)
    if left_missing or right_missing:
        return left_missing and right_missing
    return bool(left == right)


def normalize_events(events: pd.DataFrame) -> pd.DataFrame:
    """Return package-shaped events with stable normalized columns added."""

    if not isinstance(events, pd.DataFrame):
        raise TypeError("events must be a pandas DataFrame")

    result = events.copy(deep=True)
    for normalized_name in COLUMN_CANDIDATES:
        source = _source_column(events, normalized_name)
        if source is None:
            if normalized_name in REQUIRED_COLUMNS:
                raise ValueError(
                    f"Missing required EuroLeague column: "
                    f"{COLUMN_CANDIDATES[normalized_name][0]}"
                )
            result[normalized_name] = pd.NA
        else:
            result[normalized_name] = events[source]

    for column in (
        "season",
        "gamecode",
        "source_event_order",
        "provider_number_of_play",
        "period",
    ):
        result[column] = pd.to_numeric(result[column], errors="coerce").astype("Int64")

    for required in ("season", "gamecode", "source_event_order", "period"):
        if result[required].isna().any():
            bad_rows = result.index[result[required].isna()].tolist()[:5]
            raise ValueError(f"{required} is missing or invalid at rows {bad_rows}")

    result["play_type"] = result["play_type"].map(_clean_optional_string)
    if result["play_type"].isna().any():
        bad_rows = result.index[result["play_type"].isna()].tolist()[:5]
        raise ValueError(f"play_type is missing at rows {bad_rows}")
    result["play_type"] = result["play_type"].str.upper()

    for column in ("clock", "team_code", "player_id"):
        result[column] = result[column].map(_clean_optional_string)
    for column in ("player_name", "play_info"):
        result[column] = result[column].map(
            lambda value: None if value is None or pd.isna(value) else str(value)
        )
    for column in ("score_a", "score_b"):
        result[column] = pd.to_numeric(result[column], errors="coerce")

    identity = ["season", "gamecode", "period", "source_event_order"]
    duplicate = result.duplicated(identity, keep=False)
    if duplicate.any():
        examples = result.loc[duplicate, identity].head(5).to_dict("records")
        raise ValueError(f"Duplicate EuroLeague source event identity: {examples}")

    return result


class PeriodParser:
    """Parse one already ordered period into auditable event decisions."""

    def __init__(self, period_events: pd.DataFrame):
        ordered = period_events.sort_values("source_event_order", kind="stable")
        self.frame = ordered.reset_index(drop=True)
        self.events = [
            Event(
                season=int(row.season),
                gamecode=int(row.gamecode),
                source_order=int(row.source_event_order),
                period=int(row.period),
                clock=_clean_optional_string(row.clock),
                team=_clean_optional_string(row.team_code),
                play_type=str(row.play_type),
                player_id=_clean_optional_string(row.player_id),
            )
            for row in self.frame.itertuples(index=False)
        ]
        self.decisions = [
            EventDecision(parent_order=event.source_order) for event in self.events
        ]
        self.game_teams = list(
            dict.fromkeys(event.team for event in self.events if event.team is not None)
        )

    def parse(self) -> pd.DataFrame:
        self._attach_shot_children()
        self._bundle_unsportsmanlike_fouls()
        self._bundle_offensive_fouls()
        self._bundle_turnovers_and_steals()
        self._pair_committed_and_drawn_fouls()
        offsetting = self._find_offsetting_technicals()
        self._assign_free_throws(offsetting)
        self._partition_free_throw_trips()
        self._attach_rebounds_after_missed_final_fts()
        self._resolve_endpoints()
        self._mark_compound_penalties_provisional()
        return self._result_frame()

    def _after(self, index: int) -> range:
        return range(index + 1, len(self.events))

    @staticmethod
    def _before(index: int) -> range:
        return range(index - 1, -1, -1)

    def _indices_with_parent(self, parent_order: int) -> list[int]:
        return [
            index
            for index, decision in enumerate(self.decisions)
            if decision.parent_order == parent_order
        ]

    def _attach(self, index: int, parent_order: int, rule: str) -> None:
        self.decisions[index].attach(parent_order, rule)

    def _other_team(self, team: Optional[str]) -> Optional[str]:
        candidates = [candidate for candidate in self.game_teams if candidate != team]
        return candidates[0] if len(candidates) == 1 else None

    def _attach_shot_children(self) -> None:
        for index, event in enumerate(self.events):
            if event.play_type not in SHOTS:
                continue
            root = event.source_order

            if event.play_type in MADE_SHOTS:
                for child_index in self._after(index):
                    child = self.events[child_index]
                    if child.play_type == "AS" and _same(child.team, event.team):
                        self._attach(child_index, root, "shot.assist")
                        continue
                    if child.play_type in ADMINISTRATIVE or child.play_type == "AS":
                        continue
                    if child.play_type in {"CM", "RV"} | FT_TYPES:
                        continue
                    break

                window: list[int] = []
                for child_index in self._after(index):
                    child_type = self.events[child_index].play_type
                    if child_type in LIVE_BOUNDARIES | FT_TYPES | PERIOD_END:
                        break
                    window.append(child_index)

                exact_rv = [
                    child_index
                    for child_index in window
                    if self.events[child_index].play_type == "RV"
                    and self.events[child_index].team is not None
                    and self.events[child_index].team == event.team
                    and self.events[child_index].player_id is not None
                    and self.events[child_index].player_id == event.player_id
                ]
                same_team_rv = [
                    child_index
                    for child_index in window
                    if self.events[child_index].play_type == "RV"
                    and self.events[child_index].team is not None
                    and self.events[child_index].team == event.team
                ]
                committed = [
                    child_index
                    for child_index in window
                    if self.events[child_index].play_type == "CM"
                    and self.events[child_index].team is not None
                    and self.events[child_index].team != event.team
                ]

                if exact_rv and committed:
                    self._attach(committed[0], root, "shot.and_one_foul")
                    self._attach(exact_rv[0], root, "shot.and_one_drawn")
                elif same_team_rv and committed:
                    fouled_index = same_team_rv[0]
                    committed_index = committed[0]
                    ft_match = False
                    for later_index in self._after(max(fouled_index, committed_index)):
                        later = self.events[later_index]
                        if later.play_type in LIVE_BOUNDARIES | PERIOD_END:
                            break
                        if (
                            later.play_type in FT_TYPES
                            and _same(later.team, event.team)
                            and _same(later.player_id, self.events[fouled_index].player_id)
                        ):
                            ft_match = True
                            break
                    if ft_match:
                        self._attach(
                            committed_index, root, "shot.dead_ball_foul"
                        )
                        self._attach(
                            fouled_index, root, "shot.dead_ball_foul_drawn"
                        )
            else:
                for child_index in self._after(index):
                    child = self.events[child_index]
                    if child.play_type in {"AG", "FV"}:
                        self._attach(child_index, root, "shot.block_annotation")
                        continue
                    if child.play_type in ADMINISTRATIVE:
                        continue
                    if child.play_type in REBOUNDS:
                        self._attach(child_index, root, "shot.rebound")
                    break

    def _bundle_unsportsmanlike_fouls(self) -> None:
        for index, event in enumerate(self.events):
            if event.play_type != "CMU":
                continue
            root = event.source_order
            for child_index in self._after(index):
                child = self.events[child_index]
                if child.play_type == "TO" and _same(child.team, event.team):
                    self._attach(child_index, root, "unsportsmanlike.turnover")
                    continue
                if child.play_type == "RV" and not _same(child.team, event.team):
                    self._attach(child_index, root, "unsportsmanlike.drawn")
                    continue
                if child.play_type in ADMINISTRATIVE:
                    continue
                break

    def _bundle_offensive_fouls(self) -> None:
        for index, event in enumerate(self.events):
            if event.play_type != "OF":
                continue
            root = event.source_order
            for child_index in self._after(index):
                child = self.events[child_index]
                if child.play_type == "TO" and _same(child.team, event.team):
                    self._attach(child_index, root, "offensive_foul.turnover")
                    continue
                if child.play_type in {"RV", "ST"} and not _same(
                    child.team, event.team
                ):
                    self._attach(child_index, root, "offensive_foul.opponent")
                    continue
                if child.play_type in ADMINISTRATIVE:
                    continue
                break

    def _bundle_turnovers_and_steals(self) -> None:
        for index, event in enumerate(self.events):
            if event.play_type != "TO":
                continue
            if self.decisions[index].parent_order != event.source_order:
                continue
            for child_index in self._after(index):
                child = self.events[child_index]
                if child.play_type == "ST" and not _same(child.team, event.team):
                    self._attach(child_index, event.source_order, "turnover.steal")
                if child.play_type not in ADMINISTRATIVE:
                    break

    def _pair_committed_and_drawn_fouls(self) -> None:
        for index, event in enumerate(self.events):
            if event.play_type not in COMMITTED_FOULS - {"OF"}:
                continue
            if self.decisions[index].parent_order != event.source_order:
                continue
            paired = False
            for child_index in self._after(index):
                child = self.events[child_index]
                if (
                    child.play_type == "RV"
                    and self.decisions[child_index].parent_order == child.source_order
                    and not _same(child.team, event.team)
                ):
                    self._attach(child_index, event.source_order, "foul.drawn_forward")
                    paired = True
                    break
                if child.play_type in ADMINISTRATIVE:
                    continue
                if child.play_type in (
                    COMMITTED_FOULS | SHOTS | REBOUNDS | {"TO"} | FT_TYPES | PERIOD_END
                ):
                    break

            if paired:
                continue
            for child_index in self._before(index):
                child = self.events[child_index]
                if child.play_type in LIVE_BOUNDARIES:
                    break
                clock_compatible = (
                    event.clock is None
                    or child.clock is None
                    or _same(event.clock, child.clock)
                )
                if (
                    child.play_type == "RV"
                    and self.decisions[child_index].parent_order == child.source_order
                    and not _same(child.team, event.team)
                    and clock_compatible
                ):
                    self._attach(child_index, event.source_order, "foul.drawn_backward")
                    break

    def _find_offsetting_technicals(self) -> set[int]:
        by_clock: dict[str, list[int]] = defaultdict(list)
        for index, event in enumerate(self.events):
            if event.play_type == "CMT":
                key = event.clock or f"order_{event.source_order}"
                by_clock[key].append(index)

        offsetting: set[int] = set()
        for indices in by_clock.values():
            teams = {self.events[index].team for index in indices if self.events[index].team}
            if len(indices) == 2 and len(teams) == 2:
                offsetting.update(indices)
                for index in indices:
                    self.decisions[index].trace.append("technical.offsetting")
        return offsetting

    def _pending_penalty(
        self, candidate_index: int, before_index: int
    ) -> PendingPenalty:
        candidate = self.events[candidate_index]
        root = self.decisions[candidate_index].parent_order
        group_indices = self._indices_with_parent(root)
        rv_indices = [
            index for index in group_indices if self.events[index].play_type == "RV"
        ]
        benefit_team = (
            self.events[rv_indices[0]].team
            if rv_indices
            else self._other_team(candidate.team)
        )
        benefit_player = self.events[rv_indices[0]].player_id if rv_indices else None
        prior_fts = [
            index
            for index in group_indices
            if index < before_index and self.events[index].play_type in FT_TYPES
        ]
        return PendingPenalty(
            event_index=candidate_index,
            parent_order=root,
            benefit_team=benefit_team,
            benefit_player=benefit_player,
            foul_type=candidate.play_type,
            last_ft_index=max(prior_fts) if prior_fts else None,
        )

    def _assign_free_throws(self, offsetting_technicals: set[int]) -> None:
        ft_indices = [
            index for index, event in enumerate(self.events) if event.play_type in FT_TYPES
        ]
        for ft_index in ft_indices:
            ft_event = self.events[ft_index]
            previous_boundaries = [
                index
                for index in range(ft_index)
                if self.events[index].play_type in SHOTS | {"TO", "OF"}
                and not (
                    self.events[index].play_type == "TO"
                    and self.decisions[index].parent_order
                    != self.events[index].source_order
                )
            ]
            lower = max(previous_boundaries) if previous_boundaries else -1
            candidate_indices = [
                index
                for index in range(lower + 1, ft_index)
                if self.events[index].play_type in COMMITTED_FOULS - {"OF"}
            ]

            best_penalty: Optional[PendingPenalty] = None
            best_score = float("-inf")
            best_exact_player = False
            for candidate_index in candidate_indices:
                if candidate_index in offsetting_technicals:
                    continue
                penalty = self._pending_penalty(candidate_index, ft_index)
                if penalty.benefit_team is None or ft_event.team is None:
                    continue
                if penalty.benefit_team != ft_event.team:
                    continue

                if penalty.last_ft_index is not None:
                    last_ft = penalty.last_ft_index
                    if not _same(self.events[last_ft].player_id, ft_event.player_id):
                        continue
                    if not _same(self.events[last_ft].team, ft_event.team):
                        continue
                    intervening_foul = any(
                        self.events[index].play_type in COMMITTED_FOULS
                        and self.decisions[index].parent_order != penalty.parent_order
                        for index in range(last_ft + 1, ft_index)
                    )
                    if intervening_foul:
                        continue

                exact_player = (
                    penalty.benefit_player is not None
                    and ft_event.player_id is not None
                    and penalty.benefit_player == ft_event.player_id
                )
                score = 1000 - (ft_index - candidate_index)
                if exact_player:
                    score += 500
                if penalty.foul_type in SPECIAL_FOULS:
                    score += 600
                if (
                    ft_event.clock is not None
                    and self.events[candidate_index].clock is not None
                    and ft_event.clock == self.events[candidate_index].clock
                ):
                    score += 10

                if score > best_score:
                    best_score = score
                    best_penalty = penalty
                    best_exact_player = exact_player

            if best_penalty is not None:
                self._attach(
                    ft_index,
                    best_penalty.parent_order,
                    "free_throw.best_pending_penalty",
                )
                self.decisions[ft_index].confidence_pct = (
                    99 if best_exact_player else 95
                )
                continue

            prior_fts = [index for index in ft_indices if index < ft_index]
            prior_ft = max(prior_fts) if prior_fts else None
            crossed_live = True
            if prior_ft is not None:
                crossed_live = any(
                    self.events[index].play_type in LIVE_BOUNDARIES
                    for index in range(prior_ft + 1, ft_index)
                )
            can_continue = (
                prior_ft is not None
                and not crossed_live
                and _same(self.events[prior_ft].team, ft_event.team)
                and _same(self.events[prior_ft].player_id, ft_event.player_id)
            )
            if can_continue and prior_ft is not None:
                self._attach(
                    ft_index,
                    self.decisions[prior_ft].parent_order,
                    "free_throw.continue_prior_trip",
                )
            else:
                self.decisions[ft_index].mark_unresolved(
                    "free_throw.no_compatible_parent"
                )

    def _partition_free_throw_trips(self) -> None:
        trip_number = 0
        previous_ft: Optional[int] = None
        for index, event in enumerate(self.events):
            if event.play_type not in FT_TYPES:
                continue
            same_trip = False
            if previous_ft is not None:
                between = range(previous_ft + 1, index)
                same_trip = (
                    self.decisions[index].parent_order
                    == self.decisions[previous_ft].parent_order
                    and _same(event.team, self.events[previous_ft].team)
                    and _same(event.player_id, self.events[previous_ft].player_id)
                    and not any(
                        self.events[between_index].play_type in LIVE_BOUNDARIES
                        for between_index in between
                    )
                )
            if not same_trip:
                trip_number += 1
            self.decisions[index].ft_trip_id = (
                f"EL:{event.season}:{event.gamecode}:{event.period}:"
                f"{self.decisions[index].parent_order}:{trip_number}"
            )
            self.decisions[index].trace.append("free_throw.partition_trip")
            previous_ft = index

    def _attach_rebounds_after_missed_final_fts(self) -> None:
        trips = list(
            dict.fromkeys(
                decision.ft_trip_id
                for decision in self.decisions
                if decision.ft_trip_id is not None
            )
        )
        for trip in trips:
            indices = [
                index
                for index, decision in enumerate(self.decisions)
                if decision.ft_trip_id == trip
            ]
            final = max(indices)
            if self.events[final].play_type != "FTA":
                continue
            for later_index in self._after(final):
                later = self.events[later_index]
                if later.play_type in ADMINISTRATIVE | {"CM", "RV"}:
                    continue
                if later.play_type in REBOUNDS:
                    if (
                        self.decisions[later_index].parent_order
                        == later.source_order
                    ):
                        self._attach(
                            later_index,
                            self.decisions[final].parent_order,
                            "free_throw.missed_final_rebound",
                        )
                    continue
                break

    def _set_endpoint(
        self, index: int, reason: EndpointReason, rule: str
    ) -> None:
        root = self.decisions[index].parent_order
        already_closed = any(
            decision.parent_order == root and decision.final_end_poss
            for decision in self.decisions
        )
        if not already_closed:
            self.decisions[index].mark_endpoint(reason, rule)

    def _resolve_endpoints(self) -> None:
        for index, event in enumerate(self.events):
            if event.play_type == "TO":
                self._set_endpoint(index, EndpointReason.TURNOVER, "endpoint.turnover")

        for index, event in enumerate(self.events):
            if event.play_type not in MADE_SHOTS:
                continue
            root = self.decisions[index].parent_order
            has_ft = any(
                candidate.play_type in FT_TYPES
                and self.decisions[candidate_index].parent_order == root
                for candidate_index, candidate in enumerate(self.events)
            )
            if not has_ft:
                self._set_endpoint(
                    index,
                    EndpointReason.MADE_FIELD_GOAL,
                    "endpoint.made_field_goal",
                )

        self._resolve_missed_shots()
        self._resolve_period_end_offensive_rebounds()
        self._resolve_free_throw_trips()

    def _resolve_missed_shots(self) -> None:
        for index, event in enumerate(self.events):
            if event.play_type not in MISSED_SHOTS:
                continue
            has_block = False
            outcome: Optional[str] = None
            rebound_team: Optional[str] = None
            rebound_type: Optional[str] = None
            for later_index in self._after(index):
                later = self.events[later_index]
                if later.play_type in {"AG", "FV"}:
                    has_block = True
                    continue
                if later.play_type in ADMINISTRATIVE | {"CM", "RV"}:
                    continue
                if later.play_type in REBOUNDS:
                    rebound_team = later.team
                    rebound_type = later.play_type
                    continue
                if later.play_type in PERIOD_END:
                    outcome = "PERIOD_END"
                elif later.play_type in LIVE_BOUNDARIES and not _same(
                    later.team, event.team
                ):
                    outcome = "OPPONENT_CONTROL"
                break

            if rebound_type is not None:
                if rebound_team is not None and event.team is not None:
                    outcome = "O" if rebound_team == event.team else "D"
                else:
                    outcome = rebound_type

            if outcome in {"D", "OPPONENT_CONTROL"}:
                reason = (
                    EndpointReason.BLOCKED_SHOT_DEFENSIVE_REBOUND
                    if has_block
                    else EndpointReason.MISS_DEFENSIVE_REBOUND
                )
                self._set_endpoint(index, reason, "endpoint.missed_shot_control")
            elif outcome == "PERIOD_END":
                reason = (
                    EndpointReason.PERIOD_END_BLOCKED_MISS
                    if has_block
                    else EndpointReason.PERIOD_END_MISS
                )
                self._set_endpoint(index, reason, "endpoint.missed_shot_period")

    def _resolve_period_end_offensive_rebounds(self) -> None:
        for index, event in enumerate(self.events):
            if event.play_type not in PERIOD_END:
                continue
            for prior_index in self._before(index):
                prior = self.events[prior_index]
                if prior.play_type in ADMINISTRATIVE | {"AG", "FV"}:
                    continue
                if prior.play_type == "O":
                    self._set_endpoint(
                        prior_index,
                        EndpointReason.PERIOD_END_OFFENSIVE_REBOUND,
                        "endpoint.period_end_offensive_rebound",
                    )
                break

    def _resolve_free_throw_trips(self) -> None:
        trips = list(
            dict.fromkeys(
                decision.ft_trip_id
                for decision in self.decisions
                if decision.ft_trip_id is not None
            )
        )
        for trip in trips:
            indices = [
                index
                for index, decision in enumerate(self.decisions)
                if decision.ft_trip_id == trip
            ]
            final = max(indices)
            root = self.decisions[final].parent_order
            group_indices = self._indices_with_parent(root)
            group_types = {self.events[index].play_type for index in group_indices}
            is_and_one = bool(group_types & MADE_SHOTS)
            is_special = bool(group_types & SPECIAL_FOULS)
            if is_special:
                continue

            if self.events[final].play_type == "FTM":
                lower = min(group_indices)
                is_compound = any(
                    self.events[index].play_type in SPECIAL_FOULS
                    and self.decisions[index].parent_order != root
                    for index in range(lower, final + 1)
                )
                if is_and_one:
                    made_index = next(
                        index
                        for index in group_indices
                        if self.events[index].play_type in MADE_SHOTS
                    )
                    reason = (
                        EndpointReason.AND_ONE_FINAL_FT
                        if _same(
                            self.events[made_index].player_id,
                            self.events[final].player_id,
                        )
                        else EndpointReason.MADE_BASKET_DEAD_BALL_FT
                    )
                elif is_compound:
                    # One dead-ball cluster can contain a turnover by the
                    # eventual FT team followed by that team's already-earned
                    # personal-foul shots (for example offsetting common and
                    # unsportsmanlike penalties). The turnover already closes
                    # that offense's possession; the later FT trip must not
                    # emit a second same-team endpoint.
                    earlier_same_team_endpoint = any(
                        self.decisions[index].final_end_poss
                        and _same(self.events[index].team, self.events[final].team)
                        for index in range(lower, final)
                    )
                    if earlier_same_team_endpoint:
                        self.decisions[final].trace.append(
                            "endpoint.compound_same_team_already_closed"
                        )
                        continue
                    reason = EndpointReason.COMPOUND_PENALTY_RESOLVED
                else:
                    reason = EndpointReason.ORDINARY_FT_FINAL_MAKE
                self._set_endpoint(final, reason, "endpoint.final_made_ft")
                continue

            outcome: Optional[str] = None
            rebound_team: Optional[str] = None
            rebound_type: Optional[str] = None
            for later_index in self._after(final):
                later = self.events[later_index]
                if later.play_type in ADMINISTRATIVE | {"CM", "RV"}:
                    continue
                if later.play_type in REBOUNDS:
                    rebound_team = later.team
                    rebound_type = later.play_type
                    continue
                if later.play_type in PERIOD_END:
                    outcome = "PERIOD_END"
                elif later.play_type in LIVE_BOUNDARIES and not _same(
                    later.team, self.events[final].team
                ):
                    outcome = "OPPONENT_CONTROL"
                break

            if rebound_type is not None:
                if rebound_team is not None and self.events[final].team is not None:
                    outcome = (
                        "O" if rebound_team == self.events[final].team else "D"
                    )
                else:
                    outcome = rebound_type

            if outcome in {"D", "PERIOD_END", "OPPONENT_CONTROL"}:
                reason = (
                    EndpointReason.FINAL_FT_MISS_DEFENSIVE_REBOUND
                    if outcome == "D"
                    else EndpointReason.FINAL_FT_MISS_END
                )
                self._set_endpoint(final, reason, "endpoint.final_missed_ft")

    def _mark_compound_penalties_provisional(self) -> None:
        boundaries = LIVE_BOUNDARIES | PERIOD_END
        special_indices = [
            index
            for index, event in enumerate(self.events)
            if event.play_type in SPECIAL_FOULS
        ]
        for special_index in special_indices:
            cluster_indices = [special_index]
            for nearby_index in self._before(special_index):
                if self.events[nearby_index].play_type in boundaries:
                    break
                cluster_indices.append(nearby_index)
            for nearby_index in self._after(special_index):
                if self.events[nearby_index].play_type in boundaries:
                    break
                cluster_indices.append(nearby_index)

            for nearby_index in cluster_indices:
                if self.events[nearby_index].play_type in {"CM"} | FT_TYPES:
                    self.decisions[nearby_index].mark_provisional(
                        90, "qa.same_penalty_cluster_as_special"
                    )

    def _result_frame(self) -> pd.DataFrame:
        result = self.frame.copy()
        result["synthetic_parent_order"] = pd.array(
            [decision.parent_order for decision in self.decisions], dtype="Int64"
        )
        result["synthetic_ft_trip_id"] = [
            decision.ft_trip_id for decision in self.decisions
        ]
        result["final_end_poss"] = [
            decision.final_end_poss for decision in self.decisions
        ]
        result["end_reason"] = [decision.end_reason for decision in self.decisions]
        result["grouping_status"] = [
            decision.status.value for decision in self.decisions
        ]
        result["grouping_confidence_pct"] = [
            decision.confidence_pct for decision in self.decisions
        ]
        result["decision_trace"] = [
            " | ".join(decision.trace) for decision in self.decisions
        ]
        return result


def group_events(events: pd.DataFrame) -> pd.DataFrame:
    """Group all events while preserving the caller's original row order."""

    normalized = normalize_events(events)
    normalized["_input_row"] = range(len(normalized))
    sort_columns = ["season", "gamecode", "period", "source_event_order"]
    ordered = normalized.sort_values(sort_columns, kind="stable")

    grouped_frames: list[pd.DataFrame] = []
    for _, period_frame in ordered.groupby(
        ["season", "gamecode", "period"], sort=True, dropna=False
    ):
        grouped_frames.append(PeriodParser(period_frame).parse())

    if grouped_frames:
        result = pd.concat(grouped_frames, ignore_index=True)
        result = result.sort_values("_input_row", kind="stable")
    else:
        result = normalized.copy()
        for column, dtype in {
            "synthetic_parent_order": "Int64",
            "synthetic_ft_trip_id": "object",
            "final_end_poss": "bool",
            "end_reason": "object",
            "grouping_status": "object",
            "grouping_confidence_pct": "int64",
            "decision_trace": "object",
        }.items():
            result[column] = pd.Series(dtype=dtype)

    return result.drop(columns="_input_row").reset_index(drop=True)
