"""Typed state carried by the EuroLeague event parser."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class GroupingStatus(str, Enum):
    CONFIRMED = "confirmed"
    PROVISIONAL = "provisional"
    UNRESOLVED = "unresolved"


class EndpointReason(str, Enum):
    TURNOVER = "turnover"
    MADE_FIELD_GOAL = "made_field_goal"
    MISS_DEFENSIVE_REBOUND = "miss_defensive_rebound"
    BLOCKED_SHOT_DEFENSIVE_REBOUND = "blocked_shot_defensive_rebound"
    PERIOD_END_MISS = "period_end_miss"
    PERIOD_END_BLOCKED_MISS = "period_end_blocked_miss"
    PERIOD_END_OFFENSIVE_REBOUND = "period_end_offensive_rebound"
    ORDINARY_FT_FINAL_MAKE = "ordinary_ft_trip_final_make"
    AND_ONE_FINAL_FT = "and_one_final_ft"
    MADE_BASKET_DEAD_BALL_FT = "made_basket_dead_ball_ft"
    COMPOUND_PENALTY_RESOLVED = "compound_penalty_offense_resolved"
    FINAL_FT_MISS_DEFENSIVE_REBOUND = "final_ft_miss_defensive_rebound"
    FINAL_FT_MISS_END = "final_ft_miss_end"


@dataclass(frozen=True, slots=True)
class Event:
    """Normalized immutable provider event."""

    season: int
    gamecode: int
    source_order: int
    period: int
    clock: Optional[str]
    team: Optional[str]
    play_type: str
    player_id: Optional[str]


@dataclass(slots=True)
class EventDecision:
    """Mutable, auditable decisions derived for one event."""

    parent_order: int
    ft_trip_id: Optional[str] = None
    final_end_poss: bool = False
    end_reason: Optional[str] = None
    status: GroupingStatus = GroupingStatus.CONFIRMED
    confidence_pct: int = 100
    trace: list[str] = field(default_factory=list)

    def attach(self, parent_order: int, rule: str) -> None:
        self.parent_order = parent_order
        self.trace.append(rule)

    def mark_endpoint(self, reason: EndpointReason, rule: str) -> None:
        self.final_end_poss = True
        self.end_reason = reason.value
        self.trace.append(rule)

    def mark_provisional(self, confidence_pct: int, rule: str) -> None:
        if self.status != GroupingStatus.UNRESOLVED:
            self.status = GroupingStatus.PROVISIONAL
        self.confidence_pct = min(self.confidence_pct, confidence_pct)
        self.trace.append(rule)

    def mark_unresolved(self, rule: str) -> None:
        self.status = GroupingStatus.UNRESOLVED
        self.confidence_pct = 0
        self.trace.append(rule)


@dataclass(slots=True)
class PendingPenalty:
    """Explicit candidate state used while resolving a free throw."""

    event_index: int
    parent_order: int
    benefit_team: Optional[str]
    benefit_player: Optional[str]
    foul_type: str
    last_ft_index: Optional[int] = None
