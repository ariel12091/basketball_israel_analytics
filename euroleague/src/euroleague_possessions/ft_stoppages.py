"""Experimental, database-free FT incident state machine and constraint solver.

Not imported by the production parser. Candidate trips stay provisional even
when uniquely solved: observed counts do not independently prove entitlement.
Source order is identity; clocks are supporting evidence, never incident IDs.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from itertools import combinations
import json

import pandas as pd

from .parser import (
    ADMINISTRATIVE, FT_TYPES, MADE_SHOTS, PERIOD_END, SHOTS,
    is_committed_foul, is_one_shot_special, is_special_foul,
    is_unsportsmanlike, normalize_events,
)


class Phase(str, Enum):
    LIVE = "live"
    PENDING = "pending"
    ADMINISTERING = "administering"


@dataclass
class Incident:
    indices: list[int] = field(default_factory=list)
    phase: Phase = Phase.PENDING
    warnings: set[str] = field(default_factory=set)
    transitions: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Entitlement:
    foul: int
    root: int
    kind: str
    team: str | None
    shooters: frozenset[str]
    sizes: tuple[int, ...]
    warnings: tuple[str, ...] = ()
    # (out row, in row, incoming player): the fouled player left and this
    # team-mate came in during the same stoppage (FIBA: the substitute shoots).
    replacements: tuple[tuple[int, int, str], ...] = ()


def _opponent(team, teams):
    other = [t for t in teams if t != team]
    return other[0] if team and len(other) == 1 else None


def _incidents(rows):
    """Advance live -> pending -> administering -> live at evidence boundaries.

    A made basket can enter a pending foul window only for its fouled shooter.
    Team turnovers associated with special penalties are retained, but flagged.
    A new foul at a different clock after FTs closes the prior incident. An
    unmatched live event always closes; no solver may reach across that cut.
    """
    completed = []
    active = None
    anchor = []

    def finish(reason):
        nonlocal active
        if active:
            active.transitions.append(reason)
            completed.append(active)
            active = None

    for i, event in enumerate(rows):
        code = event.play_type
        if code in PERIOD_END | {"BP"}:
            finish("period_boundary")
            anchor = []
            continue
        foul = is_committed_foul(code) and code != "OF"
        if foul or code == "RV" or code in FT_TYPES:
            if active and active.phase == Phase.PENDING and code == "CM":
                prior_fouls = [rows[j] for j in active.indices if rows[j].play_type == "CM"]
                prior_evidence = prior_fouls or [rows[j] for j in active.indices if rows[j].play_type == "RV"]
                has_special = any(is_special_foul(rows[j].play_type) for j in active.indices)
                if (not has_special and prior_evidence and event.clock and prior_evidence[-1].clock
                        and event.clock != prior_evidence[-1].clock):
                    upcoming_drawn = None
                    for later in rows[i + 1:]:
                        if later.play_type == "RV":
                            upcoming_drawn = later
                            break
                        if later.play_type in SHOTS | FT_TYPES | {"TO", "OF", "D", "O"} or is_committed_foul(later.play_type):
                            break
                    preceding = prior_evidence[-1]
                    unpaired_drawn = (not prior_fouls and len(prior_evidence) == 1
                        and preceding.team_code and preceding.team_code != event.team_code
                        and preceding.player_id and (upcoming_drawn is None or (
                            upcoming_drawn.team_code == preceding.team_code
                            and upcoming_drawn.player_id == preceding.player_id)))
                    if unpaired_drawn:
                        active.transitions.append("retain_unpaired_drawn_across_clock")
                    else:
                        # A new foul at the recent basket's clock can belong
                        # to that basket rather than the older pending foul.
                        # Transfer the unadministered basket tail to the new
                        # incident; it cannot be lost with the stale foul.
                        # A basket after the clock ran since the old foul is a
                        # later play, never that foul's and-one (E2026_89 Q2).
                        made = [j for j in active.indices if rows[j].play_type in MADE_SHOTS
                                and rows[j].clock and (rows[j].clock == event.clock
                                                       or rows[j].clock != preceding.clock)
                                and rows[j].team_code
                                and rows[j].team_code != event.team_code]
                        if len(made) == 1:
                            position = active.indices.index(made[0])
                            tail = active.indices[position:]
                            if not any(rows[j].play_type in FT_TYPES for j in tail):
                                anchor = [j for j in tail if rows[j].play_type in MADE_SHOTS | ADMINISTRATIVE | {"AS", "RV"}]
                                active.indices = active.indices[:position]
                        finish("new_contact_foul_at_different_clock")
            if active and active.phase == Phase.PENDING and code == "RV":
                prior = [rows[j] for j in active.indices if rows[j].play_type == "RV"]
                has_special = any(is_special_foul(rows[j].play_type) for j in active.indices)
                if (not has_special and prior and event.clock and prior[-1].clock
                        and event.clock != prior[-1].clock and event.team_code == prior[-1].team_code
                        and event.player_id != prior[-1].player_id):
                    finish("new_fouled_player_at_different_clock")
            if active and foul and active.phase == Phase.ADMINISTERING:
                last_ft = next(rows[j] for j in reversed(active.indices) if rows[j].play_type in FT_TYPES)
                if event.clock and last_ft.clock and event.clock != last_ft.clock:
                    finish("new_foul_after_ft_at_different_clock")
            if active is None:
                if anchor:
                    basket = rows[anchor[0]]
                    incompatible = (foul and event.team_code == basket.team_code) or (
                        code == "RV" and event.team_code != basket.team_code)
                    if incompatible:
                        anchor = []
                active = Incident(indices=anchor.copy(), transitions=["live_to_pending"])
                anchor = []
            active.indices.append(i)
            if code in FT_TYPES:
                if active.phase != Phase.ADMINISTERING:
                    active.transitions.append("pending_to_administering")
                active.phase = Phase.ADMINISTERING
            continue
        if code in ADMINISTRATIVE | {"AS", "ST", "AG", "FV"}:
            if active:
                active.indices.append(i)
            elif anchor:
                anchor.append(i)
            continue
        has_special = active and any(is_special_foul(rows[j].play_type) for j in active.indices)
        penalty_turnover = code == "TO" and event.player_id is None and has_special
        # OF/TO may be a penalty annotation, but the provider cannot prove that.
        compound = code in {"TO", "OF"} and has_special and active.phase == Phase.PENDING
        if active and (penalty_turnover or compound):
            active.indices.append(i)
            active.warnings.add("penalty_turnover_boundary_requires_review")
            continue
        if active and active.phase == Phase.PENDING and code in {"D", "O"}:
            if any(is_committed_foul(rows[j].play_type) for j in active.indices):
                active.indices.append(i)
                active.warnings.add("rebound_after_foul_before_fts_requires_review")
                continue
        if code in MADE_SHOTS and active:
            matches = any(rows[j].play_type == "RV" and rows[j].team_code == event.team_code
                          and rows[j].player_id and rows[j].player_id == event.player_id
                          for j in active.indices)
            already_shot = any(rows[j].play_type in SHOTS for j in active.indices)
            prior_fts = [rows[j] for j in active.indices if rows[j].play_type in FT_TYPES]
            delayed_basket = (has_special and prior_fts
                              and all(e.team_code != event.team_code for e in prior_fts)
                              and all(e.clock and e.clock == event.clock for e in prior_fts))
            # The clock ran since every foul on the scorer: a later play, not an
            # and-one (805 such baskets in the cached games, 2026-11-19).
            clock_ran = event.clock and all(
                rows[j].clock and rows[j].clock != event.clock for j in active.indices
                if rows[j].play_type == "RV" and rows[j].player_id == event.player_id)
            if matches and not already_shot and not clock_ran and (
                    active.phase == Phase.PENDING or delayed_basket):
                active.indices.append(i)
                active.transitions.append("pending_and_one_basket")
                if delayed_basket:
                    active.warnings.add("and_one_basket_logged_after_other_penalty_fts")
                continue
        finish(f"live_boundary:{code}")
        anchor = [i] if code in MADE_SHOTS else []
    finish("period_input_end")
    return completed


def _drawn_candidates(rows, incident, foul_index):
    """Local explicit RV evidence, bounded by another contact foul or live/FT row.

    No row-distance scores. Opposing RV rows on both sides remain alternatives.
    Technical fouls do not consume contact-foul RV evidence.
    """
    position = incident.indices.index(foul_index)
    foul = rows[foul_index]
    found = set()
    for direction in (-1, 1):
        k = position + direction
        while 0 <= k < len(incident.indices):
            e = rows[incident.indices[k]]
            if e.play_type == "CM" or is_unsportsmanlike(e.play_type) or e.play_type in SHOTS | FT_TYPES | {"TO", "OF", "D", "O"}:
                break
            if e.play_type == "RV" and e.team_code and e.team_code != foul.team_code and e.player_id:
                found.add(incident.indices[k])
            k += direction
    return frozenset(found)


def _drawn_players(rows, incident, foul_index):
    return frozenset(rows[i].player_id for i in _drawn_candidates(rows, incident, foul_index))


def _personal_pairings(rows, incident, search_budget):
    """Enumerate one-to-one CM/RV pairings, preserving shared-evidence constraints.

    An RV source row cannot identify two personal fouls. Multiple observed RV
    rows for the same player remain distinct evidence; identical player mappings
    are deduplicated. Missing RVs remain explicit in the entitlement warnings.
    """
    edges = [(i, sorted(_drawn_candidates(rows, incident, i))) for i in incident.indices
             if rows[i].play_type == "CM"]
    edges = [(i, candidates) for i, candidates in edges if candidates]
    mappings, seen, visited = [], set(), 0
    exhausted = False

    def visit(k, used, mapping):
        nonlocal visited, exhausted
        visited += 1
        if visited > search_budget:
            exhausted = True
            return
        if k == len(edges):
            signature = tuple(sorted(mapping.items()))
            if signature not in seen:
                seen.add(signature)
                mappings.append({i: frozenset({player}) for i, player in mapping.items()})
            return
        foul, candidates = edges[k]
        for drawn in candidates:
            if drawn not in used:
                visit(k + 1, used | {drawn}, mapping | {foul: rows[drawn].player_id})
                if exhausted:
                    return
    visit(0, set(), {})
    return mappings, exhausted


def _entitlements(rows, incident, teams, drawn_override=None):
    fouls = [i for i in incident.indices if is_committed_foul(rows[i].play_type) and rows[i].play_type != "OF"]
    technicals = [i for i in fouls if is_one_shot_special(rows[i].play_type) and rows[i].play_type != "CMTI"]
    cancelled = set()
    # A deliberately narrow cancellation candidate: two opposing technicals,
    # one clock and neither administered between the two logged foul rows.
    if len(technicals) == 2:
        a, b = technicals
        if (rows[a].team_code and rows[b].team_code and rows[a].team_code != rows[b].team_code
                and rows[a].clock and rows[a].clock == rows[b].clock
                and not any(rows[j].play_type in FT_TYPES for j in range(a + 1, b))):
            cancelled.update(technicals)
            incident.warnings.add("opposing_technical_cancellation_candidate")
    result = []
    for i in fouls:
        e = rows[i]
        team = _opponent(e.team_code, teams)
        drawn = (drawn_override[i] if drawn_override is not None and i in drawn_override
                 else _drawn_players(rows, incident, i))
        made = [j for j in incident.indices if rows[j].play_type in MADE_SHOTS
                and rows[j].team_code == team and rows[j].player_id in drawn]
        warnings = []
        if i in cancelled:
            sizes = (0,)
            root = i
        elif is_one_shot_special(e.play_type):
            sizes = (1,)
            root = i
            if e.play_type != "CMTI":
                drawn = frozenset()
        elif is_unsportsmanlike(e.play_type):
            sizes = (1,) if len(made) == 1 else (2, 3)
            root = made[0] if len(made) == 1 else i
            warnings.append("unsportsmanlike_award_requires_context")
        elif e.play_type == "CMD":
            sizes = (1, 2, 3)
            root = i
            drawn = frozenset()
            warnings.append("disqualification_contact_and_award_unknown")
        else:
            sizes = (0, 1) if len(made) == 1 else (0, 2, 3)
            root = made[0] if len(made) == 1 else i
        if not drawn and e.play_type in {"CM", "CMTI"}:
            warnings.append("missing_fouled_player")
        if len(drawn) > 1:
            warnings.append("ambiguous_fouled_player")
        result.append(Entitlement(i, root, e.play_type, team, drawn, sizes, tuple(warnings),
                                  _replacements(rows, incident, team, drawn)))
    return result


def _replacements(rows, incident, team, drawn):
    """Substitutes allowed to shoot for a fouled player who left in this stoppage.

    Approved 2026-11-17: the fouled player's OUT and a team-mate's IN inside the
    same incident are enough; the injury or disqualification itself is not in
    the feed. All 53 reviewed replacement rows show this pair.
    """
    if not team or not drawn:
        return ()
    outs = [j for j in incident.indices if rows[j].play_type == "OUT"
            and rows[j].team_code == team and rows[j].player_id in drawn]
    ins = [j for j in incident.indices if rows[j].play_type == "IN"
           and rows[j].team_code == team and rows[j].player_id and rows[j].player_id not in drawn]
    return tuple((o, n, rows[n].player_id) for o in outs for n in ins)


def _shooters_ok(rows, entitlement, selected):
    """One shooter, or the fouled player then his substitute across the sub."""
    players = [rows[i].player_id for i in selected]
    if len(set(players)) <= 1:
        player = players[0] if players else None
        return (player is None or not entitlement.shooters or player in entitlement.shooters
                or any(player == q for _, _, q in entitlement.replacements))
    if len(set(players)) != 2 or players[0] not in entitlement.shooters:
        return False
    first, second = players[0], next(p for p in players if p != players[0])
    split = players.index(second)
    if first in players[split:]:
        return False
    return any(q == second and rows[o].player_id == first
               and selected[split - 1] < o < selected[split] and selected[split - 1] < n < selected[split]
               for o, n, q in entitlement.replacements)


def _solve(rows, incident, entitlements, search_budget, *, variants=None, diagnostics=None,
           max_solutions=2):
    fts = tuple(i for i in incident.indices if rows[i].play_type in FT_TYPES)
    solutions = []
    explored = 0
    exhausted = False
    seen = set()

    def visit(current, k, remaining, assigned):
        nonlocal explored, exhausted
        explored += 1
        if explored > search_budget:
            exhausted = True
            return
        if len(solutions) >= max_solutions:
            return
        if k == len(current):
            if not remaining:
                signature = tuple((ent.foul, ent.root, attempts) for ent, attempts in assigned if attempts)
                if signature not in seen:
                    seen.add(signature)
                    solutions.append(assigned.copy())
            return
        entitlement = current[k]
        substitutes = {q for _, _, q in entitlement.replacements}
        available = [i for i in remaining if entitlement.team and rows[i].team_code == entitlement.team
                     and rows[i].player_id and (not entitlement.shooters or rows[i].player_id in entitlement.shooters
                                                or rows[i].player_id in substitutes)]
        for size in entitlement.sizes:
            for selected in combinations(available, size):
                if not _shooters_ok(rows, entitlement, selected):
                    continue
                chosen = set(selected)
                visit(current, k + 1, tuple(i for i in remaining if i not in chosen), assigned + [(entitlement, selected)])
                if exhausted or len(solutions) >= max_solutions:
                    return
    for current in variants if variants is not None else [entitlements]:
        visit(current, 0, fts, [])
        if exhausted or len(solutions) >= max_solutions:
            break
    if diagnostics is not None:
        diagnostics["solutions"] = solutions
        diagnostics["alternatives"] = [[dict(foul=int(rows[ent.foul].source_event_order),
            parent=int(rows[ent.root].source_event_order),
            attempts=[int(rows[i].source_event_order) for i in attempts])
            for ent, attempts in solution if attempts] for solution in solutions]
    return ("ambiguous" if len(solutions) > 1 else "search_limit" if exhausted
            else "unique" if solutions else "no_fit"), solutions[0] if solutions else []


def _resolve_competing_fouls(rows, solutions):
    """Pick among allocations that differ only in which equivalent foul owns a trip.

    Approved 2026-11-17: every alternative must split the FTs into the same
    trips, and each trip's candidate fouls must share kind, fouling team,
    fouled player and parent lineup (no substitution between the candidate
    parents). The trip then goes to the latest candidate foul logged before its
    first attempt. Anything else stays ambiguous.
    """
    grouped = [{tuple(sorted(a)): ent for ent, a in s if a} for s in solutions]
    trips = set(grouped[0])
    if any(set(g) != trips for g in grouped):
        return None
    chosen = {}
    for trip in trips:
        candidates = list({g[trip].foul: g[trip] for g in grouped}.values())
        if len(candidates) == 1:
            chosen[trip] = candidates[0].foul
            continue
        if (len({e.kind for e in candidates}) > 1 or len({e.shooters for e in candidates}) > 1
                or len({rows[e.foul].team_code for e in candidates}) > 1
                or not candidates[0].shooters):
            return None
        parents = [e.root for e in candidates]
        if any(rows[j].play_type in {"IN", "OUT"} for j in range(min(parents), max(parents) + 1)):
            return None
        before = [e.foul for e in candidates if e.foul < trip[0]]
        if not before:
            return None
        chosen[trip] = max(before)
    for solution, g in zip(solutions, grouped):
        if all(g[t].foul == chosen[t] for t in trips):
            return solution
    return None


def _replacement_evidence(rows, incident):
    ft = [i for i in incident.indices if rows[i].play_type in FT_TYPES]
    for i in incident.indices:
        drawn = rows[i]
        if drawn.play_type != "RV" or not drawn.player_id:
            continue
        later_ft = next((j for j in ft if j > i and rows[j].team_code == drawn.team_code), None)
        if later_ft is None or rows[later_ft].player_id == drawn.player_id:
            continue
        middle = rows[i + 1:later_ft]
        if (any(e.play_type == "OUT" and e.team_code == drawn.team_code and e.player_id == drawn.player_id for e in middle)
                and any(e.play_type == "IN" and e.team_code == drawn.team_code and e.player_id == rows[later_ft].player_id for e in middle)):
            return True
    for a, b in zip(ft, ft[1:]):
        left, right = rows[a], rows[b]
        if left.team_code != right.team_code or left.player_id == right.player_id:
            continue
        middle = rows[a + 1:b]
        out = any(e.play_type == "OUT" and e.team_code == left.team_code and e.player_id == left.player_id for e in middle)
        incoming = any(e.play_type == "IN" and e.team_code == right.team_code and e.player_id == right.player_id for e in middle)
        if out and incoming:
            return True
    return False


def infer_ft_stoppages(events: pd.DataFrame, *, search_budget: int = 20000) -> pd.DataFrame:
    """Add shadow columns; never overwrite canonical grouping or endpoint fields.

    All unique candidates remain provisional. Unsolved attempts have null parent,
    trip, attempt and size. Endpoint candidates cover only uniquely assigned FTs;
    they are not a full replacement possession stream.
    """
    if search_budget < 1:
        raise ValueError("search_budget must be positive")
    frame = normalize_events(events)
    frame["_shadow_input"] = range(len(frame))
    output = []
    for _, period in frame.groupby(["season", "gamecode", "period"], sort=True):
        part = period.sort_values("source_event_order", kind="stable").reset_index(drop=True)
        rows = list(part.itertuples(index=False))
        teams = sorted({r.team_code for r in rows if r.team_code})
        decisions = {i: {} for i, r in enumerate(rows) if r.play_type in FT_TYPES}
        for incident in _incidents(rows):
            fts = [i for i in incident.indices if rows[i].play_type in FT_TYPES]
            if not fts:
                continue
            entitlements = _entitlements(rows, incident, teams)
            pairings, pairing_limit = _personal_pairings(rows, incident, search_budget)
            if sum(rows[i].play_type == "CM" for i in incident.indices) > 1:
                incident.transitions.append("joint_one_to_one_personal_drawn_pairing")
            diagnostics = {}
            variants = [_entitlements(rows, incident, teams, pairing) for pairing in pairings]
            if pairing_limit:
                status, assignment = "search_limit", []
            else:
                if not variants:
                    incident.warnings.add("personal_foul_drawn_pairing_conflict")
                def attempt(allow):
                    # A substitute shoots only when nothing fits with the fouled
                    # players themselves: E2025_259 Q2 had a substitute who had
                    # drawn his own foul in the same stoppage.
                    current = variants if allow else [
                        [replace(e, replacements=()) for e in v] for v in variants]
                    found = {}
                    result, chosen = _solve(rows, incident, entitlements, search_budget,
                                            variants=current, diagnostics=found)
                    if result == "ambiguous":
                        every = {}
                        wide, _ = _solve(rows, incident, entitlements, search_budget,
                                         variants=current, diagnostics=every, max_solutions=32)
                        picked = (_resolve_competing_fouls(rows, every["solutions"])
                                  if wide == "ambiguous" and len(every["solutions"]) < 32 else None)
                        if picked is not None:
                            return "unique", picked, found, True
                    return result, chosen, found, False
                status, assignment, diagnostics, resolved = attempt(False)
                if status == "no_fit":
                    fallback = attempt(True)
                    if fallback[0] != "no_fit":
                        status, assignment, diagnostics, resolved = fallback
                if resolved:
                    incident.transitions.append("competing_fouls_resolved_to_latest_before_trip")
            if status == "unique" and any(
                    attempts and any(rows[i].player_id not in ent.shooters for i in attempts) and ent.shooters
                    for ent, attempts in assignment):
                incident.transitions.append("replacement_shooter_after_substitution")
            active_penalties = [ent for ent, attempts in assignment if attempts]
            if status == "unique" and len(active_penalties) > 1:
                if any(ent.kind == "CMTI" or is_unsportsmanlike(ent.kind) or ent.kind == "CMD"
                       for ent in active_penalties):
                    incident.warnings.add("retained_penalty_sequence_requires_review")
                if sum(ent.kind == "CM" for ent in active_penalties) > 1:
                    incident.warnings.add("multiple_ordinary_trips_endpoint_order_requires_review")
            if status == "no_fit" and _replacement_evidence(rows, incident):
                status = "replacement_requires_evidence"
            first = rows[incident.indices[0]]
            identity = f"SFT:{first.season}:{first.gamecode}:{first.period}:{first.source_event_order}"
            trace = sorted(incident.warnings) + incident.transitions
            if len({rows[i].clock for i in incident.indices if rows[i].clock}) > 1:
                trace.append("clock_discrepancy")
            for i in fts:
                decisions[i] = dict(shadow_incident_id=identity, shadow_status=status,
                                    shadow_grouping_status="provisional" if status == "unique" else "unresolved",
                                    shadow_trace=json.dumps(trace), shadow_ft_end_poss=None,
                                    shadow_alternatives=json.dumps(diagnostics.get("alternatives", []))
                                        if status == "ambiguous" else None)
            if status != "unique":
                continue
            for entitlement, attempts in assignment:
                if not attempts:
                    continue
                ordered = sorted(attempts)
                for number, i in enumerate(ordered, 1):
                    decisions[i].update(shadow_parent_order=rows[entitlement.root].source_event_order,
                        shadow_foul_order=rows[entitlement.foul].source_event_order,
                        shadow_ft_trip_id=f"{identity}:F{rows[entitlement.foul].source_event_order}",
                        shadow_ft_attempt=number, shadow_ft_awarded=len(ordered),
                        shadow_trace=json.dumps(trace + list(entitlement.warnings)),
                        shadow_suppressed_basket_order=rows[entitlement.root].source_event_order
                            if entitlement.root != entitlement.foul else None,
                        shadow_ft_end_poss=False)
                final = ordered[-1]
                if not is_special_foul(entitlement.kind):
                    end = None
                    if rows[final].play_type == "FTM":
                        end = True
                    else:
                        for later in rows[final + 1:]:
                            if later.play_type in ADMINISTRATIVE | {"AS", "RV"}:
                                continue
                            if later.play_type in {"D", "O"} and later.team_code:
                                end = later.team_code != rows[final].team_code
                            elif later.play_type in PERIOD_END:
                                end = True
                            # Another pending penalty/control conflict needs a
                            # complete possession model, never a guessed end.
                            break
                    decisions[final]["shadow_ft_end_poss"] = end
                if incident.warnings or entitlement.warnings:
                    # Boundary and cancellation hypotheses do not prove ends.
                    for i in attempts:
                        decisions[i]["shadow_ft_end_poss"] = None
        columns = ["shadow_incident_id", "shadow_status", "shadow_grouping_status", "shadow_trace", "shadow_alternatives",
                   "shadow_parent_order", "shadow_foul_order", "shadow_ft_trip_id", "shadow_ft_attempt",
                   "shadow_ft_awarded", "shadow_ft_end_poss", "shadow_suppressed_basket_order"]
        for column in columns:
            part[column] = pd.Series([decisions.get(i, {}).get(column) for i in range(len(rows))], dtype="object")
        output.append(part)
    if not output:
        result = frame.copy()
        for column in ["shadow_incident_id", "shadow_status", "shadow_grouping_status", "shadow_trace", "shadow_alternatives",
                       "shadow_parent_order", "shadow_foul_order", "shadow_ft_trip_id", "shadow_ft_attempt",
                       "shadow_ft_awarded", "shadow_ft_end_poss", "shadow_suppressed_basket_order"]:
            result[column] = pd.Series(dtype="object")
    else:
        result = pd.concat(output, ignore_index=True).sort_values("_shadow_input")
    for column in ["shadow_parent_order", "shadow_foul_order", "shadow_ft_attempt", "shadow_ft_awarded", "shadow_suppressed_basket_order"]:
        result[column] = pd.array(result[column], dtype="Int64")
    result["shadow_ft_end_poss"] = pd.array(result["shadow_ft_end_poss"], dtype="boolean")
    return result.drop(columns="_shadow_input").reset_index(drop=True)


def _miss_end_reason(rows, final):
    for later in rows[final + 1:]:
        if later.play_type in ADMINISTRATIVE | {"AS", "RV"}:
            continue
        return "final_ft_miss_end" if later.play_type in PERIOD_END else "final_ft_miss_defensive_rebound"
    return "final_ft_miss_end"


def apply_ft_stoppages(grouped: pd.DataFrame) -> pd.DataFrame:
    """Overlay the stoppage model's unique answers on the parser's grouping (parser 0.3.0).

    Only incidents whose every FT is uniquely assigned are taken, and for an
    ordinary trip only when the model proposes its possession end; anything else
    keeps the parser's answer. Special-foul trips keep the parser's FT ends (they
    keep the ball, D29/D36) but take the model's parent, trip and suppressed
    basket. A trip identical to the parser's keeps its id, status and trace.
    """
    if grouped.empty:
        return grouped
    out = grouped.reset_index(drop=True).copy()
    base = out.copy()
    shadow = infer_ft_stoppages(out).reset_index(drop=True)
    if not (shadow["source_event_order"].to_numpy() == out["source_event_order"].to_numpy()).all():
        raise RuntimeError("stoppage model changed row order")
    out["synthetic_parent_order"] = out["synthetic_parent_order"].astype("Int64")
    for _, period in out.groupby(["season", "gamecode", "period"], sort=False):
        ordered = period.sort_values("source_event_order", kind="stable")
        rows = list(ordered.itertuples(index=False))
        idx = list(ordered.index)
        pos = {int(r.source_event_order): k for k, r in enumerate(rows)}
        sh = shadow.loc[idx]
        ft = sh[sh["play_type"].isin(FT_TYPES) & sh["shadow_incident_id"].notna()]
        for _, incident in ft.groupby("shadow_incident_id", sort=False):
            if not incident["shadow_status"].eq("unique").all():
                continue
            trips = []
            for trip_id, trip in incident.groupby("shadow_ft_trip_id", sort=False):
                members = sorted(int(o) for o in trip["source_event_order"])
                foul = rows[pos[int(trip["shadow_foul_order"].iloc[0])]]
                special = is_special_foul(foul.play_type)
                end = trip.loc[trip["source_event_order"] == members[-1], "shadow_ft_end_poss"].iloc[0]
                if not special and pd.isna(end):
                    trips = None
                    break
                trips.append((members, int(trip["shadow_parent_order"].iloc[0]),
                              int(trip["shadow_foul_order"].iloc[0]), special,
                              None if pd.isna(end) else bool(end),
                              trip["shadow_suppressed_basket_order"].dropna()))
            if not trips:
                continue
            # Dead-ball FTs right after a made basket (same second, nothing live
            # between) are one possession rooted on the basket: keep the parser's.
            kept = []
            for trip in trips:
                members, parent, foul_order = trip[0], trip[1], trip[2]
                old = base.at[idx[pos[members[-1]]], "synthetic_parent_order"]
                old_row = rows[pos[int(old)]] if pd.notna(old) and int(old) in pos else None
                foul = rows[pos[foul_order]]
                if (old_row is not None and int(old) != parent and parent == foul_order
                        and old_row.play_type in MADE_SHOTS
                        and old_row.team_code == rows[pos[members[0]]].team_code
                        and old_row.clock and old_row.clock == foul.clock):
                    continue
                kept.append(trip)
            trips = kept
            if not trips:
                continue
            old_roots = {int(base.at[idx[pos[o]], "synthetic_parent_order"])
                         for members, *_ in trips for o in members
                         if pd.notna(base.at[idx[pos[o]], "synthetic_parent_order"])}
            for members, parent, foul_order, special, end, suppressed in trips:
                at = [idx[pos[o]] for o in members]
                old_ids = set(base.loc[at, "synthetic_ft_trip_id"])
                old_id = next(iter(old_ids)) if len(old_ids) == 1 else None
                period_base = base.loc[idx]
                same_members = old_id is not None and sorted(
                    int(o) for o in period_base.loc[period_base["synthetic_ft_trip_id"] == old_id,
                                                    "source_event_order"]) == members
                same = bool(same_members and base.loc[at, "synthetic_parent_order"].notna().all()
                            and set(base.loc[at, "synthetic_parent_order"].astype(int)) == {parent})
                new_end = {i: False for i in at}
                if special:
                    new_end = {i: bool(base.at[i, "final_end_poss"]) for i in at}
                else:
                    new_end[at[-1]] = end
                ends_same = all(bool(base.at[i, "final_end_poss"]) == new_end[i] for i in at)
                if same and ends_same and suppressed.empty:
                    continue
                trip_id = old_id if same else (
                    f"EL:{rows[0].season}:{rows[0].gamecode}:{rows[0].period}:{parent}:F{foul_order}")
                for i in at:
                    out.at[i, "synthetic_parent_order"] = parent
                    out.at[i, "synthetic_ft_trip_id"] = trip_id
                    if bool(out.at[i, "final_end_poss"]) != new_end[i]:
                        out.at[i, "final_end_poss"] = new_end[i]
                        if not new_end[i]:
                            out.at[i, "end_reason"] = None
                        elif parent != foul_order:
                            out.at[i, "end_reason"] = "and_one_final_ft"
                        elif out.at[i, "play_type"] == "FTM":
                            out.at[i, "end_reason"] = "ordinary_ft_trip_final_make"
                        else:
                            out.at[i, "end_reason"] = _miss_end_reason(rows, pos[int(out.at[i, "source_event_order"])])
                    if not (same and ends_same):
                        out.at[i, "grouping_status"] = "provisional"
                        out.at[i, "decision_trace"] = f"{out.at[i, 'decision_trace']} | ft_stoppage_model"
                for basket in suppressed:
                    b = idx[pos[int(basket)]]
                    if bool(out.at[b, "final_end_poss"]):
                        out.at[b, "final_end_poss"] = False
                        out.at[b, "end_reason"] = None
                        out.at[b, "decision_trace"] = f"{out.at[b, 'decision_trace']} | ft_stoppage_model.and_one_basket"
            new_roots = {p for _, p, *_ in trips}
            for root in old_roots - new_roots:
                if root not in pos:
                    continue
                b = idx[pos[root]]
                still = out.loc[idx][out.loc[idx, "play_type"].isin(FT_TYPES)
                                     & (out.loc[idx, "synthetic_parent_order"] == root)]
                if (out.at[b, "play_type"] in MADE_SHOTS and still.empty
                        and not bool(out.at[b, "final_end_poss"])):
                    out.at[b, "final_end_poss"] = True
                    out.at[b, "end_reason"] = "made_field_goal"
                    out.at[b, "decision_trace"] = f"{out.at[b, 'decision_trace']} | ft_stoppage_model.basket_not_and_one"
    return out
