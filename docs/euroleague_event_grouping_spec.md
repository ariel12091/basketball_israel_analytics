# EuroLeague event grouping specification

Status: initial exploratory contract  
Scope: event relationships and possession endpoints only; no database or production ETL changes

## Purpose

EuroLeague play-by-play does not provide the canonical `parent_action_id` used by
the Israeli pipeline. This specification defines how to reconstruct equivalent
relationships without treating the game clock as an event identifier.

The first implementation should produce three separate outputs:

- `incident_id`: one atomic basketball play or whistle bundle;
- `ft_trip_id`: one set of free throws awarded to one team and shooter;
- `final_end_poss`: exactly one possession endpoint when an incident resolves a
  possession.

The separation matters because one clock value can contain multiple incidents,
and one compound incident can contain more than one free-throw trip.

## Source ordering

1. Order events by `(season, gamecode, TRUE_NUMBEROFPLAY)`.
2. Preserve provider `NUMBEROFPLAY` for audit, but never use it as the primary
   ordering key because it is not consistently ordered.
3. Use `PERIOD` and `MARKERTIME` as context only. Equal clocks do not imply a
   shared incident, and a clock change does not necessarily close an incident.
4. Never allow an incident or FT trip to cross a period boundary.

In the normalized schema, `TRUE_NUMBEROFPLAY` should become an immutable
`source_event_order`. Canonical integer action IDs can be assigned later.

## Incident construction

Every normalized action receives a non-null parent:

- a singleton action is its own parent;
- an incident root is its own parent;
- related child actions point to the incident root.

Root selection priority:

1. field-goal attempt for a shot, assist, block, rebound, or and-one bundle;
2. turnover for a turnover/steal bundle, except when an offensive foul appears
   first, in which case the offensive foul is the root;
3. committed foul for a standalone foul and FT bundle;
4. first substantive event when the committed-foul row is missing;
5. the action itself for substitutions and administrative events.

### Events that can join an open incident

- `AS` joins the immediately associated made shot.
- `AG` and `FV` annotate the preceding missed/blocked shot.
- `D` or `O` resolves the preceding missed field goal or missed final FT.
- `CM`/`RV` join each other when teams are opposed and the sequence describes
  one foul.
- `CM`/`RV` can join a preceding made shot to form an and-one.
- A made shot, opponent personal foul, same-team `RV`, and compatible FT trip
  can form one dead-ball scoring incident even when the FT shooter is a
  different teammate. This is labelled separately from an and-one.
- `OF`, its matching `TO`, and `RV` form one offensive-foul incident.
- `CMU`, its same-player/team `TO`, and the opponent `RV` form one
  unsportsmanlike-foul incident rather than two turnovers.
- `FTA`/`FTM` join the currently open penalty whose team, shooter, foul class,
  and ordering are compatible.

Substitutions, timeouts, reviews, and challenges are transparent while looking
for a later FT, but are not automatically children of the basketball incident.

## Free-throw trips

An FT trip is identified from event state, not from `PLAYINFO`. Values such as
`6/7` in `PLAYINFO` are cumulative player game totals rather than attempt six of
a seven-shot trip.

Open a new trip when any of these changes:

- shooting team;
- shooter;
- parent foul/incident;
- foul class or possession entitlement;
- a new committed foul appears after an FT already attached to an older foul;
- a live-ball control event intervenes.

Maintain more than one pending penalty during compound sequences. This is
required when a player is fouled and then commits an unsportsmanlike or
technical foul before either penalty is administered.

When assigning provisional compound-penalty evidence, define the penalty
cluster by live-play boundaries rather than row distance. Substitutions,
timeouts, reviews, foul annotations, and assists are transparent. A shot,
rebound, turnover, offensive foul, or period end closes the cluster. Only
ordinary fouls and FTs inside the uninterrupted cluster inherit the special
penalty's provisional status.

Each FT row must resolve to exactly one `ft_trip_id`, or be explicitly flagged
`unresolved`. An unresolved trip must not be silently assigned by clock.

## Possession endpoint rules

Emit at most one `final_end_poss = TRUE` per incident.

- A made field goal without a bonus FT ends the possession on the shot.
- An and-one ends on the final FT resolution, not on both the shot and FT.
- An ordinary made final FT ends the possession.
- A missed final FT ends the possession only when the opponent gains control or
  the period ends; an offensive rebound continues it.
- A missed field goal ends on the shot when followed, after transparent block
  annotations, by a defensive rebound, opponent control, or period end.
- Rebound control is determined first from rebound team versus shooting team;
  provider `O`/`D` labels remain evidence but do not override contradictory
  team ownership.
- An offensive rebound continues the possession.
- A turnover ends the possession once. Its offensive-foul and steal annotations
  do not create additional endpoints.
- Technical, bench, and coach-foul FTs normally do not create a new endpoint;
  the state machine must preserve the entitled inbound team.
- Unsportsmanlike and other retained-ball penalties use the recorded foul class
  and the next reliable live-ball owner to decide entitlement.
- `EP` and `EG` close unresolved end-of-period attempts, including blocked shots
  without a recorded rebound.

When the next reliable live-ball action contradicts the inferred owner, retain
the raw events, mark the incident for QA, and do not invent a second possession
to force alternation.

## Confidence and review

Each derived incident receives a percentage and review status:

- `confirmed` (98-100): deterministic action sequence and compatible score or
  next-possession evidence;
- `provisional` (85-97): one relationship depends on inferred possession
  entitlement;
- `unresolved` (<85): conflicting or insufficient evidence; manual review is
  required before publication.

Confidence describes the relationship reconstruction, not the reliability of
the raw event itself.

## Initial labelled fixtures

The machine-readable fixture is
`etl/euroleague/fixtures/event_grouping_edge_cases.csv`. It preserves the raw
package fields and adds only `expected_*`, `review_status`, and
`label_confidence_pct` labels.

It currently covers:

- a clock-shifted and-one;
- an and-one with substitutions before the FT;
- a four-point play;
- a bench technical after a made basket;
- separate personal and technical FT trips at the same clock;
- opposing ordinary FT trips at the same clock;
- interleaved personal and unsportsmanlike penalties;
- a coach technical whose FT is recorded at a later clock;
- a blocked shot followed by `AG`, `FV`, and a defensive rebound;
- a blocked shot closed by `EP` without a rebound;
- technical and personal FT trips with different shooters in one stoppage;
- offsetting technical fouls alongside an ordinary personal FT trip;
- a throw-in-foul FT followed by a separate ordinary FT trip;
- two retained-possession sequences that legitimately produce consecutive
  same-team possession endings;
- an offensive rebound immediately before the period-ending marker;
- a made basket followed by a dead-ball personal foul and FTs shot by a
  different teammate;
- a new personal foul that closes an earlier special-penalty FT trip even when
  the same player shoots both trips;
- an unsportsmanlike foul recorded as `CMU -> TO -> RV`, followed by retained
  ball after the FTs;
- a provider offensive-rebound row interleaved between a foul and its FTs.

Provenance: EuroLeague competition `E`, season 2025, retrieved through
`euroleague-api` 0.1.1 from the EuroLeague live play-by-play endpoint on
2026-08-05.

## Validation gates for an implementation

Before derived possessions are written to a database:

1. every FT has exactly one trip or an explicit unresolved flag;
2. every parent points to an action in the same game and period;
3. every incident has zero or one possession endpoints;
4. scoring actions reconcile to the official score progression;
5. game FGA, FTA, offensive rebounds, defensive rebounds, and turnovers
   reconcile to the box score or have documented provider exceptions;
6. inferred offensive-team transitions are checked for unexplained consecutive
   endings;
7. every provisional or unresolved relationship is retained in a QA report.

Passing the fixture tests establishes structural correctness only. Full-season
box-score and possession validation is a separate release gate.
