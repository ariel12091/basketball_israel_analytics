# Chat unit and season fixes — 2026-10-31

Implemented the metadata and verification fixes locally and in a test Preview. The six-successful-call limit stays unchanged. No production promotion, database change, UI/layout work or commit was performed.

## Result

The first sixteen paired questions improve from **7/16 to 14/16 good-or-minor** under the same AI-assisted rubric. Changes: {"better":9,"same":7}. Rating-unit errors fall from 6 cases to 0; normalization problems fall from 3 to 1. The first candidate still produced a contradictory unit in one Hebrew answer; it is preserved in the report, not replaced with a later answer.

That Hebrew failure exposed rating_basis on Compare Players despite those rows being box-score production. Final code removes that misleading metadata and offers only the actual normalization. Six focused API checks then exercised the final runtime, including the previously failing Hebrew case, explicit 2025-26, missing 2024-25 data, and a legitimate rating-versus-raw-ratio explanation. All six preserve the targeted unit/season meaning. Two more checks verify the final capability-description correction. No terminal failures or fallbacks occurred in these follow-up checks. This is targeted evidence, not a general correctness guarantee.

## Implementation

- display.ts resolves On/Off on-court/off-court labels to the canonical per 100 definitions. Page cell values and formatting are unchanged.
- run.ts supplies quotable applied-season, rating-unit and production-normalization metadata. Compare Players gets its actual per 30/per 60 basis and never a rating basis.
- markers.ts and semantics.ts reject demonstrated wrong-unit statements and missing normalized scales. Correct digit/word unit phrases are allowed without permitting invented scoring totals. The checker supports an explicit negation or contrast with a true raw ratio, and catches a wrong rating definition placed after the numeric sentence.
- turn.ts validates seasonal tool arguments before lookup. Current defaults are per league; recognized explicit ranges override them, historical requests with an omitted year are filled, comparisons may use both years, and follow-ups inherit prior scope unless the visitor specifies current scope. Contradictory arguments return actionable errors without spending a data lookup. Historical history reruns keep their original scope.
- The prompt's analytical rules remain the previously retained v1. One obsolete capability sentence is corrected: it no longer claims rate_basis is unavailable. The initial paired comparison held the entire v1 prompt constant; this final description correction is checked separately.

Runtime files: web/src/ask/display.ts, markers.ts, run.ts, turn.ts, semantics.ts; prompt.ts has only the capability-description correction relative to v1. Tests are in semantics.test.ts, turn.test.ts and run.test.ts. Existing unrelated workspace changes are preserved.

## Evidence and tool use

| Measure | Control /16 | Initial fixes /16 |
|---|---:|---:|
| Final answers | 16 | 16 |
| Successful calls | 37 | 39 |
| Calls per final | 2.31 | 2.44 |
| Tool errors / rejected arguments | 2 | 5 |
| Rejected drafts | 8 | 7 |
| Median / p95 time | 13.6s / 18.7s | 13.3s / 21.1s |
| Reported model cost | $0.0457 | $0.0464 |

Control tool types: {"onoff":13,"list_teams":10,"player_stats":4,"lineup_roster":1,"compare":4,"team_ratings":4,"guide":1}. Initial fixed tool types: {"onoff":15,"list_teams":9,"lineup_roster":2,"compare":6,"player_stats":1,"team_ratings":5,"guide":1}. Input/output tokens: 882327/26708 versus 918759/26618. Reported cost/tokens cover retained final answers; latency includes CLI/network overhead. Rejected arguments are not successful data calls and do not consume the six-call allowance.

Successful data reads with a season/league outside the expected case scope: control 8, initial fixes 0. Some control reads are explicitly labeled discovery rather than a wrong final season. The structured artifact lists every call. Initial fixes reject 5 conflicting season arguments before retrieval. Final checks also preserve the explicit historical requests, including the empty 2024 dataset, without substituting current data.

Observable examples:

- Dawson, Burns and Zoosman retain per 100 values instead of points per possession. Hall can use unambiguous rating labels when a phrasing retry occurs.
- The Moshkovits comparison retains per 30 units. The first Hebrew metadata draft fails; the final Hebrew check reports 18.3 versus 5.7 points per 30 minutes with no rejected drafts.
- Wrong EuroCup 2025 calls for this season are rejected, followed by current 2026 reads. Explicit EuroLeague 2025-26 remains 2025; empty 2024-25 is reported as unavailable, not silently replaced.
- The Knight question still inherits hidden possession minima beyond the requested 20-mpg definition. Its units improve, but the ranking population remains wrong. This is a separate unresolved reader/filter contract, not evidence that a larger call budget is needed.

## Validation and limitations

- **200 tests passed** across Ask and its API route; TypeScript typecheck passed. Both refined Preview builds passed. Tests cover all three leagues, English/Hebrew unit phrasing, per 30/per 60 bases, a missing-scale failure, rejected wrong-season arguments, historical ranges, follow-up scope, and preserving numeric provenance.
- **40 API turns total:** 32 paired turns on the initial implementation, six final-runtime checks, and two final capability-description checks. Requests alternate control/candidate within the paired run, use fresh conversations and retain valid bad answers and terminal errors. The last eight are targeted follow-ups, not a new fully paired accuracy estimate.
- The first candidate remains immutable at 9ngvjq7ir; refined runtime at 7sgro5h47; exact final code/description is at the Preview in the release manifest. Do not use the earlier candidate for deployment. The final source hashes are verified against the workspace.
- Paired data versions: control 2026-10-29 22:49:47; 2026-10-29 22:52:08.886863+00, initial fixes 2026-10-29 22:49:47; 2026-10-29 22:52:08.886863+00. The last two checks run after a data refresh (2026-10-30 22:30:36.31026+00; 2026-10-30 22:25:00); they check units and season protection against their returned cells, not equality with old reference values.
- The reviewer is the same AI assistant, unblinded, on curated questions. No independent human review, statistical significance, representative traffic accuracy, or hidden chain-of-thought access is claimed.
- Semantic checks target demonstrated forms of prose; they are not a complete natural-language type system. Season protection handles recognized ranges, native-year forms and current/previous/follow-up requests; arbitrary ambiguous historical wording is not comprehensively parsed.
- Sample confidence, causal wording, hidden eligibility, and player-name discovery can still be wrong. These fixes improve unit/scope contracts; they do not certify every analytical claim. Mobile/desktop receive the same API behavior; no layout or interaction code changed and no UI redesign was tested.

## Recommendation

Keep these targeted fixes for review. Keep six calls and avoid another broad prompt rewrite. Before a full release, separately address the remaining explicit-eligibility mismatch and global English/Hebrew player-name resolution if those questions are a priority. More calls alone would not repair either the unit contract or a silently narrowed ranking population.

- [All paired answers and final checks](../../output/ask/chat-semantics-2026-10-30/comparisons.md)
- [Structured evidence and metrics](../../output/ask/chat-semantics-2026-10-30/comparison.json)
- [Final deployment and source hashes](../../output/ask/chat-semantics-release-2026-10-30/manifest.json)
- [Prior prompt-only recalibration](prompt_recalibration_2026-10-30.md)
