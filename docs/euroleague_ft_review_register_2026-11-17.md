# FT coverage-loss review register — 2026-11-17

Fixed original cohort: **163 FT rows / 113 candidate incidents / 101 games**. Current model recovers unique provisional candidates for **133** original rows; **30** still lack unique assignments.

The all-game replay has **0 new coverage losses outside this fixed cohort**.

Automatic categories below are triage suggestions. A recovered candidate is not a verified feed correction. The source-order windows, baseline parents, and complete baseline trip membership are preserved in ignored `euroleague/data/diagnostics/ft_stoppage_shadow/review/contexts.json`.

Original model SHA256: `584fb9257694fbc8fe7064dfd1cded000f06971d592bc8eb971748c053c9b4fd`. Current model SHA256: `bf19f0b3f065e39e9f98ecd2509bd0ee9fbba6514e7f6d3852ceadd7e461add3`.

## Category counts (original FT rows)

| Category | Original FT rows | Unique candidates now | Still open |
|---|---:|---:|---:|
| competing_personal_fouls | 39 | 31 | 8 |
| fouled_shooter_mismatch | 61 | 50 | 11 |
| made_basket_award_or_boundary | 51 | 49 | 2 |
| parent_outside_candidate_window | 1 | 0 | 1 |
| replacement_condition_unverified | 3 | 3 | 0 |
| shooter_award_or_scope | 5 | 0 | 5 |
| single_ft_personal_without_linked_basket | 3 | 0 | 3 |

## Evidence required

- **replacement_condition_unverified:** Verify injury, foul-out or disqualification permitting the observed shooter substitution.
- **competing_personal_fouls:** Review distinct foul counters and joint foul/RV assignments; retain competing FT allocations without additional evidence.
- **parent_outside_candidate_window:** Inspect the excluded baseline parent and intervening live events; establish the correct incident boundary.
- **made_basket_award_or_boundary:** Verify the basket/foul relation and actual award before treating the FT as an and-one.
- **shooter_award_or_scope:** Check shooter identity and completeness of the awarded attempts using PBP and available box-score evidence.
- **fouled_shooter_mismatch:** The FT shooter differs from the observed foul-drawn player, with no verified replacement condition. Verify attribution externally.
- **single_ft_personal_without_linked_basket:** A personal foul has one observed FT and no linked made basket. Establish whether the source omitted evidence, recorded a special award as CM, or an attempt is missing.

## Incident register

| Game | Period | Original FT source orders | Original status | Triage category | Unique candidates now |
|---|---:|---|---|---|---:|
| E2025_104 | 2 | 139 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_116 | 3 | 364, 365 | ambiguous | competing_personal_fouls | 2/2 |
| E2025_119 | 2 | 260 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_121 | 2 | 226 | no_fit | parent_outside_candidate_window | 0/1 |
| E2025_134 | 2 | 207, 208 | ambiguous | competing_personal_fouls | 2/2 |
| E2025_14 | 4 | 384, 385, 386 | ambiguous | competing_personal_fouls | 3/3 |
| E2025_149 | 3 | 397 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_150 | 2 | 180 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_158 | 3 | 324, 325 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_171 | 3 | 350 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_176 | 4 | 522, 523 | ambiguous | competing_personal_fouls | 2/2 |
| E2025_18 | 2 | 228 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_194 | 1 | 58 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_2 | 4 | 524 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_21 | 2 | 253, 254 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_215 | 2 | 217 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_221 | 5 | 622 | no_fit | single_ft_personal_without_linked_basket | 0/1 |
| E2025_226 | 2 | 191 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_227 | 3 | 312 | no_fit | fouled_shooter_mismatch | 0/1 |
| E2025_23 | 1 | 42, 44 | ambiguous | competing_personal_fouls | 0/2 |
| E2025_237 | 3 | 326 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_254 | 4 | 406, 407 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_254 | 4 | 428 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_274 | 2 | 225 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_275 | 4 | 599, 600 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_291 | 1 | 104 | no_fit | fouled_shooter_mismatch | 0/1 |
| E2025_292 | 2 | 208 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_295 | 3 | 286, 288 | ambiguous | competing_personal_fouls | 2/2 |
| E2025_295 | 4 | 444 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_301 | 1 | 85, 86 | ambiguous | competing_personal_fouls | 2/2 |
| E2025_305 | 4 | 574, 575 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_309 | 2 | 216 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_318 | 4 | 459, 460 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_322 | 4 | 420, 421 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_324 | 4 | 602, 603 | no_fit | fouled_shooter_mismatch | 0/2 |
| E2025_327 | 4 | 589, 590 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_329 | 2 | 150, 151 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_341 | 3 | 424 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_341 | 3 | 456, 457 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_344 | 2 | 162 | no_fit | fouled_shooter_mismatch | 1/1 |
| E2025_349 | 1 | 65 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_35 | 4 | 409 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_370 | 3 | 406, 407 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_371 | 1 | 135, 136 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_374 | 3 | 417, 418 | ambiguous | competing_personal_fouls | 2/2 |
| E2025_374 | 4 | 487, 488 | no_fit | fouled_shooter_mismatch | 0/2 |
| E2025_374 | 4 | 504 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_390 | 2 | 212 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_398 | 3 | 413 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_4 | 3 | 344, 346 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2025_71 | 4 | 571 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_8 | 2 | 336 | no_fit | single_ft_personal_without_linked_basket | 0/1 |
| E2025_82 | 3 | 332 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2025_98 | 3 | 375 | no_fit | shooter_award_or_scope | 0/1 |
| E2026_1 | 4 | 629, 630 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2026_24 | 3 | 457, 458 | ambiguous | competing_personal_fouls | 2/2 |
| E2026_27 | 3 | 359, 361 | ambiguous | competing_personal_fouls | 2/2 |
| E2026_31 | 2 | 238 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2026_31 | 3 | 369 | replacement_requires_evidence | replacement_condition_unverified | 1/1 |
| E2026_39 | 3 | 397 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2026_44 | 2 | 189 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2026_53 | 3 | 409 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2026_58 | 3 | 302 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2026_60 | 4 | 489, 491 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2026_63 | 2 | 224, 225 | ambiguous | competing_personal_fouls | 2/2 |
| E2026_67 | 1 | 94, 96 | ambiguous | competing_personal_fouls | 0/2 |
| E2026_67 | 2 | 258, 259 | no_fit | fouled_shooter_mismatch | 2/2 |
| E2026_78 | 4 | 467 | no_fit | made_basket_award_or_boundary | 1/1 |
| E2026_89 | 2 | 194 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_111 | 4 | 583 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_117 | 1 | 87 | no_fit | fouled_shooter_mismatch | 0/1 |
| U2025_119 | 3 | 337 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_140 | 2 | 181 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_165 | 2 | 184, 186 | no_fit | fouled_shooter_mismatch | 2/2 |
| U2025_172 | 1 | 59 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_176 | 4 | 552, 553 | no_fit | fouled_shooter_mismatch | 2/2 |
| U2025_187 | 3 | 418 | no_fit | fouled_shooter_mismatch | 1/1 |
| U2025_3 | 1 | 9, 10 | no_fit | fouled_shooter_mismatch | 0/2 |
| U2025_32 | 1 | 141, 142 | ambiguous | competing_personal_fouls | 2/2 |
| U2025_35 | 4 | 519 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_4 | 2 | 351 | no_fit | fouled_shooter_mismatch | 1/1 |
| U2025_44 | 4 | 530 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_49 | 2 | 136 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_5 | 3 | 335 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_66 | 3 | 370 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_73 | 3 | 419, 420, 421 | no_fit | fouled_shooter_mismatch | 3/3 |
| U2025_79 | 1 | 55 | replacement_requires_evidence | replacement_condition_unverified | 1/1 |
| U2025_86 | 4 | 450 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2025_86 | 4 | 465 | no_fit | shooter_award_or_scope | 0/1 |
| U2025_9 | 5 | 605 | no_fit | single_ft_personal_without_linked_basket | 0/1 |
| U2025_98 | 1 | 54 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2026_100 | 3 | 390 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2026_108 | 4 | 551, 552 | no_fit | fouled_shooter_mismatch | 2/2 |
| U2026_109 | 2 | 134 | replacement_requires_evidence | replacement_condition_unverified | 1/1 |
| U2026_16 | 1 | 90 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2026_17 | 2 | 278, 279 | no_fit | fouled_shooter_mismatch | 2/2 |
| U2026_19 | 2 | 197, 198 | ambiguous | competing_personal_fouls | 0/2 |
| U2026_29 | 3 | 321, 322 | ambiguous | competing_personal_fouls | 2/2 |
| U2026_4 | 4 | 518 | no_fit | made_basket_award_or_boundary | 0/1 |
| U2026_47 | 4 | 573, 574 | ambiguous | competing_personal_fouls | 2/2 |
| U2026_5 | 4 | 627, 628 | no_fit | fouled_shooter_mismatch | 2/2 |
| U2026_50 | 4 | 501 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2026_56 | 2 | 233, 234 | ambiguous | competing_personal_fouls | 0/2 |
| U2026_56 | 4 | 459, 464, 465 | no_fit | shooter_award_or_scope | 0/3 |
| U2026_6 | 1 | 25, 26 | ambiguous | competing_personal_fouls | 2/2 |
| U2026_6 | 3 | 409 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2026_6 | 3 | 428 | no_fit | made_basket_award_or_boundary | 0/1 |
| U2026_60 | 4 | 470 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2026_61 | 4 | 530, 531 | no_fit | fouled_shooter_mismatch | 2/2 |
| U2026_62 | 2 | 207 | no_fit | made_basket_award_or_boundary | 1/1 |
| U2026_62 | 2 | 258, 259 | ambiguous | competing_personal_fouls | 2/2 |
| U2026_65 | 1 | 74, 75 | no_fit | fouled_shooter_mismatch | 0/2 |
| U2026_7 | 2 | 163 | no_fit | made_basket_award_or_boundary | 1/1 |

## Reproduce

```powershell
& euroleague/.venv/Scripts/python.exe euroleague/scripts/compare_ft_stoppages.py
& euroleague/.venv/Scripts/python.exe euroleague/scripts/review_ft_stoppages.py
```

The checked-in seed fixes cohort identity across changes. The full comparison must also be checked for new coverage losses outside this cohort. Production and the canonical parser remain unchanged.
