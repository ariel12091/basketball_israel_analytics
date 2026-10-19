# Stats layout discussion — paused 2026-10-07

The user asked whether the stats pages feel crowded between the navigation and
tables, then requested a mockup. After reviewing it, the user said:

> i think it's a bit too far, and also somewhat boring. let's save it to memory and continue sometime next

Stop design work here. The mockup is not approved for implementation.

## Direction for the next session

- Prefer small changes to the existing app over a broad layout redesign.
- Preserve its visual character and useful statistical detail. The proposed
  simplified layout felt too different and too boring to the user.
- The original concern remains the crowded space between navigation and the
  stats tables: explanation boxes, example links, view controls, legends,
  filter chips, and minimum-possession controls.
- Revisit specific changes with the user before implementing the redesign.
  Combining explanation/example content is a candidate, not an approved change.
- Keep desktop filters shown by default, with a way to hide the entire sidebar
  and let results use its width. Preserve selected filters while hidden.
- Check phone/tablet layouts and Israeli league, EuroLeague, and EuroCup parity.

## Mockup artifacts

- Interactive standalone prototype: `output/mockups/stats-layout.html`.
- Optional preview server: `node output/mockups/serve.cjs`, serving only this
  mockup at `http://127.0.0.1:3880`. Do not assume that server is still running.
- Screenshots: `output/mockups/stats-desktop-filters.png`,
  `stats-desktop-focus.png`, `stats-desktop-about.png`, `stats-phone-focus.png`,
  `stats-phone-scrolled.png`, and `stats-phone-filters.png`.
- It uses clearly marked illustrative data. View buttons demonstrate selection,
  not the actual alternative stats tables. The app was not changed by the mockup.
- The prototype combines view controls, minimum-possession inputs, and the
  sidebar toggle into a toolbar; moves explanations/example content into an
  "About this view" disclosure; and shows chips only for selected filters.
- Browser checks passed for default-expanded desktop filters, reclaiming the
  full sidebar width, retaining selections, help disclosure, phone layout, and
  full player/team labels during horizontal table scrolling.

## Existing implementation status

- Full-name labels during horizontal scrolling were approved and deployed to
  production `main` as `d29e76a`. Original feature commit: `20e77aa` on
  `infra/intro-video`.
- Production is Posit Connect Cloud and deploys from GitHub `main`, not the
  feature branch or the obsolete shinyapps.io metadata.
- Separately, desktop filter-toggle improvements are still local and uncommitted:
  explicit "Hide filters" / "Show filters" labels in `app/R/global.R` and
  `app/www/app.js`; shared toggle added to `app/R/ui_tab6_team_stats.R` and
  `app/R/ui_tab7_compare.R`; corresponding existing test updated in
  `app/tests/testthat/test-filter-collapse.R`.
- Existing CSS already hides the whole sidebar and expands the results column;
  browser measurements confirmed 314px sidebar -> 0px and 942px results -> 1256px.
- Desktop checks across all three leagues passed, including preserved selections
  and keyboard reopening. Phone/tablet inline filter checks across all leagues
  also passed after allowing Bootstrap collapse animations to finish. Focused
  R tests, JS syntax, and the feature diff checks passed.
- Do not describe these local toggle improvements as deployed. Do not commit or
  publish unrelated dirty files when resuming; the workspace has substantial
  pre-existing work outside this feature.
