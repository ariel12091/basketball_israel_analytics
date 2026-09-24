# iPad navigation handoff — 2026-09-24

## Request and reproduction

The user reported that the live app's navigation menu malfunctions on an iPad and that the pop-up menu is hard to follow when accessing Traditional Stats. Live app: `https://arieltaieb-basketball-israel-analytics.share.connect.posit.cloud/`.

Using Playwright CLI with `--device "iPad Pro 11" --headed` (Chromium device emulation, 834 CSS px wide), the live app's hamburger button was visible but untappable. Playwright's click timed out because `#last_updated` inside the fixed `#navbar_right_cluster` intercepted pointer events. Geometry showed the cluster at x=263–824 and the menu button at x=756–812. At 834 px, the navbar collapses, but `mobile.js` only enabled its mobile navigation relocation below 768 px.

Traditional is under **Team Ratings**, not Player Stats. The route is hamburger → Team Ratings → Traditional. Before the local change, the submenu used the desktop hover treatment at iPad width, which makes touch navigation difficult.

## Local changes (not deployed)

- `app/www/mobile.js`: Added an `ibpl-collapsed-nav` body class and moved the right header cluster into the collapsed navbar at widths below 992 px. The existing phone-only `ibpl-mobile` behavior remains below 768 px.
- `app/www/mobile.css`: Applied the in-flow cluster and active-tab submenu styles to `ibpl-collapsed-nav`, so tablet navigation shows the active tab's view choices as tappable rows in the open menu. Removed reserved desktop right padding from the collapsed tab list.
- After the initial iPad check, increased collapsed navigation links and view choices to 48 px tap rows and laid out league/season controls in full-width grid columns. This latest spacing adjustment still needs a browser check.

The changes are limited to those two files. The repository already had many unrelated modified and untracked files; do not include them in any commit.

## Verification so far

- `node --check app/www/mobile.js` passed.
- In the local app at `http://127.0.0.1:7666/`, the iPad menu button became tappable after the first change. The menu opened and the league/season controls appeared inside it.
- Tapping Team Ratings exposed Summary, Four Factors, Shot Profile, and Traditional; tapping Traditional set `input[name=tr_view_mode]:checked` to `Traditional`.
- After extending the submenu CSS, the Shiny app was restarted and the iPad emulator reloaded. The refreshed page reported no console errors at load, and the menu reopened. **The final submenu layout after this restart still needs a visual check.**
- Phone width and EuroLeague/EuroCup navigation still need verification. Return the visible browser to iPad size afterward.

## Separate issue noticed

The local Team Ratings Traditional panel showed `Error: missing value where TRUE/FALSE needed` at the default 26–27 season. Local R logs reported a `tab3` `tr_params` error during initial server setup, before Traditional was selected. This appears separate from the menu overlap and was not investigated or changed. Check the live app and server logic before claiming the Traditional data view works.

## Running tools

- Local Shiny server: `Rscript.exe -e "shiny::runApp('app', host='127.0.0.1', port=7666, launch.browser=FALSE)"` (launched in an escalated `exec_command` session; last session id was `70716`).
- Visible Playwright CLI browser has two tabs: live app (tab 0) and local app (tab 1, current), opened with iPad Pro 11 device emulation. CLI binary: `C:/Users/ariel/AppData/Local/npm-cache/_npx/31e32ef8478fbf80/node_modules/.bin/playwright-cli.cmd`. Browser commands required `sandbox_permissions: "require_escalated"` to reach the same visible session.
- WebKit was not installed, so this is Chromium iPad device emulation, not Safari.

## Next steps

1. Confirm the refreshed local Team Ratings submenu is visibly in flow and Traditional can be tapped again.
2. Verify hamburger and active submenu at a phone width and at iPad width for the EuroLeague view; verify desktop header has no regression.
3. Investigate the separate Traditional data error if it also occurs live.
4. Keep the emulator open for the user. The CSS/JS fix is local and needs deployment before the live app changes.
