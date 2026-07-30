# Reports the size of the bookmark URL produced by the running app.
# Usage: open the app, set the heaviest filter state you can (Compare tab,
# both sides populated, many teams/players), then paste the JS block below
# into the browser console and record the number.
cat(
  "Run in the browser console with the app open:\n\n",
  'window.Shiny.addCustomMessageHandler("ibpl_bookmark_url", function(m) {\n',
  '  console.log("bookmark bytes:", new Blob([m.url]).size);\n',
  "});\n\n",
  "Record the worst-case size in docs/superpowers/plans/",
  "2026-07-29-idle-restore-bookmarking.md (Task 4).\n",
  sep = ""
)
