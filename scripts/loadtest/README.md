# Deployed Shiny load test

This read-only harness targets the public Posit Connect app with independent
real-browser sessions. Run the 5/10/20-user by 10/30/60-second control matrix:

   ```powershell
   powershell -ExecutionPolicy Bypass -File scripts/loadtest/run_browser_matrix.ps1
   ```

Every browser session chooses its own seeded random tab path. Think time is
jittered independently to 65%-135% of the target cadence, and session starts
are staggered over two seconds. Each action records click-to-Shiny-idle latency;
the result also captures navigation time, HTTP errors, request failures, and
browser console errors. Seeds and per-user paths are saved for reproducibility.
Generated recordings and run output live under `output/loadtest/` and are
ignored by Git.

For launch-like mixed traffic, run all three cadence classes in the same
population. Cadence assignments are balanced, shuffled, and saved:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/loadtest/run_mixed_browser.ps1
```

The browser runner is used instead of protocol replay because this app can
return independent asynchronous Shiny outputs in different valid orders.
Protocol-order assertions turn that behavior into false playback failures.
