import { readFileSync, writeFileSync } from 'node:fs';
const rawPath='../output/scalability/compare-runtime.json';
const report=JSON.parse(readFileSync(rawPath,'utf8'));
if (report.runs.length !== 54) throw Error('Expected all 54 stages; experiment is incomplete');
const runtimeNames={r:'Original R functions',typescript:'TypeScript async',
  typescript_blocking_control:'TypeScript blocking control'};
const summaries=[];
for (const delay of report.workload.delaysMs)
  for (const concurrency of report.workload.concurrency)
    for (const runtime of Object.keys(runtimeNames)) {
      const trials=report.runs.filter(r=>r.delayMs===delay && r.concurrency===concurrency && r.runtime===runtime);
      if (trials.length !== 3) throw Error('Missing trial');
      const samples=trials.flatMap(t=>t.samples);
      const times=samples.map(s=>s.ms).sort((a,b)=>a-b);
      summaries.push({runtime,delay,concurrency,requests:samples.length,
        failures:samples.filter(s=>!s.valid).length,
        throughput:samples.length/trials.reduce((s,t)=>s+t.seconds,0),
        p95:times[Math.ceil(times.length*.95)-1],max:times.at(-1)});
    }
const checks=Object.values(report.correctness).flat();
const total=report.runs.reduce((s,r)=>s+r.count,0);
const failures=report.runs.reduce((s,r)=>s+r.failures,0);
let md=`# Local Compare runtime experiment — ${report.timestamp.slice(0,10)}

The original 2026-10-23 checkpoint is archived under docs/verification/:
compare-runtime-2026-10-23.json and benchmark-compare-matched.measured.txt.
Future reruns write to output/scalability/ and do not replace that archive.

Async waiting improves concurrent delivery in this controlled experiment.
**This does not prove that the React/Next application supports more users than
the Shiny application.** In the no-wait mixed calculation workload, the original
R component has higher measured throughput than the TypeScript component.

## What ran

The original R Compare join/finalization functions and production TypeScript
Compare rules consumed the same recorded Shiny frames. All 98 cases passed
full output checks in each of three variants (${checks.length} checks;
${checks.filter(r=>!r.valid).length} failures).
The HTTP wrappers were httpuv and Node HTTP, **not the full Shiny and Next apps**.
The third variant uses identical TypeScript calculations with blocking waits
as an experimental control; the product never uses that blocking code.

Each action reads A and B. R executes them sequentially, as the current Compare
code does; async TypeScript overlaps them. Each read either returns immediately
or waits a configured 40 ms. These are synthetic waits, not measured SQL times.
Timer precision, GC and OS scheduling affect actual durations.

Each variant ran three trials at 1, 10 and 50 concurrent HTTP requests, with
50 actions per trial. The same deterministic spread of 50 of the 98 cases
covered Teams/Lineups, summary/Four Factors, metrics and gap directions.
The all-case correctness gate was run separately. Across measured stages:
**${total} actions, ${failures} failures**. Every action's output was checked against
the R oracle. Percentiles include failures if any; timeouts are censored at
30 seconds. The generator uses closed-loop concurrency and no think time.

Fixtures were preloaded; there was no response cache, database, connection
pool, admission limit or browser. Variants ran separately, alternating order
between trials, on the same Windows machine and local loopback network.
These short trials are mechanism measurements, not a sustained capacity test.

## Results

Each table cell pools three trials (150 actions). Throughput is total actions
divided by summed trial wall time. p95 uses the nearest-rank pooled latency.

`;
for (const delay of report.workload.delaysMs) {
  md+=`### ${delay} ms configured wait per side\n\n`;
  md+='| Concurrent requests | Component | Actions/sec | p95 ms | Maximum ms | Failures |\n|---:|---|---:|---:|---:|---:|\n';
  for (const s of summaries.filter(s=>s.delay===delay))
    md+=`| ${s.concurrency} | ${runtimeNames[s.runtime]} | ${s.throughput.toFixed(1)} | ${s.p95.toFixed(1)} | ${s.max.toFixed(1)} | ${s.failures} |\n`;
  md+='\n';
}
md+=`## Interpretation and remaining evidence

The blocking TypeScript control separates the scheduling effect from differences
between the R and TypeScript calculations. Overlapping waits can keep one
process responsive while independent reads are outstanding. This does not
make CPU calculations parallel or reduce database work.

The React application code admits only two active database snapshots and twenty
queued requests per process, with a 500 ms queue wait. This experiment bypasses
those limits. Its 50 concurrent requests must **not** be interpreted as 50
supported database users. Initial React SSR also waits for references and its
paired answer; this experiment does not measure that path. Shiny already has
process-level caching, which is excluded here for both implementations.

No cold/warm application comparison, browser readiness, SQL contention, fleet
connection budget, CDN behavior, deployment cost or 150-user acceptance result
is established. The next database-backed run is specified in
[matched_local_load_protocol.md](matched_local_load_protocol.md) and requires
the project's quiet window. No production database traffic, push or deployment
was performed for this offline experiment.

## Reproduce

From repository root, regenerate the ignored JSON fixture if missing:

\`\`\`powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/export_compare_rule_goldens.R --replay
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/benchmark_compare_runtime.R
\`\`\`

Leave that local server running. In another terminal, from web/:

\`\`\`powershell
node node_modules/tsx/dist/cli.mjs scripts/benchmark-compare-runtime.ts
\`\`\`

In a third terminal, from web/:

\`\`\`powershell
node scripts/benchmark-compare-matched.mjs
node scripts/summarize-compare-runtime.mjs
\`\`\`

Stop both component servers with Ctrl+C afterwards. Ports 3191/3192 bind only
to loopback. The application on 3100 is separate and is not used by this test.

Raw per-request results, correctness checks, hardware/runtime metadata and
source/fixture SHA-256 hashes: output/scalability/compare-runtime.json (ignored).
Keep that file to audit this run; rerunning overwrites it. Source hashes cover
the named key inputs, not a complete dependency/build manifest.
The measured driver is archived as
output/scalability/benchmark-compare-matched.measured.mjs (ignored); the source
driver subsequently received a logging-only unused-variable lint cleanup.

Machine: ${report.environment.cpu}; ${report.environment.logicalCpus} logical
CPUs; ${(report.environment.memoryBytes/1024**3).toFixed(1)} GiB RAM;
${report.environment.os} ${report.environment.release}; Node ${report.environment.node};
${report.environment.r.r}; httpuv ${report.environment.r.httpuv};
Shiny ${report.environment.r.shiny}; dplyr ${report.environment.r.dplyr}.
`;
writeFileSync('../docs/compare_runtime_results_2026-10-23.md',md);
console.log(JSON.stringify({total,failures,correctnessChecks:checks.length,summaries},null,2));
