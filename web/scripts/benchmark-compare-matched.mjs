// Local, offline component experiment. Both servers must use recorded data.
// No production endpoints, browser/session simulation or database capacity claim.
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { performance } from 'node:perf_hooks';
import { isDeepStrictEqual } from 'node:util';
import { createHash } from 'node:crypto';
import { cpus, totalmem, platform, release } from 'node:os';
const golden = JSON.parse(readFileSync('test/goldens/compare/rules.json','utf8'));
const bases = { typescript: 'http://127.0.0.1:3191', r: 'http://127.0.0.1:3192',
  typescript_blocking_control: 'http://127.0.0.1:3191' };
const cases = Object.keys(golden.cases);
const report = {
  timestamp: new Date().toISOString(),
  scope: 'Offline original Compare calculations + controlled I/O; excludes full apps, browsers, SQL, caches, pg admission and hosting',
  environment: {node:process.version,os:platform(),release:release(),
    cpu:cpus()[0].model,logicalCpus:cpus().length,memoryBytes:totalmem()},
  sources: Object.fromEntries(['test/goldens/compare/rules.json',
    'src/rules/compare.ts','../app/R/server_tab7_compare.R',
    'scripts/benchmark-compare-runtime.ts','scripts/benchmark-compare-matched.mjs',
    '../scripts/benchmark_compare_runtime.R','../scripts/compare_r_sources.R',
    '../app/tests/goldens/out/compare_rules.rds'].map(path=>[path,
      createHash('sha256').update(readFileSync(path)).digest('hex')])),
  workload: { cases, readsPerAction: 2, delaysMs: [0,40], concurrency: [1,10,50],
    requestsPerStage: 50, trials: 3, timeoutMs: 30000, thinkTimeMs: 0,
    note: 'Closed loop; same deterministic case order (index*37 modulo case count). R reads sequential; TS reads parallel. A blocking-TS control isolates event-loop waiting with identical TS calculations. Fixtures preloaded; no response cache; no DB admission limit.' },
  correctness: {}, runs: []
};
function equivalent(a,b) {
  if (typeof a === 'number' && typeof b === 'number')
    return Math.abs(a-b) <= 1e-8 * Math.max(1,Math.abs(a),Math.abs(b));
  if (Array.isArray(a) && Array.isArray(b))
    return a.length === b.length && a.every((v,i) => equivalent(v,b[i]));
  if (a && b && typeof a === 'object' && typeof b === 'object')
    return isDeepStrictEqual(Object.keys(a).sort(),Object.keys(b).sort()) &&
      Object.keys(a).every(k=>equivalent(a[k],b[k]));
  return a === b;
}
async function request(runtime,id,delay) {
  const start = performance.now();
  try {
    const res = await fetch(`${bases[runtime]}/compare?case=${encodeURIComponent(id)}&delay=${delay}&blocking=${runtime==='typescript_blocking_control'}`,
      { signal: AbortSignal.timeout(30000) });
    const bytes = await res.arrayBuffer();
    const body = JSON.parse(new TextDecoder().decode(bytes));
    const valid = res.ok && body.case === id && equivalent(body.rows,golden.cases[id].expected);
    return { case:id, ms:performance.now()-start, status:res.status, valid,
      bytes:bytes.byteLength, ...(!valid ? {error:body.error ?? 'Oracle mismatch'} : {}) };
  } catch(e) { return {case:id,ms:performance.now()-start,status:0,valid:false,bytes:0,error:String(e)}; }
}
mkdirSync('../output/scalability',{recursive:true});
const save=()=>writeFileSync('../output/scalability/compare-runtime.json',JSON.stringify(report,null,2));
for (const runtime of Object.keys(bases)) {
  report.environment[runtime] = await (await fetch(`${bases[runtime]}/health`)).json();
  const checks=[];
  for (const id of cases) checks.push(await request(runtime,id,0));
  report.correctness[runtime]=checks;
  save();
  if (checks.some(r=>!r.valid)) throw Error(`${runtime}: correctness check failed; see raw report`);
}
for (const delay of report.workload.delaysMs)
  for (const concurrency of report.workload.concurrency)
    for (let trial=1;trial<=3;trial++)
      // Alternate runtime order to reduce systematic warmup/time bias.
      for (const runtime of trial%2 ? ['r','typescript','typescript_blocking_control'] : ['typescript_blocking_control','typescript','r']) {
        let next=0;
        const samples=[];
        const start=performance.now();
        await Promise.all(Array.from({length:concurrency},async()=>{
          while (next<50) { const i=next++; samples.push(await request(runtime,cases[(i*37)%cases.length],delay)); }
        }));
        const seconds=(performance.now()-start)/1000;
        const times=samples.map(r=>r.ms).sort((a,b)=>a-b);
        const run={ runtime, delayMs:delay, concurrency, trial, count:samples.length,
          failures:samples.filter(r=>!r.valid).length, seconds, requestsPerSecond:samples.length/seconds,
          p50Ms:times[Math.ceil(times.length*.5)-1],p95Ms:times[Math.ceil(times.length*.95)-1],maxMs:times.at(-1), samples };
        report.runs.push(run);save();
        console.log(JSON.stringify({...run,samples:undefined}));
      }
if (report.runs.some(r=>r.failures)) process.exitCode=1;
