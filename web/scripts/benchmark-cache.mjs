// Local production-server probe only. This measures warm API delivery, not
// CDN scaling, cold database capacity, browser readiness, or Shiny throughput.
import { writeFileSync, mkdirSync } from "node:fs";
import { performance } from "node:perf_hooks";
const base = process.argv[2] ?? "http://localhost:3100";
if (!["localhost", "127.0.0.1"].includes(new URL(base).hostname))
  throw new Error("This bounded probe only targets a local server");
const paths = [
  "/api/v1/il/gameflow?game_id=390&team_id=2",
  "/api/v1/euroleague/gameflow?game_id=781&team_id=8",
];
async function request(path) {
  const start = performance.now();
  try {
    const res = await fetch(new URL(path, base), {signal:AbortSignal.timeout(30000)});
    const bytes = await res.arrayBuffer();
    const payload = JSON.parse(new TextDecoder().decode(bytes));
    return { ms:performance.now()-start,status:res.status,bytes:bytes.byteLength,
      valid:res.ok && payload.bars?.length>0 && payload.meta?.version != null };
  } catch(e) { return {ms:performance.now()-start,status:0,bytes:0,valid:false,error:e.name}; }
}
const report = {timestamp:new Date().toISOString(),base,
  scope:"Warm local production API delivery; no Shiny comparison or CDN/database-miss capacity claim",
  warmups:[],runs:[]};
for(const path of paths) {
  const result=await request(path);
  report.warmups.push({path,...result});
  if(!result.valid) throw new Error(`Warmup failed (${result.status}): ${path}`);
}
for(const concurrency of [1,10,50,150]) {
  const count=300,results=[];let next=0;
  const start=performance.now();
  await Promise.all(Array.from({length:concurrency},async()=>{
    while(next<count) {
      const i=next++;
      results.push(await request(paths[i%paths.length]));
    }
  }));
  const seconds=(performance.now()-start)/1000;
  const times=results.map(r=>r.ms).sort((a,b)=>a-b);
  const percentile=p=>times[Math.ceil(times.length*p)-1];
  const run={concurrency,requests:count,successes:results.filter(r=>r.valid).length,
    failures:results.filter(r=>!r.valid).length,seconds,
    requestsPerSecond:count/seconds,p50Ms:percentile(.5),p95Ms:percentile(.95),maxMs:times.at(-1),
    bytes:results.reduce((sum,r)=>sum+r.bytes,0)};
  report.runs.push(run);console.log(JSON.stringify(run));
}
mkdirSync("../output/scalability",{recursive:true});
writeFileSync("../output/scalability/warm-cache.json",JSON.stringify(report,null,2));
if(report.runs.some(r=>r.failures)) process.exitCode=1;
