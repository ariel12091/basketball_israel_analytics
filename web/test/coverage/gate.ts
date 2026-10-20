import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

export type Run = { title: string; project: string; status: string };
export type Scope = { required: string[]; deferred: Record<string,string>; leagues: string[]; widths: string[] };
type Suite = { title?: string; suites?: Suite[]; specs?: {title:string;tests:{projectName:string;results:{status:string}[]}[]}[] };

export function playwrightRuns(report: {suites:Suite[]}): Run[] {
  const out:Run[]=[];
  const walk=(suite:Suite, parents:string[])=>{
    const prefix=[...parents,suite.title ?? ''];
    for(const spec of suite.specs ?? []) for(const test of spec.tests) {
      // A flaky retry cannot conceal a failed required run.
      const status=test.results.length>0 && test.results.every(r=>r.status==='passed') ? 'passed' : 'failed-or-skipped';
      out.push({title:[...prefix,spec.title].join(' '),project:test.projectName,status});
    }
    for(const child of suite.suites ?? []) walk(child,prefix);
  };
  for(const suite of report.suites ?? []) walk(suite,[]);
  return out;
}

export function missingCoverage(scope:Scope,runs:Run[]):string[] {
  const missing:string[]=[];
  for(const id of scope.required) {
    if(scope.deferred[id]) {missing.push(`${id}: required but deferred`);continue;}
    for(const league of scope.leagues) for(const project of scope.widths) {
      const claimed=runs.filter(r=>r.title.includes(`[${id}]`) &&
        r.title.includes(`[league:${league}]`) && r.project===project && !r.title.includes('[deferred:'));
      if(!claimed.length || claimed.some(r=>r.status!=='passed')) missing.push(`${id}/${league}/${project}`);
    }
  }
  return missing;
}

export function main(args=process.argv.slice(2)):void {
  const scope:Scope=JSON.parse(readFileSync(args[0] ?? 'test/coverage/scope.json','utf8'));
  const report=JSON.parse(readFileSync(args[1] ?? 'test-results/playwright.json','utf8'));
  const missing=missingCoverage(scope,playwrightRuns(report));
  if(missing.length) throw new Error(`Missing executed coverage: ${missing.join(', ')}`);
  console.log(`Coverage passed: ${scope.required.join(', ')}; deferred: ${Object.keys(scope.deferred).join(', ')}`);
}
if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href) main();
