import {describe,it,expect} from 'vitest';
import {missingCoverage,playwrightRuns,type Scope,type Run} from './gate';
const scope:Scope={required:['GL-01'],deferred:{},leagues:['il'],widths:['phone','desktop']};
const run=(project:string,status='passed'):Run=>({title:'[GL-01] [league:il] screen',project,status});
describe('executed coverage',()=>{
 it('requires both widths',()=>expect(missingCoverage(scope,[run('desktop')])).toEqual(['GL-01/il/phone']));
 it('rejects skipped and failed tests',()=>{
  expect(missingCoverage(scope,[run('phone','skipped'),run('desktop')])).toEqual(['GL-01/il/phone']);
  expect(missingCoverage(scope,[run('phone','failed'),run('desktop')])).toHaveLength(1);
 });
 it('never completes deferred features',()=>{
  expect(missingCoverage({...scope,deferred:{'GL-01':'later'}},[run('phone'),run('desktop')])).toEqual(['GL-01: required but deferred']);
  expect(missingCoverage(scope,[{...run('phone'),title:'[GL-01] [league:il] [deferred:phase6]'},run('desktop')])).toHaveLength(1);
 });
 it('accepts passing actual runs',()=>expect(missingCoverage(scope,[run('phone'),run('desktop')])).toEqual([]));
 it('does not hide a failed retry',()=>{
  const runs=playwrightRuns({suites:[{specs:[{title:'[GL-01] [league:il]',tests:[{projectName:'phone',results:[{status:'failed'},{status:'passed'}]}]}]}]});
  expect(runs[0].status).toBe('failed-or-skipped');
 });
});
