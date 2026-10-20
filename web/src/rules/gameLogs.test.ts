import {describe,it,expect} from 'vitest';
import {readFileSync,readdirSync} from 'node:fs';
import {gameLogs,percentRank,type GameLogParams,type SeasonInput,type Row} from './gameLogs';
import {rRound} from './round';
describe('R parity',()=>{
 it('rounds ties to even',()=>{expect([.5,1.5,2.5,-.5].map(x=>rRound(x))).toEqual([0,2,2,0]);});
 it('percent rank preserves missing values',()=>expect(percentRank([3,1,null,3,2])).toEqual([2/3,0,null,2/3,1/3]));
 for(const file of readdirSync('test/goldens/game_logs').filter(x=>x.endsWith('.json'))){
  it(file,()=>{const g=JSON.parse(readFileSync('test/goldens/game_logs/'+file,'utf8')) as {input:SeasonInput;params:GameLogParams;service:{rows:Row[]|null}};const result=gameLogs(g.input,g.input.scoreless??null,g.params);expect(result.rows.length).toBe(g.service.rows?.length??0);for(const [i,row]of (g.service.rows??[]).entries())for(const [col,value]of Object.entries(row))expect(result.rows[i][col],`row ${i} column ${col}`).toBe(value);});
 }
});
