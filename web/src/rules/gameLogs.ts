import { rRound } from './round';
import { applyStatFilters } from './statFilters';
export type League = 'il' | 'euroleague' | 'eurocup';
export type Row = Record<string, string | number | boolean | null>;
export type IlGameLogRow = Row & { game_id: number; team_id: number; type_lineup: string | null; num_starters: number | null };
export type EuroNativeRow = Row & { game_id: number; team_id: number; own_starters: number | null; opp_starters: number | null };
export type StatFilter = {col: string; op: 'ge' | 'le'; value: number};
export type Bounds = {off_min?:number|null;off_max?:number|null;def_min?:number|null;def_max?:number|null};
export interface GameLogParams {league:League;season:number|string;view:'summary'|'four_factors'; team?:string|null;teams?:number[]|null; dates?: (string|null)[]|null;game_type?:string[]|string|null;phase?:string[]|string|null;opponents?:string[]|string|null;home_away?:string;outcome?:string;starters?:Bounds;gn?:{min_gn?:number|null;max_gn?:number|null;last_n?:number|null};stat_filters?:StatFilter[]}
export interface SeasonInput {rows:Row[];ff?:Row[];schedule?:Row[];scoreless?:Row[]|null}
const arr = (v:unknown):string[] => v == null ? [] : (Array.isArray(v)?v:[v]).map(String);
const n = (v:unknown) => v == null ? 0 : Number(v);
const key = (r:Row) => `${r.game_id}/${r.team_id}`;
const pct = (a:number,b:number) => b > 0 ? rRound(a/b*100,1):null;
const prName = (name:string) => 'pr_'+name.replace(/[^A-Za-z0-9]+/g,'_');
export const FF_METRICS = ['off_ppp','def_ppp','net_rtg','off_efg_pct','off_oreb_pct','off_tov_pct','off_ftr_pct','def_efg_pct','def_oreb_pct','def_tov_pct','def_ftr_pct'];
export const HEAT_REVERSE:Record<string,boolean> = {off_ppp:false,def_ppp:true,off_efg_pct:false,off_oreb_pct:false,off_tov_pct:true,off_ftr_pct:false,def_efg_pct:true,def_oreb_pct:true,def_tov_pct:false,def_ftr_pct:true};
/** app/R/svc_game_logs.R gl_filter_games; whole-game Last N precedes all other filters (D24). */
export function filterGames(rows:Row[], p:GameLogParams, league:League=p.league):Row[] {
  let selected = rows;
  const gp=p.gn??{}, euro=league!=='il', round=euro?'round_number':'gn';
  if(gp.last_n != null){
    const games=new Map<string,Row>(); for(const r of rows) games.set(key(r),r);
    const recent=new Set<string>(),counts=new Map<string,number>();
    for(const r of [...games.values()].sort((a,b)=> String(a.team_id).localeCompare(String(b.team_id)) || String(b.game_date).localeCompare(String(a.game_date)) || n(b.game_id)-n(a.game_id))){const t=String(r.team_id),c=counts.get(t)??0;if(c<gp.last_n)recent.add(key(r));counts.set(t,c+1);}
    selected=selected.filter(r=>recent.has(key(r)));
  }
  const teams=arr(euro?p.teams:p.team).filter(Boolean), opponents=arr(p.opponents),types=arr(euro?p.phase:p.game_type).flatMap(t=>t.split(','));
  return selected.filter(r => (!teams.length||teams.includes(String(r.team_id))) && (!opponents.length||opponents.includes(String(r.opp_team_id))) && (!types.length||types.includes(String(r[euro?'phase':'game_type']))) && (!p.home_away||r.is_home===(p.home_away==='home')) && (!p.outcome||r.has_won===(p.outcome==='win')) && (gp.min_gn==null||n(r[round])>=gp.min_gn) && (gp.max_gn==null||n(r[round])<=gp.max_gn) && (!p.dates || ((!euro || p.dates.every(Boolean)) && (!p.dates[0]||String(r.game_date)>=p.dates[0]) && (!p.dates[1]||String(r.game_date)<=p.dates[1]))));
}
/** dplyr percent_rank: minimum rank, nonmissing population, singleton NA. */
export function percentRank(values:(number|null)[]):(number|null)[]{const population=values.filter((x):x is number=>x!=null&&Number.isFinite(x)).sort((a,b)=>a-b);return values.map(v=>v==null||population.length<2?null:population.indexOf(v)/(population.length-1));}
function starterMatch(r:Row,b?:Bounds){if(!b)return true;const side=r.type_lineup==='offense'?'off':r.type_lineup==='defense'?'def':null;if(!side)return false;const lo=b[`${side}_min`],hi=b[`${side}_max`];return (lo==null||(r.num_starters!=null&&n(r.num_starters)>=lo))&&(hi==null||(r.num_starters!=null&&n(r.num_starters)<=hi));}
const SUMMARY_COUNTS:Record<string,string>={poss:'total_poss',pts:'total_pts',fg2m:'fg2_made',fg2a:'fg2_att',fg3m:'fg3_made',fg3a:'fg3_att'};
const FF_COUNTS:Record<string,string>={pts:'total_points',poss:'total_poss',ts_poss:'ts_poss_count',oreb:'oreb_count',oreb_opp:'oreb_opportunities',tov:'tov_count',fta:'total_ft_attempts',fga:'total_fga',fgm:'total_fgm',fg3m:'total_fg3_made'};
/** app/R/svc_game_logs.R gl_build_summary_metrics / gl_build_ff_metrics. */
function buildMetrics(rows:Row[],schedule:Row[],bounds:Bounds|undefined,ff:boolean,apply=true):Row[]{
  const valid=new Set(schedule.map(key)),counts=ff?FF_COUNTS:SUMMARY_COUNTS,groups=new Map<string,Row>();
  for(const r of rows){if(!valid.has(key(r))||(apply&&!starterMatch(r,bounds))||!['offense','defense'].includes(String(r.type_lineup)))continue;const k=key(r)+'/'+r.type_lineup;let g=groups.get(k);if(!g){g={game_id:r.game_id,team_id:r.team_id,type_lineup:r.type_lineup,minutes:0};for(const col of Object.keys(counts))g[col]=0;groups.set(k,g);}for(const [dest,src] of Object.entries(counts))g[dest]=n(g[dest])+n(r[src]);g.minutes=n(g.minutes)+n(r.minutes);}
  const out:Row[]=[];for(const g of groups.values()){if(g.type_lineup!=='offense')continue;const d=groups.get(key(g)+'/defense');const row:Row={game_id:g.game_id,team_id:g.team_id};for(const col of Object.keys(counts)){row['off_'+col]=g[col];row['def_'+col]=d?.[col]??null;}
    row.off_ppp=pct(n(row.off_pts),n(row.off_poss));row.def_ppp=pct(n(row.def_pts),n(row.def_poss));row.net_rtg=ff&&(row.off_ppp==null||row.def_ppp==null)?null:rRound(n(row.off_ppp)-n(row.def_ppp),1);row.minutes=rRound(n(g.minutes),1);
    if(ff){row.off_minutes=g.minutes;for(const side of ['off','def']){row[side+'_ts_pct']=pct(n(row[side+'_pts']),2*n(row[side+'_ts_poss']));row[side+'_efg_pct']=pct(n(row[side+'_fgm'])+.5*n(row[side+'_fg3m']),n(row[side+'_fga']));row[side+'_oreb_pct']=pct(n(row[side+'_oreb']),n(row[side+'_oreb_opp']));row[side+'_tov_pct']=pct(n(row[side+'_tov']),n(row[side+'_poss']));row[side+'_ftr_pct']=pct(n(row[side+'_fta']),n(row[side+'_fga']));}}
    out.push(row);
  }return out;
}
export const buildSummaryMetrics=(rows:Row[],schedule:Row[],bounds?:Bounds,apply=true)=>buildMetrics(rows,schedule,bounds,false,apply);
export const buildFfMetrics=(rows:Row[],schedule:Row[],bounds?:Bounds,apply=true)=>buildMetrics(rows,schedule,bounds,true,apply);
const GAME_LABELS:Record<string,string>={'5':'Regular season','16':'PO QF','17':'PO Finals','26':'PO SF','33':'Play-in','10':'Winner Cup','34':'Winner Cup','35':'State Cup'};
const PHASE_LABELS:Record<string,string>={RS:'Regular Season',PLAYOFFS:'Playoffs',FF:'Final Four',TS:'Top 16'};
const defaultOrder=(a:Row,b:Row)=>String(b.game_date).localeCompare(String(a.game_date))||n(b.gn??b.round_number)-n(a.gn??a.round_number)||n(a.game_id)-n(b.game_id)||String(a.team_name).localeCompare(String(b.team_name));
/** app/R/svc_game_logs.R gl_join_schedule_info. */
export function joinScheduleInfo(metrics:Row[],schedule:Row[]):Row[]{const map=new Map(schedule.map(r=>[key(r),r]));return metrics.flatMap(m=>{const s=map.get(key(m));if(!s)return [];return [{...m,...Object.fromEntries(['team_name','gn','game_type','game_date','opp_team_name','team_score','opp_score','has_won'].map(k=>[k,s[k]])),game_type_label:GAME_LABELS[String(s.game_type)]??String(s.game_type),result:s.has_won?'W':'L',score_display:`${s.team_score}-${s.opp_score}`}];}).sort(defaultOrder);}
/** app/R/svc_game_logs.R gl_build_euro_games: retain native rows until after filters. */
export function buildEuroGames(native:Row[],bounds?:Bounds,apply=true):Row[]{const schedules=[...new Map(native.map(r=>[key(r),r])).values()];const expanded:Row[]=[];for(const r of native)for(const side of ['off','def']){const row:Row={game_id:r.game_id,team_id:r.team_id,type_lineup:side==='off'?'offense':'defense',num_starters:r[side==='off'?'own_starters':'opp_starters'],minutes:side==='off'?r.minutes:null};for(const [short,dest]of Object.entries(FF_COUNTS))row[dest]=r[side+'_'+short];expanded.push(row);}const map=new Map(schedules.map(r=>[key(r),r]));return buildFfMetrics(expanded,schedules,bounds,apply).map(m=>{const r=map.get(key(m))!;const sched=Object.fromEntries(['round_number','phase','game_date','opp_team_id','is_home','has_won','team_points','opp_points','team_name','opp_team_name'].map(k=>[k,r[k]]));return {...m,...sched,result:r.has_won?'W':'L',score:`${r.team_points}-${r.opp_points}`,phase_label:PHASE_LABELS[String(r.phase)]??r.phase};}).sort(defaultOrder);}
/** app/R/svc_game_logs.R gl_attach_percentiles: join season rank, never rerank filtered rows. */
export function attachPercentiles(display:Row[],baseline:Row[],metrics:string[]):Row[]{const rank=new Map(baseline.map(r=>[key(r),{} as Row]));for(const metric of metrics){const values=percentRank(baseline.map(r=>r[metric]==null?null:n(r[metric])));baseline.forEach((r,i)=>rank.get(key(r))![prName(metric)]=values[i]);}return display.map(r=>({...r,...rank.get(key(r))}));}
const withScores=(rows:Row[],scoreless:Row[]|null=[])=>{const bad=new Set((scoreless??[]).map(key));return rows.map(r=>({...r,has_scores:!bad.has(key(r))}));};
/** app/R/svc_game_logs.R gl_summary_display. */
export function summaryDisplay(rows:Row[],filters:StatFilter[]=[],scoreless:Row[]|null=[]):Row[]{return withScores(applyStatFilters(rows.map(r=>{const x={...r};for(const side of ['off','def']){const a=n(r[side+'_fg2a']),b=n(r[side+'_fg3a']);x[side==='off'?'Off Shot':'Def Shot']=a+b;x[side+'_fg2_pct']=pct(n(r[side+'_fg2m']),a);x[side+'_fg3_pct']=pct(n(r[side+'_fg3m']),b);x[side+'_fg2_freq']=pct(a,a+b);x[side+'_fg3_freq']=pct(b,a+b);}return x;}),filters),scoreless);}
export const ffDisplay=(rows:Row[],filters:StatFilter[]=[],scoreless:Row[]|null=[])=>withScores(applyStatFilters(rows,filters),scoreless);
/** app/R/svc_game_logs.R gl_shot_averages. */
export function shotAverages(rows:Row[]){if(!rows.length)return null;return Object.fromEntries(['off','def'].map(side=>[side,Object.fromEntries([2,3].map(fg=>{const a=rows.reduce((s,r)=>s+n(r[`${side}_fg${fg}a`]),0),m=rows.reduce((s,r)=>s+n(r[`${side}_fg${fg}m`]),0);return ['fg'+fg,a>0?rRound(m/a*100):fg===2?53:34];}))]));}
/** app/R/svc_game_logs.R svc_game_logs composition. */
export function gameLogs(input:SeasonInput|Row[],scoreless:Row[]|null,p:GameLogParams){const data:SeasonInput=Array.isArray(input)?{rows:input}:input;let rows:Row[];if(p.league==='il'){const schedule=data.schedule??data.rows;const ff=data.ff??data.rows.map(r=>({...r,total_poss:r.ff_total_poss??r.total_poss,minutes:r.ff_minutes??r.minutes}));const source=p.view==='summary'?data.rows:ff,build=p.view==='summary'?buildSummaryMetrics:buildFfMetrics;rows=joinScheduleInfo(attachPercentiles(build(source,filterGames(schedule,p),p.starters),build(source,schedule,undefined,false),p.view==='summary'?['off_ppp','def_ppp','net_rtg']:FF_METRICS),schedule);rows=p.view==='summary'?summaryDisplay(rows,p.stat_filters,scoreless??data.scoreless):ffDisplay(rows,p.stat_filters,scoreless??data.scoreless);}else rows=withScores(attachPercentiles(buildEuroGames(filterGames(data.rows,p),p.starters),buildEuroGames(data.rows,undefined,false),FF_METRICS));return {rows,meta:{default_sort:[{column:'game_date',direction:'desc'},{column:p.league==='il'?'gn':'round_number',direction:'desc'}],shot_averages:p.league==='il'&&p.view==='summary'?shotAverages(rows):null,percentile_population:'season'}};}
