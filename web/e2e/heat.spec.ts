import {test,expect} from '@playwright/test';
for(const league of ['il','euroleague','eurocup'])
 test(`[X-12][league:${league}] full-population heat uses opposite offense and defense polarity`,async({page})=>{
  const rows=[0,1].map((rank,i)=>({game_id:i+1,team_id:2,team_name:'Audit team',opp_team_name:'Opponent',game_date:'2026-01-01',gn:i+1,round_number:i+1,off_ppp:100,def_ppp:100,pr_off_ppp:rank,pr_def_ppp:rank,off_poss:50,def_poss:50,has_scores:false}));
  await page.route('**/api/v1/**',route=>route.fulfill({json:route.request().url().includes('/version')?{leagues:[]}:{rows,meta:{version:'audit',view:'summary',teams:[],rounds:[],phases:[],shot_averages:null}}}));
  await page.goto(`/${league}/game-logs?season=${league==='il'?'2026':'2025'}`);
  const bad=page.locator('tr[data-game-id="1"]');
  const good=page.locator('tr[data-game-id="2"]');
  // Endpoints exported from Shiny HEAT_COLORS/HEAT_COLORS_REVERSED.
  await expect(bad.locator('td[data-column="off_ppp"]')).toHaveCSS('background-color','rgb(110, 38, 34)');
  await expect(bad.locator('td[data-column="def_ppp"]')).toHaveCSS('background-color','rgb(47, 127, 77)');
  await expect(good.locator('td[data-column="off_ppp"]')).toHaveCSS('background-color','rgb(47, 127, 77)');
  await expect(good.locator('td[data-column="def_ppp"]')).toHaveCSS('background-color','rgb(110, 38, 34)');
 });
