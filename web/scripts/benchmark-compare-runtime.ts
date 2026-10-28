// Offline component benchmark; not the Next.js app, pg pool, or Vercel.
// Use the same recorded frames, production rules and controlled I/O as R.
import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { setTimeout as wait } from 'node:timers/promises';
import { joinCompare, finalizeCompare } from '../src/rules/compare';
import type { Row } from '../src/rules/gameLogs';

const port = Number(process.argv[2] ?? 3191);
if (!Number.isInteger(port) || port < 1024 || port > 65535) throw Error('Invalid port');
type Case = { a: string; b: string; mode: string; metric: string;
  reverse: boolean; minimum: number; expected: Row[] };
const golden = JSON.parse(readFileSync('test/goldens/compare/rules.json', 'utf8')) as {
  datasets: Record<string, Row[]>; cases: Record<string, Case>;
  teams: {team_id: number; team_name: string}[];
};
const teams = golden.teams.map(t => ({ value: String(t.team_id), label: t.team_name }));
createServer(async (req, res) => {
  res.setHeader('Content-Type', 'application/json');
  try {
    const url = new URL(req.url!, 'http://127.0.0.1');
    if (url.pathname === '/health') { res.end(JSON.stringify({ready:true,node:process.version})); return; }
    if (url.pathname !== '/compare') throw Error('Unknown path');
    const id = url.searchParams.get('case')!;
    const c = golden.cases[id];
    const delay = url.searchParams.get('delay');
    const blocking = url.searchParams.get('blocking') === 'true';
    if (!c || !['0', '40'].includes(delay!)) throw Error('Invalid workload');
    async function read(key: string) {
      if (Number(delay) > 0) {
        // Experimental control only: same TS calculations, blocking I/O model.
        // This is never used by the application.
        if (blocking) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,Number(delay));
        else await wait(Number(delay));
      }
      return golden.datasets[key];
    }
    const [a, b] = await Promise.all([read(c.a), read(c.b)]);
    const rows = finalizeCompare(joinCompare(a,b,c.mode,c.metric,teams),
      c.mode,c.reverse ? 'date_split' : '',c.minimum);
    const columns = Object.keys(c.expected[0] ?? {});
    res.end(JSON.stringify({ case: id, rows: rows.map(row =>
      Object.fromEntries(columns.map(key => [key,row[key]]))) }));
  } catch (e) {
    res.statusCode = 500;
    res.end(JSON.stringify({ error: String(e) }));
  }
}).listen(port, '127.0.0.1', () => console.log(`Offline TypeScript Compare component on ${port}`));
