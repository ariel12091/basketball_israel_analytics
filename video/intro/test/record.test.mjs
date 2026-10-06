import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync, readdirSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, '..');
const OUT = join(HERE, 'tmp', 'rec');
const URL = pathToFileURL(join(HERE, 'fixtures', 'table.html')).href;
const run = (...extra) => spawnSync(process.execPath, [join(ROOT, 'record.mjs'), '--no-shiny', '--url', URL,
  '--script', join(HERE, 'fixtures', 'script.fixture.json'), '--out', OUT, ...extra], { encoding: 'utf8', timeout: 120000 });

test('records frames and a timeline, waiting for a late element', () => {
  rmSync(OUT, { recursive: true, force: true });
  const r = run('--chapter', 'fx');
  assert.equal(r.status, 0, r.stderr + r.stdout);
  const tl = JSON.parse(readFileSync(join(OUT, 'rec', 'fx', 'timeline.json'), 'utf8'));
  assert.ok(tl.frames.length >= 5, `only ${tl.frames.length} frames`);
  assert.ok(readdirSync(join(OUT, 'rec', 'fx', 'frames')).length === tl.frames.length);
  const [cell, late] = tl.steps;
  assert.equal(cell.values.net, '+17.0');
  assert.ok(cell.bbox && cell.bbox.width > 0);
  assert.ok(cell.t0 <= cell.tFocus && cell.tFocus < cell.t1);
  assert.ok(cell.t1 - cell.tFocus >= 1.95);
  assert.ok(late.t0 >= cell.t1);
  const team = tl.steps.find((s) => s.id === 'fx-team');
  assert.match(team.values.teams, /MACCABI TEL AVIV/);
  assert.doesNotMatch(team.values.teams, /KIRYAT ATA/, 'clear:true must replace, not add to, the selection');
  assert.ok(tl.frames[0].ts <= cell.t0 + 1, 'frame clock and step clock disagree');
  const probe = spawnSync('ffprobe', ['-v', 'error', '-show_entries', 'stream=width,height', '-of', 'csv=p=0',
    join(OUT, 'rec', 'fx', 'frames', tl.frames[0].file)], { encoding: 'utf8' });
  const [w, h] = probe.stdout.trim().split(',').map(Number);
  assert.ok(w === 1920 && Math.abs(h - 1080) <= 1, `frames are ${w}x${h}, want 1920x1080 (device scale 1.2)`);
});

test('a missing target fails the run with the step id', () => {
  const r = run('--chapter', 'fx-missing');
  assert.notEqual(r.status, 0);
  assert.match(r.stderr + r.stdout, /fx-gone/);
});

test('--check reports per step without recording', () => {
  rmSync(OUT, { recursive: true, force: true });
  const r = run('--check');
  assert.equal(r.status, 1);
  assert.match(r.stdout, /ok\s+fx-cell .*net=\+17\.0/);
  assert.match(r.stdout, /FAIL\s+fx-gone/);
  assert.throws(() => readdirSync(join(OUT, 'rec')));
});
