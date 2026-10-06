import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { shortPlan, captionWindows } from '../lib/plan.mjs';
import { probeDuration } from '../lib/ffmpeg.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, '..');
const OUT = join(HERE, 'tmp', 'pipe');
const SCRIPT = join(HERE, 'fixtures', 'script.fixture.json');
const node = (file, ...a) => spawnSync(process.execPath, [join(ROOT, file), '--out', OUT, '--script', SCRIPT, ...a], { encoding: 'utf8', timeout: 600000 });

test('shortPlan orders by short, trims to focus, refuses > 60 s', () => {
  const script = { chapters: [{ id: 'c', steps: [{ id: 'x', short: 2 }, { id: 'y', short: 1 }, { id: 'z' }] }] };
  const tl = { c: { chapter: 'c', frames: [{ ts: 100 }], steps: [
    { id: 'x', t0: 101, tFocus: 105, t1: 108 }, { id: 'y', t0: 110, tFocus: 110.5, t1: 113 }, { id: 'z', t0: 1, tFocus: 1, t1: 2 }] } };
  assert.deepEqual(shortPlan(script, tl), [{ chapter: 'c', id: 'y', a: 10, b: 13 }, { chapter: 'c', id: 'x', a: 4, b: 8 }]);
  tl.c.steps[0].t1 = 170;
  assert.throws(() => shortPlan(script, tl), /short cut would run/);
  assert.deepEqual(captionWindows(tl.c).map((w) => w.id), ['x', 'y', 'z']);
});

test('fixture chapter goes through overlays, compose and verify', () => {
  rmSync(OUT, { recursive: true, force: true });
  const url = pathToFileURL(join(HERE, 'fixtures', 'table.html')).href;
  const rec = node('record.mjs', '--no-shiny', '--url', url, '--chapter', 'fx');
  assert.equal(rec.status, 0, rec.stderr);
  const ov = node('overlays.mjs', '--only-recorded');
  assert.equal(ov.status, 0, ov.stderr);
  assert.ok(existsSync(join(OUT, 'overlays', 'he', 'wide', 'fx-cell.png')));
  assert.ok(existsSync(join(OUT, 'overlays', 'he', 'square', 'fx-cell.png')));
  const cp = node('compose.mjs', '--chapter', 'fx');
  assert.equal(cp.status, 0, cp.stderr);
  const clean = probeDuration(join(OUT, 'build', 'clean', 'fx.mp4'));
  assert.ok(Math.abs(probeDuration(join(OUT, 'build', 'he', 'fx.mp4')) - clean) < 0.2);
  const st = node('verify.mjs', '--still', 'fx-cell');
  assert.equal(st.status, 0, st.stderr);
  assert.ok(existsSync(join(OUT, 'stills', 'en-fx-cell.png')));
  assert.ok(existsSync(join(OUT, 'stills', 'he-fx-cell.png')));
  const rv = node('review.mjs', '--only-recorded');
  assert.equal(rv.status, 0, rv.stderr);
  assert.match(readFileSync(join(OUT, 'captions_review.md'), 'utf8'), /\| fx-cell \| Clark III: \+17\.0 \| קלארק: \+17\.0 \|/);
});
