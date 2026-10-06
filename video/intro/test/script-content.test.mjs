import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateScript } from '../lib/script.mjs';

const script = JSON.parse(readFileSync(join(dirname(fileURLToPath(import.meta.url)), '..', 'script.json'), 'utf8'));

test('the real script is valid', () => {
  assert.deepEqual(validateScript(script), []);
});

test('it covers every chapter the spec lists, in order', () => {
  assert.deepEqual(script.chapters.map((c) => c.id),
    ['cold-open', 'home', 'onoff', 'lineups', 'team', 'gamelogs', 'players', 'compare', 'euro', 'tips', 'tips-mobile']);
});

test('planned length fits the spec', () => {
  const holds = script.chapters.flatMap((c) => c.steps).reduce((t, s) => t + s.hold + 1.6, 0);
  assert.ok(holds > 200 && holds < 330, `planned ${holds.toFixed(0)}s`);
  const short = script.chapters.flatMap((c) => c.steps).filter((s) => s.short !== undefined);
  assert.ok(short.length >= 6 && short.length <= 8);
  assert.ok(short.reduce((t, s) => t + s.hold + 1.0, 5) <= 55);
});

test('captions never hard-code a stat', () => {
  for (const st of script.chapters.flatMap((c) => c.steps)) {
    assert.doesNotMatch(st.en, /[+\-−]\d|\d+\.\d/, `${st.id} has a typed number; use read`);
  }
});

test('"he is still {noluck}" can only be filled with a positive number', async () => {
  // If removing 3PT luck ever flips the sign, the caption would be false;
  // the read must then fail the recording instead.
  const { extractValue } = await import('../lib/text.mjs');
  const luck = script.chapters.flatMap((c) => c.steps).find((s) => s.id === 'onoff-luck');
  const p = luck.read.noluck.pattern;
  assert.equal(extractValue('Without 3PT luck +8.5 SAMPLE SIZE', p), '+8.5');
  assert.throws(() => extractValue('Without 3PT luck -2.1 SAMPLE SIZE', p), /not found/);
});
