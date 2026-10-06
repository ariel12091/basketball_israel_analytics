import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { concatList, zoompanFilter, cleanGraph, captionGraph, ff, probeDuration, writeGraph, ENC } from '../lib/ffmpeg.mjs';

const TMP = join(dirname(fileURLToPath(import.meta.url)), 'tmp', 'ffmpeg');

test('concatList gives each frame its time until the next', () => {
  const s = concatList([{ file: 'C:/a/f0.jpg', ts: 10 }, { file: 'C:/a/f1.jpg', ts: 10.5 }], 12);
  assert.equal(s, "ffconcat version 1.0\nfile 'C:/a/f0.jpg'\nduration 0.5000\nfile 'C:/a/f1.jpg'\nduration 1.5000\nfile 'C:/a/f1.jpg'\n");
  assert.throws(() => concatList([], 1), /no frames/);
});

test('zoompan without zooms is identity', () => {
  assert.equal(zoompanFilter([]), 'zoompan=z=1:x=0:y=0:d=1:s=1920x1080:fps=30');
  const f = zoompanFilter([{ a: 1, b: 4, scale: 1.5, xf: 300, yf: 200 }]);
  assert.match(f, /^zoompan=z='1\/\(1-\(/);
  assert.match(f, /iw\/1920/);
  assert.match(f, /ih\/1080/);
});

test('ffmpeg accepts the clean graph with a zoom and the caption graph', () => {
  rmSync(TMP, { recursive: true, force: true });
  mkdirSync(TMP, { recursive: true });
  const src = join(TMP, 'src.mp4');
  ff(['-f', 'lavfi', '-i', 'testsrc2=s=1920x1080:r=30:d=3', ...ENC, src]);
  const g = join(TMP, 'clean.txt');
  writeGraph(g, cleanGraph([{ a: 0.5, b: 2.8, scale: 1.5, xf: 400, yf: 200 }], false));
  const clean = join(TMP, 'clean.mp4');
  ff(['-i', src, '-/filter_complex', g, '-map', '[out]', ...ENC, clean]);
  assert.ok(Math.abs(probeDuration(clean) - 3) < 0.15);

  const png = join(TMP, 'cap.png');
  ff(['-f', 'lavfi', '-i', 'color=c=red@0.5:s=1920x1080,format=rgba', '-frames:v', '1', png]);
  const { graph, out } = captionGraph([{ a: 0.2, b: 2.5 }]);
  const cg = join(TMP, 'cap.txt');
  writeGraph(cg, graph);
  const final = join(TMP, 'final.mp4');
  ff(['-i', clean, '-loop', '1', '-framerate', '30', '-t', '3', '-i', png, '-/filter_complex', cg, '-map', out, ...ENC, final]);
  assert.ok(Math.abs(probeDuration(final) - 3) < 0.15);
});

test('mobile chapters are padded, not zoomed', () => {
  assert.match(cleanGraph([], true), /pad=1920:1080/);
  assert.throws(() => cleanGraph([{ a: 0, b: 2, scale: 1.2, xf: 0, yf: 0 }], true), /mobile/);
});
