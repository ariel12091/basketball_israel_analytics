import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { concatList, zoompanFilter, cleanGraph, captionGraph, captionInputs, cleanIsFresh, ff, probeDuration, writeGraph, ENC } from '../lib/ffmpeg.mjs';

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
  const caps = [{ id: 'c', a: 0.2, b: 2.5 }];
  const { graph, out } = captionGraph(caps);
  const cg = join(TMP, 'cap.txt');
  writeGraph(cg, graph);
  const final = join(TMP, 'final.mp4');
  ff(['-i', clean, ...captionInputs(caps, () => png), '-/filter_complex', cg, '-map', out, ...ENC, final]);
  assert.ok(Math.abs(probeDuration(final) - 3) < 0.15);
});

// Average colour of the frame at time t, as [r, g, b].
const colourAt = (file, t) => {
  const r = spawnSync('ffmpeg', ['-v', 'error', '-ss', String(t), '-i', file, '-frames:v', '1', '-vf', 'scale=1:1', '-f', 'rawvideo', '-pix_fmt', 'rgb24', '-'], { maxBuffer: 1024 });
  return [...r.stdout];
};

test('each caption input lasts only its own window and shows only inside it', () => {
  const caps = [{ id: 'x', a: 1.0, b: 2.0 }];
  const args = captionInputs(caps, (id) => `C:/o/${id}.png`);
  assert.deepEqual(args, ['-loop', '1', '-framerate', '30', '-t', '1.000', '-i', 'C:/o/x.png']);

  mkdirSync(TMP, { recursive: true });
  const black = join(TMP, 'black.mp4');
  ff(['-f', 'lavfi', '-i', 'color=c=black:s=1920x1080:r=30:d=3', ...ENC, black]);
  const png = join(TMP, 'red.png');
  ff(['-f', 'lavfi', '-i', 'color=c=red:s=1920x1080,format=rgba', '-frames:v', '1', png]);
  const { graph, out } = captionGraph(caps);
  const cg = join(TMP, 'win.txt');
  writeGraph(cg, graph);
  const final = join(TMP, 'win.mp4');
  ff(['-i', black, ...captionInputs(caps, () => png), '-/filter_complex', cg, '-map', out, ...ENC, final]);
  assert.ok(Math.abs(probeDuration(final) - 3) < 0.15);
  assert.ok(colourAt(final, 1.5)[0] > 150, `inside window: ${colourAt(final, 1.5)}`);
  assert.ok(colourAt(final, 0.5)[0] < 30, `before window: ${colourAt(final, 0.5)}`);
  assert.ok(colourAt(final, 2.6)[0] < 30, `after window: ${colourAt(final, 2.6)}`);
});

test('mobile chapters are padded, not zoomed', () => {
  assert.match(cleanGraph([], true), /pad=1920:1080/);
  assert.throws(() => cleanGraph([{ a: 0, b: 2, scale: 1.2, xf: 0, yf: 0 }], true), /mobile/);
});

test('a cached zoom pass is reused only if the recording and the zoom graph are both unchanged', () => {
  const base = { outMtime: 200, timelineMtime: 100, prevGraph: 'g1', graph: 'g1' };
  assert.equal(cleanIsFresh(base), true);
  assert.equal(cleanIsFresh({ ...base, outMtime: null }), false, 'no cached file');
  assert.equal(cleanIsFresh({ ...base, timelineMtime: 300 }), false, 're-recorded');
  assert.equal(cleanIsFresh({ ...base, graph: 'g2' }), false, 'script.json zoom edited');
  assert.equal(cleanIsFresh({ ...base, prevGraph: null }), false, 'no graph on record');
});
