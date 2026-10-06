import { test } from 'node:test';
import assert from 'node:assert/strict';
import { relSteps, stepRecord } from '../lib/timeline.mjs';
import { fmtTime, mergeShortChapters, youtubeChapters } from '../lib/chapters.mjs';
import { captionsReviewMarkdown } from '../lib/review.mjs';

const tl = {
  chapter: 'c1',
  frames: [{ file: 'f0.jpg', ts: 1000 }, { file: 'f1.jpg', ts: 1000.5 }],
  tEnd: 1010,
  steps: [{ id: 's1', t0: 1001, tFocus: 1002, t1: 1004, bbox: null, values: { net: '+17.0' } }],
};

test('relSteps is relative to the first frame', () => {
  assert.deepEqual(relSteps(tl), [{ id: 's1', a: 1, focus: 2, b: 4, bbox: null, values: { net: '+17.0' } }]);
  assert.throws(() => relSteps({ ...tl, frames: [] }), /no frames/);
  assert.equal(stepRecord(tl, 's1').values.net, '+17.0');
  assert.throws(() => stepRecord(tl, 'zz'), /step zz missing from timeline c1/);
});

test('chapter times and YouTube rules', () => {
  assert.equal(fmtTime(0), '0:00');
  assert.equal(fmtTime(75.9), '1:15');
  assert.equal(fmtTime(3725), '1:02:05');
  const merged = mergeShortChapters([
    { title: 'Intro', start: 0, end: 8 },
    { title: 'Home', start: 8, end: 33 },
    { title: 'On/Off', start: 33, end: 90 },
    { title: 'Lineups', start: 90, end: 140 },
  ]);
  assert.deepEqual(merged.map((e) => [e.title, e.start]), [['Intro', 0], ['On/Off', 33], ['Lineups', 90]]);
  assert.equal(youtubeChapters(merged), '0:00 Intro\n0:33 On/Off\n1:30 Lineups\n');
  assert.throws(() => youtubeChapters(merged.slice(0, 2)), /at least 3/);
});

test('review table fills values and escapes pipes', () => {
  const script = { chapters: [{ id: 'c1', title_en: 'Q?', title_he: 'שאלה?', steps: [{ id: 's1', en: 'Net {net} | x', he: 'נטו {net}' }] }] };
  const md = captionsReviewMarkdown(script, { s1: { net: '+17.0' } });
  assert.match(md, /\| s1 \| Net \+17\.0 \\\| x \| נטו \+17\.0 \|/);
  assert.match(md, /## c1 — Q\? \/ שאלה\?/);
});
