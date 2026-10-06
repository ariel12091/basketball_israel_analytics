#!/usr/bin/env node
// Extracts a frame per step from the finished videos for inspection and checks
// durations. --still <stepId> grabs one captioned frame per language from the
// chapter build instead (the look-and-feel approval checkpoint).
import { readFileSync, mkdirSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { ff, probeDuration } from './lib/ffmpeg.mjs';
import { readTimeline, relSteps } from './lib/timeline.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const { values: opt } = parseArgs({ options: {
  out: { type: 'string', default: join(HERE, 'out') },
  script: { type: 'string', default: join(HERE, 'script.json') },
  still: { type: 'string' },
} });
const script = JSON.parse(readFileSync(opt.script, 'utf8'));
const OUT = opt.out;
const grab = (video, t, png) => { mkdirSync(dirname(png), { recursive: true }); ff(['-ss', t.toFixed(3), '-i', video, '-frames:v', '1', png]); };

if (opt.still) {
  const ch = script.chapters.find((c) => c.steps.some((s) => s.id === opt.still));
  if (!ch) { console.error(`no step ${opt.still}`); process.exit(1); }
  const r = relSteps(readTimeline(OUT, ch.id)).find((s) => s.id === opt.still);
  for (const lang of ['en', 'he']) grab(join(OUT, 'build', lang, `${ch.id}.mp4`), (r.focus + r.b) / 2, join(OUT, 'stills', `${lang}-${opt.still}.png`));
  console.log(`stills in ${join(OUT, 'stills')}`);
  process.exit(0);
}

let bad = 0;
for (const lang of ['en', 'he']) {
  const intro = join(OUT, `intro_${lang}.mp4`);
  const short = join(OUT, `short_${lang}.mp4`);
  if (!existsSync(intro) || !existsSync(short)) { console.error(`${lang}: run compose.mjs first`); process.exit(1); }
  const di = probeDuration(intro);
  const ds = probeDuration(short);
  console.log(`${lang}: intro ${di.toFixed(1)}s, short ${ds.toFixed(1)}s`);
  if (di < 210 || di > 360) { console.error(`${lang}: intro ${di.toFixed(1)}s outside 210-360s`); bad++; }
  if (ds > 60) { console.error(`${lang}: short ${ds.toFixed(1)}s over 60s`); bad++; }
  const index = JSON.parse(readFileSync(join(OUT, `index_${lang}.json`), 'utf8'));
  Object.entries(index).forEach(([id, w], i) => grab(intro, (w.focus + w.t1) / 2, join(OUT, 'verify', lang, `${String(i).padStart(2, '0')}-${id}.jpg`)));
}
console.log(`frames in ${join(OUT, 'verify')} -- inspect every one`);
process.exit(bad ? 1 : 0);
