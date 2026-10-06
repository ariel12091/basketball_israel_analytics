#!/usr/bin/env node
// Builds clean (zoomed) chapter video from frames, overlays captions per
// language, and concatenates the tutorial and the short.
// Usage: node compose.mjs [--out d] [--script p] [--chapter id]
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { validateScript } from './lib/script.mjs';
import { focusPlan, squareCropX } from './lib/geometry.mjs';
import { readTimeline, relSteps } from './lib/timeline.mjs';
import { ff, probeDuration, concatList, cleanGraph, captionGraph, writeGraph, ENC, FPS } from './lib/ffmpeg.mjs';
import { CARD, shortPlan, captionWindows } from './lib/plan.mjs';
import { mergeShortChapters, youtubeChapters } from './lib/chapters.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const { values: opt } = parseArgs({ options: {
  out: { type: 'string', default: join(HERE, 'out') },
  script: { type: 'string', default: join(HERE, 'script.json') },
  chapter: { type: 'string' },
} });
const script = JSON.parse(readFileSync(opt.script, 'utf8'));
const errors = validateScript(script);
if (errors.length) { console.error(errors.join('\n')); process.exit(1); }

const OUT = opt.out;
const B = join(OUT, 'build');
const fwd = (p) => p.replace(/\\/g, '/');
const LANGS = ['en', 'he'];

function cleanVideo(ch, tl) {
  const out = join(B, 'clean', `${ch.id}.mp4`);
  const recDir = join(OUT, 'rec', ch.id, 'frames');
  const list = join(B, 'lists', `${ch.id}.ffconcat`);
  writeGraph(list, concatList(tl.frames.map((f) => ({ file: fwd(join(recDir, f.file)), ts: f.ts })), tl.tEnd));
  const rel = relSteps(tl);
  const zooms = ch.steps.flatMap((st) => {
    const r = rel.find((x) => x.id === st.id);
    const { zoom } = focusPlan(r.bbox, tl.viewport, st.zoom);
    return zoom ? [{ a: r.focus, b: r.b, scale: zoom.scale, xf: zoom.xf, yf: zoom.yf }] : [];
  });
  const g = join(B, 'graphs', `clean-${ch.id}.txt`);
  writeGraph(g, cleanGraph(zooms, !!tl.viewport.mobile));
  mkdirSync(dirname(out), { recursive: true });
  ff(['-f', 'concat', '-safe', '0', '-i', list, '-/filter_complex', g, '-map', '[out]', ...ENC, out]);
  return out;
}

function captioned(ch, tl, clean, lang) {
  const out = join(B, lang, `${ch.id}.mp4`);
  const dur = probeDuration(clean);
  const caps = captionWindows(tl);
  const inputs = caps.flatMap((c) => ['-loop', '1', '-framerate', String(FPS), '-t', dur.toFixed(3), '-i', join(OUT, 'overlays', lang, 'wide', `${c.id}.png`)]);
  const { graph, out: label } = captionGraph(caps);
  const g = join(B, 'graphs', `cap-${lang}-${ch.id}.txt`);
  writeGraph(g, graph);
  mkdirSync(dirname(out), { recursive: true });
  ff(['-i', clean, ...inputs, '-/filter_complex', g, '-map', label, ...ENC, out]);
  return out;
}

function cardVideo(png, dur, out) {
  mkdirSync(dirname(out), { recursive: true });
  ff(['-loop', '1', '-framerate', String(FPS), '-t', String(dur), '-i', png,
    '-vf', `fade=t=in:st=0:d=0.3,fade=t=out:st=${(dur - 0.3).toFixed(2)}:d=0.3,format=yuv420p,setsar=1`, ...ENC, out]);
  return out;
}

function concat(files, out) {
  const list = `${out}.ffconcat`;
  writeFileSync(list, 'ffconcat version 1.0\n' + files.map((f) => `file '${fwd(f)}'`).join('\n') + '\n');
  ff(['-f', 'concat', '-safe', '0', '-i', list, '-c', 'copy', out]);
}

const chapters = script.chapters.filter((ch) => !opt.chapter || ch.id === opt.chapter);
const timelines = Object.fromEntries(chapters.map((ch) => [ch.id, readTimeline(OUT, ch.id)]));
const clean = Object.fromEntries(chapters.map((ch) => [ch.id, cleanVideo(ch, timelines[ch.id])]));
for (const lang of LANGS) for (const ch of chapters) captioned(ch, timelines[ch.id], clean[ch.id], lang);

if (!opt.chapter) {
  for (const lang of LANGS) {
    const ov = join(OUT, 'overlays', lang);
    const segs = [{ file: cardVideo(join(ov, 'cards', 'title.png'), CARD.title, join(B, lang, 'card-title.mp4')), title: script.title_card[lang].title }];
    for (const ch of chapters) {
      if (ch.card !== false) segs.push({ file: cardVideo(join(ov, 'cards', `${ch.id}.png`), CARD.chapter, join(B, lang, `card-${ch.id}.mp4`)), title: ch[`title_${lang}`] });
      segs.push({ file: join(B, lang, `${ch.id}.mp4`), chapter: ch });
    }
    segs.push({ file: cardVideo(join(ov, 'cards', 'outro.png'), CARD.outro, join(B, lang, 'card-outro.mp4')) });

    const index = {};
    const entries = [];
    let t = 0;
    for (const s of segs) {
      const d = probeDuration(s.file);
      if (s.title) entries.push({ title: s.title, start: t, end: t + d });
      else if (entries.length) entries.at(-1).end = t + d;
      if (s.chapter) {
        for (const r of relSteps(timelines[s.chapter.id])) index[r.id] = { t0: t + r.a, focus: t + r.focus, t1: t + r.b };
      }
      t += d;
    }
    concat(segs.map((s) => s.file), join(OUT, `intro_${lang}.mp4`));
    writeFileSync(join(OUT, `index_${lang}.json`), JSON.stringify(index, null, 1));
    writeFileSync(join(OUT, `chapters_${lang}.txt`), youtubeChapters(mergeShortChapters(entries)));

    const picks = shortPlan(script, timelines);
    const shortSegs = [cardVideo(join(ov, 'square', 'hook.png'), CARD.hook, join(B, lang, 'sq-hook.mp4'))];
    for (const p of picks) {
      const ch = script.chapters.find((c) => c.id === p.chapter);
      const st = ch.steps.find((x) => x.id === p.id);
      const tl = timelines[p.chapter];
      const rec = tl.steps.find((x) => x.id === p.id);
      const x = squareCropX(focusPlan(rec.bbox, tl.viewport, st.zoom).box, 1920, 1080);
      const len = p.b - p.a;
      const out = join(B, lang, `sq-${p.id}.mp4`);
      const g = join(B, 'graphs', `sq-${lang}-${p.id}.txt`);
      writeGraph(g, `[0:v]crop=1080:1080:${x}:0,setpts=PTS-STARTPTS[b];[1:v]format=rgba,fade=t=in:st=0:d=0.25:alpha=1,fade=t=out:st=${(len - 0.25).toFixed(3)}:d=0.25:alpha=1[c];[b][c]overlay=0:0,setsar=1[out]`);
      ff(['-ss', p.a.toFixed(3), '-to', p.b.toFixed(3), '-i', clean[p.chapter], '-loop', '1', '-framerate', String(FPS), '-t', len.toFixed(3),
        '-i', join(ov, 'square', `${p.id}.png`), '-/filter_complex', g, '-map', '[out]', ...ENC, out]);
      shortSegs.push(out);
    }
    shortSegs.push(cardVideo(join(ov, 'square', 'outro.png'), CARD.shortOutro, join(B, lang, 'sq-outro.mp4')));
    concat(shortSegs, join(OUT, `short_${lang}.mp4`));
    console.log(`${lang}: intro ${probeDuration(join(OUT, `intro_${lang}.mp4`)).toFixed(1)}s, short ${probeDuration(join(OUT, `short_${lang}.mp4`)).toFixed(1)}s`);
  }
}
