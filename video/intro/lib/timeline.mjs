import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';

export function readTimeline(outDir, chapterId) {
  const p = join(outDir, 'rec', chapterId, 'timeline.json');
  if (!existsSync(p)) throw new Error(`no timeline for chapter ${chapterId} (${p}); run record.mjs first`);
  return JSON.parse(readFileSync(p, 'utf8'));
}

export function relSteps(tl) {
  if (!tl.frames?.length) throw new Error(`no frames recorded for chapter ${tl.chapter}`);
  const z = tl.frames[0].ts;
  return tl.steps.map((s) => ({ id: s.id, a: s.t0 - z, focus: s.tFocus - z, b: s.t1 - z, bbox: s.bbox, values: s.values }));
}

export function stepRecord(tl, id) {
  const s = tl.steps.find((x) => x.id === id);
  if (!s) throw new Error(`step ${id} missing from timeline ${tl.chapter}`);
  return s;
}
