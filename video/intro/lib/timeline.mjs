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

// A hold is a fixed sleep, so one that ran long means the page stalled and the
// video would sit on a frozen frame.
export function holdOverruns(ch, tl, slack = 2) {
  return ch.steps.flatMap((st) => {
    const r = tl.steps.find((x) => x.id === st.id);
    const ran = r ? r.t1 - r.tFocus : 0;
    return ran > st.hold + slack
      ? [`${ch.id}/${st.id}: hold ran ${ran.toFixed(1)}s, planned ${st.hold}s (page stalled?) -- re-record this chapter`]
      : [];
  });
}

export function stepRecord(tl, id) {
  const s = tl.steps.find((x) => x.id === id);
  if (!s) throw new Error(`step ${id} missing from timeline ${tl.chapter}`);
  return s;
}
