import { relSteps } from './timeline.mjs';

export const CARD = { chapter: 1.5, title: 2.5, outro: 3.5, hook: 2.0, shortOutro: 3.0 };
export const SHORT_MAX = 60;

export function captionWindows(tl) {
  return relSteps(tl).map((s) => ({ id: s.id, a: s.a, b: s.b }));
}

export function shortPlan(script, timelines) {
  const picks = [];
  for (const ch of script.chapters) {
    for (const st of ch.steps) {
      if (st.short === undefined) continue;
      const tl = timelines[ch.id];
      if (!tl) throw new Error(`short step ${st.id} needs chapter ${ch.id} recorded`);
      const s = relSteps(tl).find((x) => x.id === st.id);
      if (!s) throw new Error(`step ${st.id} missing from timeline ${ch.id}`);
      picks.push({ order: st.short, chapter: ch.id, id: st.id, a: Math.max(s.a, s.focus - 1.0), b: s.b });
    }
  }
  picks.sort((x, y) => x.order - y.order);
  const total = picks.reduce((t, p) => t + (p.b - p.a), CARD.hook + CARD.shortOutro);
  if (total > SHORT_MAX) throw new Error(`short cut would run ${total.toFixed(1)}s (> ${SHORT_MAX}s); shorten holds or drop a short step`);
  return picks.map(({ chapter, id, a, b }) => ({ chapter, id, a, b }));
}
