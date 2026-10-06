// Validates script.json, the single source of truth for the film.
export const ACTIONS = new Set(['none', 'wait', 'click', 'hover', 'selectize', 'clear', 'scroll', 'tab']);
export const MAX_WORDS = 14;
export const MIN_HOLD = 1.5;
export const MAX_ZOOM = 1.6;
const PLACEHOLDER = /\{([a-z_][a-z0-9_]*)\}/gi;
const HEBREW = /[֐-׿]/;

export function placeholders(text) {
  return [...String(text).matchAll(PLACEHOLDER)].map((m) => m[1]);
}

export function wordCount(text) {
  return String(text).trim().split(/\s+/).filter((w) => /[\p{L}\p{N}{]/u.test(w)).length;
}

function checkAction(where, s, errors) {
  if (!ACTIONS.has(s.do)) errors.push(`${where}: unknown action "${s.do}"`);
  else if (!['none', 'wait'].includes(s.do) && !s.target) errors.push(`${where}: action "${s.do}" needs a target`);
  if (s.do === 'selectize' && !s.value) errors.push(`${where}: selectize needs a value`);
}

export function validateScript(script) {
  const errors = [];
  const chapters = Array.isArray(script?.chapters) ? script.chapters : [];
  if (chapters.length === 0) errors.push('script has no chapters');
  for (const k of ['url', 'title_card', 'outro_card', 'short']) if (!script?.[k]) errors.push(`script: missing ${k}`);
  for (const [i, s] of (script?.setup ?? []).entries()) checkAction(`setup[${i}]`, s, errors);
  const ids = new Set();
  for (const ch of chapters) {
    const cid = ch.id ?? '?';
    for (const k of ['id', 'title_en', 'title_he']) if (!ch[k]) errors.push(`chapter ${cid}: missing ${k}`);
    if (ch.title_he && !HEBREW.test(ch.title_he)) errors.push(`chapter ${cid}: title_he has no Hebrew`);
    for (const [i, s] of (ch.setup ?? []).entries()) checkAction(`${cid}/setup[${i}]`, s, errors);
    if (!Array.isArray(ch.steps) || ch.steps.length === 0) errors.push(`chapter ${cid}: no steps`);
    for (const st of ch.steps ?? []) {
      const where = `${cid}/${st.id ?? '?'}`;
      if (!st.id) errors.push(`${where}: missing id`);
      else if (ids.has(st.id)) errors.push(`${where}: duplicate step id`);
      else ids.add(st.id);
      checkAction(where, st, errors);
      if (typeof st.hold !== 'number' || st.hold < MIN_HOLD) errors.push(`${where}: hold must be a number >= ${MIN_HOLD}`);
      if (st.zoom !== undefined) {
        if (!(st.zoom >= 1 && st.zoom <= MAX_ZOOM)) errors.push(`${where}: zoom must be within 1..${MAX_ZOOM}`);
        if (!(st.hold >= 2)) errors.push(`${where}: a zoomed step needs hold >= 2`);
        if (ch.viewport) errors.push(`${where}: zoom is not supported in a custom-viewport chapter`);
      }
      for (const lang of ['en', 'he']) {
        const t = st[lang];
        if (typeof t !== 'string' || !t.trim()) { errors.push(`${where}: missing ${lang} caption`); continue; }
        const n = wordCount(t);
        if (n > MAX_WORDS) errors.push(`${where}: ${lang} caption has ${n} words (max ${MAX_WORDS})`);
        for (const p of placeholders(t)) if (!st.read || !(p in st.read)) errors.push(`${where}: {${p}} has no read entry`);
      }
      if (st.he && !HEBREW.test(st.he)) errors.push(`${where}: he caption has no Hebrew`);
      if (st.en && st.he) {
        const a = placeholders(st.en).sort().join(',');
        const b = placeholders(st.he).sort().join(',');
        if (a !== b) errors.push(`${where}: placeholders differ (en: ${a || '-'}, he: ${b || '-'})`);
      }
      if (st.short !== undefined && !Number.isInteger(st.short)) errors.push(`${where}: short must be an integer order`);
    }
  }
  const shorts = chapters.flatMap((c) => c.steps ?? []).filter((s) => s.short !== undefined).map((s) => s.short);
  if (new Set(shorts).size !== shorts.length) errors.push('short orders must be unique');
  return errors;
}
