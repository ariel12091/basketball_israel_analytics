// Caption text: template filling, reading values off cells, and Hebrew bidi isolation.
const PLACEHOLDER = /\{([a-z_][a-z0-9_]*)\}/gi;
// A Latin run starts with a letter, digit, sign or "(", may contain spaces and
// inner punctuation, and ends on a letter, digit, "%" or ")" -- so a sentence's
// final "." or ":" stays in the Hebrew flow.
const LATIN_RUN = /[A-Za-z0-9+\-−±(][A-Za-z0-9+\-−±.,%/:'()_ ]*[A-Za-z0-9%)]|[A-Za-z0-9]/g;

export function fillTemplate(tpl, values) {
  return String(tpl).replace(PLACEHOLDER, (_, k) => {
    const v = values?.[k];
    if (v === undefined || v === null || String(v).trim() === '') {
      throw new Error(`template value "${k}" missing for: ${tpl}`);
    }
    return String(v).trim();
  });
}

export function extractValue(text, pattern) {
  const s = String(text ?? '').replace(/\s+/g, ' ').trim();
  if (!pattern) return s;
  const m = s.match(new RegExp(pattern));
  if (!m) throw new Error(`pattern /${pattern}/ not found in "${s}"`);
  return m[0];
}

export function escapeHtml(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

export function captionHtml(text, lang) {
  const s = String(text);
  if (lang !== 'he') return escapeHtml(s);
  let out = '';
  let last = 0;
  for (const m of s.matchAll(LATIN_RUN)) {
    out += escapeHtml(s.slice(last, m.index)) + `<bdi dir="ltr">${escapeHtml(m[0])}</bdi>`;
    last = m.index + m[0].length;
  }
  return out + escapeHtml(s.slice(last));
}
