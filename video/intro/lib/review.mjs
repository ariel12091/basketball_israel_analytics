import { fillTemplate } from './text.mjs';

const cell = (s) => String(s).replace(/\|/g, '\\|');

export function captionsReviewMarkdown(script, valuesById) {
  const lines = ['# Caption review', '', 'Check the Hebrew column. Edit `script.json`, not this file.', ''];
  for (const ch of script.chapters) {
    lines.push(`## ${ch.id} — ${ch.title_en} / ${ch.title_he}`, '', '| step | English | עברית |', '|---|---|---|');
    for (const st of ch.steps) {
      const v = valuesById[st.id] ?? {};
      lines.push(`| ${st.id} | ${cell(fillTemplate(st.en, v))} | ${cell(fillTemplate(st.he, v))} |`);
    }
    lines.push('');
  }
  return lines.join('\n');
}
