#!/usr/bin/env node
// Renders every caption and card PNG for both languages from script.json and
// the recorded timelines. --only-recorded skips chapters with no timeline.
import { chromium } from 'playwright';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { validateScript } from './lib/script.mjs';
import { fillTemplate, captionHtml } from './lib/text.mjs';
import { focusPlan, captionPosition } from './lib/geometry.mjs';
import { readTimeline, stepRecord } from './lib/timeline.mjs';
import { openOverlayPage, renderOverlay } from './lib/overlay-render.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const { values: opt } = parseArgs({ options: {
  out: { type: 'string', default: join(HERE, 'out') },
  script: { type: 'string', default: join(HERE, 'script.json') },
  'only-recorded': { type: 'boolean', default: false },
} });
const script = JSON.parse(readFileSync(opt.script, 'utf8'));
const errors = validateScript(script);
if (errors.length) { console.error(errors.join('\n')); process.exit(1); }

const recorded = (ch) => existsSync(join(opt.out, 'rec', ch.id, 'timeline.json'));
const chapters = script.chapters.filter((ch) => !opt['only-recorded'] || recorded(ch));
const carded = script.chapters.filter((ch) => ch.card !== false);
const browser = await chromium.launch();
try {
  const page = await openOverlayPage(browser);
  for (const lang of ['en', 'he']) {
    const dir = join(opt.out, 'overlays', lang);
    const h = (s) => captionHtml(s, lang);
    for (const ch of chapters) {
      const tl = readTimeline(opt.out, ch.id);
      for (const st of ch.steps) {
        const rec = stepRecord(tl, st.id);
        const html = h(fillTemplate(st[lang], rec.values));
        const { box } = focusPlan(rec.bbox, tl.viewport, st.zoom);
        await renderOverlay(page, { kind: 'caption', lang, html, position: captionPosition(box, 1080), w: 1920, h: 1080 }, join(dir, 'wide', `${st.id}.png`));
        if (st.short !== undefined) {
          await renderOverlay(page, { kind: 'caption', lang, html, position: captionPosition(box, 1080), w: 1080, h: 1080 }, join(dir, 'square', `${st.id}.png`));
        }
      }
      if (ch.card !== false) {
        const n = `${carded.indexOf(ch) + 1}/${carded.length}`;
        await renderOverlay(page, { kind: 'card', lang, html: h(ch[`title_${lang}`]), icon: ch.icon, n, w: 1920, h: 1080 }, join(dir, 'cards', `${ch.id}.png`));
      }
    }
    const tc = script.title_card[lang];
    const oc = script.outro_card[lang];
    await renderOverlay(page, { kind: 'card', lang, html: h(tc.title), sub: h(tc.sub), icon: 'bi-activity', w: 1920, h: 1080 }, join(dir, 'cards', 'title.png'));
    await renderOverlay(page, { kind: 'card', lang, html: h(oc.title), sub: h(script.url), icon: 'bi-activity', w: 1920, h: 1080 }, join(dir, 'cards', 'outro.png'));
    await renderOverlay(page, { kind: 'card', lang, html: h(script.short[`hook_${lang}`]), icon: 'bi-activity', w: 1080, h: 1080 }, join(dir, 'square', 'hook.png'));
    await renderOverlay(page, { kind: 'card', lang, html: h(oc.title), sub: h(script.url), icon: 'bi-activity', w: 1080, h: 1080 }, join(dir, 'square', 'outro.png'));
  }
} finally {
  await browser.close();
}
console.log(`overlays written for ${chapters.length} chapter(s)`);
