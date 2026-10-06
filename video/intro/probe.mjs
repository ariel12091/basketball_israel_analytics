#!/usr/bin/env node
// Prints how many elements match each selector, and the first match's HTML,
// after the global and chapter setup have run. Use it to fix selectors with
// evidence: node probe.mjs <chapterId|-> <selector> [...]
import { chromium } from 'playwright';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const [chapterId, ...selectors] = process.argv.slice(2);
const url = process.env.APP_URL ?? 'http://127.0.0.1:3838/';
const script = JSON.parse(readFileSync(join(HERE, 'script.json'), 'utf8'));
const ch = script.chapters.find((c) => c.id === chapterId);
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });
await page.addInitScript({ path: join(HERE, 'page', 'director.js') });
await page.goto(url, { timeout: 120000 });
await page.waitForFunction(() => window.Shiny?.shinyapp?.isConnected?.(), null, { timeout: 120000 });
const busyWait = () => page.waitForFunction(() => !window.__dir.busy(), null, { timeout: 60000 }).then(() => page.waitForTimeout(500));
await busyWait();
for (const s of [...(script.setup ?? []), ...(ch?.setup ?? [])]) {
  if (s.do === 'selectize') {
    await page.locator(`${s.target} + .selectize-control .selectize-input`).click();
    await page.keyboard.type(s.value);
    await page.locator(`${s.target} + .selectize-control .selectize-dropdown .option`, { hasText: s.value }).first().click();
    await page.keyboard.press('Escape');
  } else if (s.do === 'click') await page.locator(s.target).first().click();
  else if (s.do === 'tab') await page.locator(`.navbar a[data-value="${s.target}"]`).click();
  await busyWait();
}
for (const sel of selectors) {
  const loc = page.locator(sel);
  const n = await loc.count();
  const html = n ? (await loc.first().evaluate((e) => e.outerHTML)).slice(0, 600) : '';
  console.log(`--- ${sel}  (${n} match${n === 1 ? '' : 'es'})\n${html}\n`);
}
await browser.close();
