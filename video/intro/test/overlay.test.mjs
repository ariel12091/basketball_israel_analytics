import { test } from 'node:test';
import assert from 'node:assert/strict';
import { chromium } from 'playwright';
import { readFileSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { openOverlayPage, renderOverlay } from '../lib/overlay-render.mjs';
import { captionHtml } from '../lib/text.mjs';

const TMP = join(dirname(fileURLToPath(import.meta.url)), 'tmp', 'overlay');
const pngInfo = (p) => { const b = readFileSync(p); return { w: b.readUInt32BE(16), h: b.readUInt32BE(20), colorType: b[25] }; };

test('captions are transparent RGBA at frame size; Hebrew is RTL; cards are opaque', async () => {
  rmSync(TMP, { recursive: true, force: true });
  const browser = await chromium.launch();
  try {
    const page = await openOverlayPage(browser);
    const he = join(TMP, 'he.png');
    await renderOverlay(page, { kind: 'caption', lang: 'he', html: captionHtml("ג'ימי קלארק: +17.0 נקודות", 'he'), position: 'bottom', w: 1920, h: 1080 }, he);
    assert.deepEqual(pngInfo(he), { w: 1920, h: 1080, colorType: 6 });
    assert.equal(await page.locator('.cap').getAttribute('dir'), 'rtl');
    assert.equal(await page.locator('.cap bdi').innerText(), '+17.0');
    const font = await page.locator('.cap').evaluate((e) => getComputedStyle(e).fontFamily);
    assert.match(font, /Heebo/);

    const sq = join(TMP, 'sq.png');
    await renderOverlay(page, { kind: 'caption', lang: 'en', html: 'Hello', position: 'top', w: 1080, h: 1080 }, sq);
    assert.deepEqual(pngInfo(sq), { w: 1080, h: 1080, colorType: 6 });

    const card = join(TMP, 'card.png');
    await renderOverlay(page, { kind: 'card', lang: 'en', html: 'Who is helping my team?', icon: 'bi-person-fill', n: '2/9', w: 1920, h: 1080 }, card);
    assert.equal(pngInfo(card).w, 1920);
    assert.equal(await page.evaluate(() => document.fonts.check('700 84px "DM Sans"')), true);
  } finally {
    await browser.close();
  }
});
