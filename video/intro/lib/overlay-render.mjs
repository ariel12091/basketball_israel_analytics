import { mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));

export async function openOverlayPage(browser) {
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
  await page.goto(pathToFileURL(join(HERE, '..', 'page', 'overlay.html')).href, { waitUntil: 'networkidle' });
  await page.evaluate(() => Promise.all([
    document.fonts.load('600 40px "DM Sans"'), document.fonts.load('700 84px "DM Sans"'),
    document.fonts.load('600 40px "Heebo"', 'א'), document.fonts.load('700 84px "Heebo"', 'א'),
    document.fonts.load('600 30px "JetBrains Mono"'),
  ]));
  return page;
}

export async function renderOverlay(page, o, outPath) {
  await page.setViewportSize({ width: o.w, height: o.h });
  await page.evaluate((x) => window.__render(x), o);
  mkdirSync(dirname(outPath), { recursive: true });
  await page.locator('#stage').screenshot({ path: outPath, omitBackground: o.kind === 'caption' });
}
