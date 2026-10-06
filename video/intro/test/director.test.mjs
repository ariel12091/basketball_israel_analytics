import { test } from 'node:test';
import assert from 'node:assert/strict';
import { chromium } from 'playwright';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));

test('director finds cells, draws the ring and reports busy', async () => {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });
    await page.addInitScript({ path: join(HERE, '..', 'page', 'director.js') });
    await page.goto(pathToFileURL(join(HERE, 'fixtures', 'table.html')).href);
    await page.waitForFunction(() => window.jQuery?.fn?.dataTable?.isDataTable('#t'));

    assert.equal(await page.evaluate(() => window.__dir.tag({ cell: { table: '#dt', row: 'clark iii', col: 'net rtg  diff' } }, 'a')), true);
    assert.equal(await page.locator('[data-dir-target="a"]').innerText(), '+17.0');
    assert.equal(await page.evaluate(() => window.__dir.tag({ header: { table: '#dt', col: 'Net RTG Diff' } }, 'h')), true);
    assert.equal(await page.locator('[data-dir-target="h"]').innerText(), 'Net RTG Diff');
    assert.equal(await page.evaluate(() => window.__dir.tag({ cell: { table: '#dt', row: 'nobody', col: 'Net' } }, 'n')), false);
    assert.equal(await page.evaluate(() => window.__dir.tag({ cell: { table: '#dt', row: 'clark', col: 'Hidden' } }, 'x')), false);

    await page.evaluate(() => window.__dir.ring({ x: 100, y: 120, width: 50, height: 20 }));
    const ring = await page.locator('#__dir_ring').boundingBox();
    assert.deepEqual([ring.x, ring.y, ring.width, ring.height], [94, 114, 62, 32]);
    await page.evaluate(() => window.__dir.cursorTo(300, 200, 0));
    assert.ok(await page.locator('#__dir_cursor').isVisible());

    assert.equal(await page.evaluate(() => window.__dir.busy()), false);
    // Shiny leaves outputs on hidden tabs .recalculating for as long as they
    // stay suspended; only a visible one means the page is still loading.
    await page.evaluate(() => {
      const hidden = document.createElement('div');
      hidden.className = 'recalculating';
      hidden.style.display = 'none';
      hidden.textContent = 'x';
      document.body.appendChild(hidden);
    });
    assert.equal(await page.evaluate(() => window.__dir.busy()), false);
    await page.evaluate(() => document.querySelector('#t').classList.add('recalculating'));
    assert.equal(await page.evaluate(() => window.__dir.busy()), true);
    await page.evaluate(() => document.querySelector('#t').classList.remove('recalculating'));
    await page.evaluate(() => document.documentElement.classList.add('shiny-busy'));
    assert.equal(await page.evaluate(() => window.__dir.busy()), true);
  } finally {
    await browser.close();
  }
});
