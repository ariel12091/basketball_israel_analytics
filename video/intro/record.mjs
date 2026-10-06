#!/usr/bin/env node
// Drives the app through script.json and records each chapter as DevTools
// screencast frames plus a timeline of step times, element boxes and values.
// Usage: node record.mjs [--check] [--chapter id] [--url u] [--script p] [--out d] [--no-shiny]
import { chromium } from 'playwright';
import { mkdirSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { validateScript } from './lib/script.mjs';
import { extractValue, fillTemplate } from './lib/text.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const { values: opt } = parseArgs({
  options: {
    check: { type: 'boolean', default: false },
    chapter: { type: 'string' },
    url: { type: 'string', default: 'http://127.0.0.1:3838/' },
    script: { type: 'string', default: join(HERE, 'script.json') },
    out: { type: 'string', default: join(HERE, 'out') },
    'no-shiny': { type: 'boolean', default: false },
  },
});

const DEFAULT_VIEWPORT = { width: 1600, height: 900, dsf: 1.2, mobile: false };
const now = () => Date.now() / 1000;
const pause = (page, s) => page.waitForTimeout(Math.round(s * 1000));
let tagSeq = 0;

// The app debounces inputs ~300 ms before reloading, so the page reads idle
// for a moment after a change; wait past that before trusting busy().
async function settle(page) {
  await pause(page, 0.6);
  await page.waitForFunction(() => !window.__dir.busy(), null, { timeout: 60000, polling: 100 });
  await pause(page, 0.35);
}

async function locate(page, target) {
  if (typeof target === 'string') {
    const loc = page.locator(target).first();
    await loc.waitFor({ state: 'visible', timeout: 15000 });
    return loc;
  }
  // A DataTables redraw can replace the tagged node just after tagging, which
  // leaves the tag on a detached element; re-tag until it survives a beat.
  const deadline = Date.now() + 15000;
  for (;;) {
    const id = `t${tagSeq++}`;
    const tagged = await page.evaluate(([t, i]) => window.__dir.tag(t, i), [target, id]);
    if (tagged) {
      await pause(page, 0.3);
      const alive = await page.evaluate((i) => !!document.querySelector(`[data-dir-target="${i}"]`)?.isConnected, id);
      if (alive) return page.locator(`[data-dir-target="${id}"]`).first();
    }
    if (Date.now() > deadline) throw new Error(`target not found: ${JSON.stringify(target)}`);
    await pause(page, 0.25);
  }
}

async function moveTo(page, loc, fast) {
  await loc.scrollIntoViewIfNeeded();
  const b = await loc.boundingBox();
  if (!b) throw new Error('target has no box');
  const x = b.x + b.width / 2;
  const y = b.y + b.height / 2;
  if (!fast) {
    await page.evaluate(([px, py]) => window.__dir.cursorTo(px, py, 600), [x, y]);
    await pause(page, 0.65);
  }
  await page.mouse.move(x, y);
}

async function perform(page, s, fast) {
  switch (s.do) {
    case 'none': return;
    case 'wait': return pause(page, s.value ?? 1);
    case 'tab': {
      const loc = await locate(page, `.navbar a[data-value="${s.target}"]`);
      await moveTo(page, loc, fast);
      return loc.click();
    }
    case 'click': case 'hover': case 'scroll': {
      const loc = await locate(page, s.target);
      await moveTo(page, loc, fast);
      if (s.do === 'click') return loc.click();
      if (s.do === 'hover') return loc.hover();
      return page.mouse.wheel(0, s.value ?? 400);
    }
    case 'clear': {
      // Empty a selectize (e.g. the on/off team box, which defaults to a team).
      await locate(page, `${s.target} + .selectize-control`);
      return page.evaluate((sel) => document.querySelector(sel).selectize.clear(), s.target);
    }
    case 'selectize': {
      const ctl = await locate(page, `${s.target} + .selectize-control .selectize-input`);
      await moveTo(page, ctl, fast);
      // A multi-select may already hold a team (e.g. a remembered default);
      // clear:true makes the pick replace it instead of adding to it.
      if (s.clear) {
        await page.evaluate((sel) => document.querySelector(sel).selectize.clear(), s.target);
        await settle(page);
      }
      await ctl.click();
      await page.keyboard.type(s.value, { delay: fast ? 0 : 70 });
      const option = page.locator(`${s.target} + .selectize-control .selectize-dropdown .option`, { hasText: s.value }).first();
      await option.waitFor({ state: 'visible', timeout: 15000 });
      if (!fast) await pause(page, 0.3);
      await option.click();
      await page.keyboard.press('Escape');
      // Escape does not close a multi-select's dropdown; left open it covers
      // the next target and sits in the frame.
      return page.evaluate((sel) => document.querySelector(sel).selectize.blur(), s.target);
    }
    default: throw new Error(`unknown action ${s.do}`);
  }
}

function ringTargetOf(st) {
  if (st.ring === false) return null;
  if (st.ringTarget) return st.ringTarget;
  if (['none', 'wait', 'tab'].includes(st.do)) return null;
  return st.do === 'selectize' ? `${st.target} + .selectize-control` : st.target;
}

async function runStep(page, st, fast) {
  const rec = { id: st.id, t0: now(), bbox: null, values: {} };
  try {
    await perform(page, st, fast);
    await settle(page);
    const rt = ringTargetOf(st);
    if (rt) {
      const b = await (await locate(page, rt)).boundingBox();
      if (b) rec.bbox = { x: b.x, y: b.y, width: b.width, height: b.height };
      if (rec.bbox && !fast) await page.evaluate((r) => window.__dir.ring(r), rec.bbox);
    }
    for (const [k, r] of Object.entries(st.read ?? {})) {
      rec.values[k] = extractValue(await (await locate(page, r.target)).innerText(), r.pattern);
    }
    for (const lang of ['en', 'he']) fillTemplate(st[lang], rec.values);
  } catch (e) {
    throw new Error(`step ${st.id}: ${e.message}`);
  }
  rec.tFocus = now();
  if (!fast) await pause(page, st.hold);
  rec.t1 = now();
  if (!fast) {
    await page.evaluate(() => window.__dir.clearRing());
    await pause(page, 0.3);
  }
  return rec;
}

// The screencast ignores a context's deviceScaleFactor and captures CSS pixels;
// only the browser-wide flag makes it capture device pixels. So each chapter
// gets its own browser, launched at that chapter's scale.
async function openChapter(script, ch) {
  const vp = { ...DEFAULT_VIEWPORT, ...(ch.viewport ?? {}) };
  const browser = await chromium.launch({ args: [`--force-device-scale-factor=${vp.dsf}`] });
  const ctx = await browser.newContext({
    viewport: { width: vp.width, height: vp.height },
    isMobile: vp.mobile, hasTouch: vp.mobile, locale: 'en-US',
  });
  await ctx.addInitScript({ path: join(HERE, 'page', 'director.js') });
  const page = await ctx.newPage();
  await page.goto(opt.url, { timeout: 120000, waitUntil: 'load' });
  if (!opt['no-shiny']) {
    await page.waitForFunction(() => window.Shiny?.shinyapp?.isConnected?.(), null, { timeout: 120000 });
  }
  await settle(page);
  // A chapter can opt out of the global setup (the phone layout hides the
  // navbar season picker it drives).
  const global = ch.global_setup === false ? [] : (script.setup ?? []);
  for (const s of [...global, ...(ch.setup ?? [])]) {
    await perform(page, s, true);
    await settle(page);
  }
  await page.mouse.move(vp.width / 2, vp.height / 2);
  return { browser, ctx, page, vp };
}

async function checkChapter(script, ch) {
  const { browser, page } = await openChapter(script, ch);
  let failures = 0;
  for (const st of ch.steps) {
    try {
      const r = await runStep(page, st, true);
      const vals = Object.entries(r.values).map(([k, v]) => `${k}=${v}`).join(' ');
      const box = r.bbox ? `${Math.round(r.bbox.x)},${Math.round(r.bbox.y)} ${Math.round(r.bbox.width)}x${Math.round(r.bbox.height)}` : '-';
      console.log(`ok    ${st.id}  box=${box}  ${vals}`);
    } catch (e) {
      failures++;
      console.log(`FAIL  ${st.id}  ${e.message}`);
    }
  }
  await browser.close();
  return failures;
}

async function recordChapter(script, ch) {
  const { browser, ctx, page, vp } = await openChapter(script, ch);
  const dir = join(opt.out, 'rec', ch.id);
  rmSync(dir, { recursive: true, force: true });
  mkdirSync(join(dir, 'frames'), { recursive: true });
  const cdp = await ctx.newCDPSession(page);
  const frames = [];
  let k = 0;
  cdp.on('Page.screencastFrame', ({ data, metadata, sessionId }) => {
    const file = `f${String(k++).padStart(6, '0')}.jpg`;
    writeFileSync(join(dir, 'frames', file), Buffer.from(data, 'base64'));
    frames.push({ file, ts: metadata.timestamp });
    cdp.send('Page.screencastFrameAck', { sessionId }).catch(() => {});
  });
  await cdp.send('Page.startScreencast', {
    format: 'jpeg', quality: 92, everyNthFrame: 1,
    maxWidth: Math.round(vp.width * vp.dsf), maxHeight: Math.round(vp.height * vp.dsf),
  });
  await pause(page, 0.8);
  const steps = [];
  for (const st of ch.steps) steps.push(await runStep(page, st, false));
  await pause(page, 0.5);
  const tEnd = now();
  await cdp.send('Page.stopScreencast');
  writeFileSync(join(dir, 'timeline.json'), JSON.stringify({ chapter: ch.id, viewport: vp, frames, tEnd, steps }, null, 1));
  console.log(`recorded ${ch.id}: ${frames.length} frames, ${(tEnd - (frames[0]?.ts ?? tEnd)).toFixed(1)}s`);
  await browser.close();
  return 0;
}

const script = JSON.parse(readFileSync(opt.script, 'utf8'));
const errors = validateScript(script);
if (errors.length) {
  console.error(errors.join('\n'));
  process.exit(1);
}
let failures = 0;
for (const ch of script.chapters) {
  if (opt.chapter && ch.id !== opt.chapter) continue;
  try {
    failures += opt.check ? await checkChapter(script, ch) : await recordChapter(script, ch);
  } catch (e) {
    failures++;
    console.error(`chapter ${ch.id}: ${e.message}`);
  }
}
process.exit(failures ? 1 : 0);
