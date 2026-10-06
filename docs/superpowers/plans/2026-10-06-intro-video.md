# Intro Video Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a captioned ~4.5-minute tutorial and a <= 60 s highlight cut of IBPL Analytics, in English and Hebrew, recorded from the real app.

**Architecture:** One `script.json` describes every chapter and step. `record.mjs` drives the local Shiny app with Playwright, captures DevTools screencast frames and writes a timeline (step times, element boxes, values read off the page). `overlays.mjs` renders captions and cards as PNGs per language; `compose.mjs` turns frames + timeline into zoomed clean video with ffmpeg, overlays the captions, and concatenates chapters. `verify.mjs` extracts a frame per step for inspection and checks durations.

**Tech Stack:** Node 22 (ESM, `node:test`), Playwright (Chromium), ffmpeg 7.1 (gyan full build at `C:\ffmpeg\bin`), Google Fonts (DM Sans, Heebo, JetBrains Mono), Bootstrap Icons.

**Spec:** `docs/superpowers/specs/2026-10-06-intro-video-design.md` (read the Amendments section too).

## Global Constraints

- All new files live under `video/intro/`. Rendered output goes to `video/intro/out/` (gitignored). Never commit video or frames.
- Branch `infra/intro-video`. The working tree holds the user's unrelated uncommitted edits: **stage only files under `video/intro/` and `docs/superpowers/`** (`git add <explicit paths>`, never `git add -A` / `.`).
- Do not create worktrees or extra folders outside `video/intro/` (user preference).
- Run `npm` / `npx` through the **PowerShell** tool, not the Bash tool (npx path mangling on this machine). `node` and `ffmpeg` work in either.
- Recording uses the local app per the `run-shiny-local` project skill: `IBPL_CACHE_UI=false`, port 3838, launched with `shiny::runApp`, health check passed first.
- Season 2025-26 (`game_year = 2026`, label `25-26`); team MACCABI TEL AVIV.
- Frame 1920x1080 @ 30 fps; app viewport 1600x900 at device scale 1.2; short 1080x1080.
- Colours: background `#14100C`, accent `#e8a435`. Fonts: DM Sans (EN text), Heebo (HE text), JetBrains Mono (numbers on cards).
- Captions: <= 14 words, >= 1.5 s hold per step, zoom <= 1.6x and only with hold >= 2 s.
- Numbers in captions come from `read` entries filled at record time, never typed.
- URL on the outro card: `https://arieltaieb-basketball-israel-analytics.share.connect.posit.cloud/`.
- Tutorial must land within 210-360 s; short must be <= 60 s.

## Review Focus

1. **A caption template whose value was not read** (selector moved, cell empty) must stop the recorder at that step with the step id, never render `{net}` or an empty number. -> Task 2 (`fillTemplate` throws) + Task 8 (recorder fills both languages at record time).
2. **Recording while the app is still loading** (shiny-busy / `.recalculating`) must wait, and a target that never appears must fail the chapter, not record a wrong frame. -> Task 6 (`__dir.busy()`), Task 8 (fixture test with a delayed element and a missing one).
3. **Hebrew captions with Latin names, signed numbers and UI labels** (`Clark III: +17.0`, `Starters vs Bench.`) must keep the sign before the number and sentence punctuation on the Hebrew side. -> Task 2 bidi tests.
4. **A highlighted element near the frame edge or in the lower third** must stay inside the zoom window and push the caption to the top. -> Task 3 tests.
5. **The short running over 60 s** must be refused before encoding. -> Task 9 (`shortPlan` throws) test.

---

### Task 1: Scaffold and script validator

**Files:**
- Create: `video/intro/package.json`, `video/intro/.gitignore`, `video/intro/lib/script.mjs`
- Test: `video/intro/test/script.test.mjs`

**Interfaces:**
- Produces: `validateScript(script) -> string[]` (empty = valid), `placeholders(text) -> string[]`, `wordCount(text) -> number`, constants `ACTIONS`, `MAX_WORDS=14`, `MIN_HOLD=1.5`, `MAX_ZOOM=1.6`.
- Script schema (used by every later task):
  - top level: `url`, `setup: Action[]`, `title_card: {en:{title,sub}, he:{title,sub}}`, `outro_card: {en:{title}, he:{title}}`, `short: {hook_en, hook_he}`, `chapters: Chapter[]`
  - `Chapter`: `id`, `icon` (bootstrap icon class), `title_en`, `title_he`, `card` (default true), `viewport?` `{width,height,dsf,mobile}`, `setup?: Action[]`, `steps: Step[]`
  - `Action`: `do` in `none|wait|click|hover|selectize|scroll|tab`, `target?` (Target), `value?`
  - `Target`: CSS/Playwright selector string, or `{cell:{table,row,col}}`, or `{header:{table,col}}`
  - `Step` = Action + `id`, `hold`, `en`, `he`, `ring?` (false disables), `ringTarget?`, `zoom?`, `read?: {name: {target, pattern?}}`, `short?` (integer order)

- [ ] **Step 1: Create the package and ignore file**

`video/intro/package.json`:
```json
{
  "name": "ibpl-intro-video",
  "private": true,
  "type": "module",
  "scripts": {
    "test": "node --test"
  },
  "dependencies": {
    "playwright": "^1.55.0"
  }
}
```

`video/intro/.gitignore`:
```
node_modules/
out/
test/tmp/
```

- [ ] **Step 2: Install dependencies (PowerShell tool)**

```powershell
Set-Location video/intro; npm install; npx playwright install chromium
```
Expected: `added N packages`, chromium already present or downloaded.

- [ ] **Step 3: Write the failing test**

`video/intro/test/script.test.mjs`:
```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateScript, placeholders, wordCount } from '../lib/script.mjs';

const base = () => ({
  url: 'https://example.test/',
  setup: [{ do: 'selectize', target: '#game_year', value: '25-26' }],
  title_card: { en: { title: 'T', sub: 'S' }, he: { title: 'כותרת', sub: 'משנה' } },
  outro_card: { en: { title: 'Try it' }, he: { title: 'נסו' } },
  short: { hook_en: 'Hook', hook_he: 'פתיח' },
  chapters: [{
    id: 'c1', icon: 'bi-person-fill', title_en: 'Q?', title_he: 'שאלה?',
    steps: [{ id: 's1', do: 'hover', target: '#x', hold: 3, en: 'Net is {net}', he: 'נטו {net}', read: { net: { target: '#x' } } }],
  }],
});

test('a well-formed script has no errors', () => {
  assert.deepEqual(validateScript(base()), []);
});

test('placeholders and word counts', () => {
  assert.deepEqual(placeholders('a {net} b {rank}'), ['net', 'rank']);
  assert.equal(wordCount('Green helps, red hurts — the brighter, the bigger.'), 8);
});

test('every rule reports its step', () => {
  const s = base();
  const st = s.chapters[0].steps[0];
  st.he = 'no hebrew {other}';
  st.hold = 1;
  st.zoom = 2;
  st.do = 'jump';
  const errs = validateScript(s).join('\n');
  assert.match(errs, /c1\/s1: unknown action "jump"/);
  assert.match(errs, /c1\/s1: hold must be/);
  assert.match(errs, /c1\/s1: zoom must be within/);
  assert.match(errs, /c1\/s1: a zoomed step needs hold >= 2/);
  assert.match(errs, /c1\/s1: he caption has no Hebrew/);
  assert.match(errs, /c1\/s1: placeholders differ/);
  assert.match(errs, /c1\/s1: \{other\} has no read entry/);
});

test('duplicate ids, long captions, missing selectize value, duplicate short order', () => {
  const s = base();
  const a = s.chapters[0].steps[0];
  a.short = 1;
  s.chapters[0].steps.push({ ...a, en: 'one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen' });
  s.chapters[0].setup = [{ do: 'selectize', target: '#teams' }];
  const errs = validateScript(s).join('\n');
  assert.match(errs, /duplicate step id/);
  assert.match(errs, /en caption has 15 words/);
  assert.match(errs, /c1\/setup\[0\]: selectize needs a value/);
  assert.match(errs, /short orders must be unique/);
});

test('missing top-level keys and empty chapters', () => {
  assert.match(validateScript({ chapters: [] }).join('\n'), /script has no chapters/);
  assert.match(validateScript({ chapters: [] }).join('\n'), /script: missing url/);
});
```

- [ ] **Step 4: Run test to verify it fails**

Run (in `video/intro`): `node --test test/script.test.mjs`
Expected: FAIL — `Cannot find module '../lib/script.mjs'`.

- [ ] **Step 5: Implement**

`video/intro/lib/script.mjs`:
```js
// Validates script.json, the single source of truth for the film.
export const ACTIONS = new Set(['none', 'wait', 'click', 'hover', 'selectize', 'scroll', 'tab']);
export const MAX_WORDS = 14;
export const MIN_HOLD = 1.5;
export const MAX_ZOOM = 1.6;
const PLACEHOLDER = /\{([a-z_][a-z0-9_]*)\}/gi;
const HEBREW = /[\u0590-\u05FF]/;

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
```

- [ ] **Step 6: Run test to verify it passes**

Run: `node --test test/script.test.mjs` — Expected: all 5 tests pass.

- [ ] **Step 7: Commit**

```bash
git add video/intro/package.json video/intro/package-lock.json video/intro/.gitignore video/intro/lib/script.mjs video/intro/test/script.test.mjs
git commit -m "Intro video: scaffold and script validator"
```

---

### Task 2: Caption text — templates, value extraction, Hebrew bidi

**Files:**
- Create: `video/intro/lib/text.mjs`
- Test: `video/intro/test/text.test.mjs`

**Interfaces:**
- Produces: `fillTemplate(tpl, values) -> string` (throws on missing/blank value), `extractValue(text, pattern?) -> string` (throws if pattern absent), `escapeHtml(s)`, `captionHtml(text, lang) -> string` (Hebrew: Latin runs wrapped in `<bdi dir="ltr">`).

- [ ] **Step 1: Write the failing test**

`video/intro/test/text.test.mjs`:
```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fillTemplate, extractValue, captionHtml, escapeHtml } from '../lib/text.mjs';

test('fillTemplate fills and refuses missing or blank values', () => {
  assert.equal(fillTemplate('Clark III: {net} pts', { net: '+17.0' }), 'Clark III: +17.0 pts');
  assert.throws(() => fillTemplate('x {net}', {}), /template value "net" missing/);
  assert.throws(() => fillTemplate('x {net}', { net: '  ' }), /template value "net" missing/);
});

test('extractValue takes the first match of the pattern from cell text', () => {
  assert.equal(extractValue('+17.0\n3PT luck', '[+\\-−]?\\d+(\\.\\d+)?'), '+17.0');
  assert.equal(extractValue('  MACCABI   TEL AVIV '), 'MACCABI TEL AVIV');
  assert.throws(() => extractValue('n/a', '\\d+'), /pattern/);
});

test('English captions are only escaped', () => {
  assert.equal(captionHtml('A & B <c>', 'en'), 'A &amp; B &lt;c&gt;');
  assert.equal(escapeHtml('"q"'), '&quot;q&quot;');
});

test('Hebrew captions isolate Latin runs, keeping signs and sentence punctuation outside', () => {
  assert.equal(
    captionHtml("ג'ימי קלארק: Clark III: +17.0 נקודות", 'he'),
    "ג'ימי קלארק: <bdi dir=\"ltr\">Clark III: +17.0</bdi> נקודות",
  );
  assert.equal(
    captionHtml('בחירה מהירה: Starters vs Bench.', 'he'),
    'בחירה מהירה: <bdi dir="ltr">Starters vs Bench</bdi>.',
  );
  assert.equal(captionHtml('ל־100 פוזשנים', 'he'), 'ל־<bdi dir="ltr">100</bdi> פוזשנים');
  assert.equal(captionHtml('A & B', 'he'), '<bdi dir="ltr">A</bdi> &amp; <bdi dir="ltr">B</bdi>');
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/text.test.mjs` — Expected: FAIL, module not found.

- [ ] **Step 3: Implement**

`video/intro/lib/text.mjs`:
```js
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/text.test.mjs` — Expected: 4 pass. If a bidi case fails, fix the regex, not the expectation: the expectations encode Review Focus #3.

- [ ] **Step 5: Commit**

```bash
git add video/intro/lib/text.mjs video/intro/test/text.test.mjs
git commit -m "Intro video: caption templates and Hebrew bidi isolation"
```

---

### Task 3: Geometry — zoom window, caption position, square crop

**Files:**
- Create: `video/intro/lib/geometry.mjs`
- Test: `video/intro/test/geometry.test.mjs`

**Interfaces:**
- Produces (boxes are `{x,y,width,height}`):
  - `FRAME = {w:1920,h:1080}`
  - `scaleBox(box, f)`, `applyTransform(box, t)`
  - `zoomTransform(box, W, H, scale) -> {scale, tx, ty}` (screen = scale*p + t; element centred, clamped so the zoomed frame never shows outside the page)
  - `focusPlan(bbox, viewport, zoom) -> {box, zoom: {scale, xf, yf} | null}` — `bbox` in viewport CSS px; `box` in frame px at full zoom; `xf,yf` = zoom window top-left in frame px. Throws if `zoom > 1` and no bbox.
  - `captionPosition(box, frameH) -> 'top'|'bottom'`
  - `squareCropX(box, frameW, side) -> int`

- [ ] **Step 1: Write the failing test**

`video/intro/test/geometry.test.mjs`:
```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { zoomTransform, applyTransform, focusPlan, captionPosition, squareCropX, scaleBox } from '../lib/geometry.mjs';

const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-6, `${a} != ${b}`);

test('zoom centres a mid-frame element', () => {
  const box = { x: 900, y: 500, width: 120, height: 80 };
  const t = zoomTransform(box, 1920, 1080, 1.5);
  const z = applyTransform(box, t);
  near(z.x + z.width / 2, 960);
  near(z.y + z.height / 2, 540);
});

test('zoom near the edge is clamped inside the frame', () => {
  const t = zoomTransform({ x: 5, y: 1040, width: 40, height: 30 }, 1920, 1080, 1.6);
  assert.equal(t.tx, 0);
  near(t.ty, 1080 - 1.6 * 1080);
  const p = focusPlan({ x: 1580, y: 10, width: 15, height: 10 }, { width: 1600, height: 900 }, 1.6);
  assert.ok(p.zoom.xf >= 0 && p.zoom.xf <= 1920 - 1920 / 1.6 + 1e-6);
  assert.ok(p.zoom.yf >= 0 && p.zoom.yf <= 1080 - 1080 / 1.6 + 1e-6);
  assert.ok(p.box.x + p.box.width <= 1920 + 1e-6);
});

test('focusPlan scales viewport px to frame px and handles no zoom / no box', () => {
  const p = focusPlan({ x: 100, y: 100, width: 100, height: 50 }, { width: 1600, height: 900 });
  assert.deepEqual(p, { box: scaleBox({ x: 100, y: 100, width: 100, height: 50 }, 1.2), zoom: null });
  assert.deepEqual(focusPlan(null, { width: 1600, height: 900 }), { box: null, zoom: null });
  assert.throws(() => focusPlan(null, { width: 1600, height: 900 }, 1.4), /zoom needs a bbox/);
  assert.deepEqual(focusPlan({ x: 1, y: 1, width: 1, height: 1 }, { width: 390, height: 844, mobile: true }), { box: null, zoom: null });
});

test('caption moves to the top when the focus is in the lower third', () => {
  assert.equal(captionPosition(null, 1080), 'bottom');
  assert.equal(captionPosition({ x: 0, y: 100, width: 10, height: 10 }, 1080), 'bottom');
  assert.equal(captionPosition({ x: 0, y: 700, width: 10, height: 80 }, 1080), 'top');
});

test('square crop follows the focus and stays inside the frame', () => {
  assert.equal(squareCropX(null, 1920, 1080), 420);
  assert.equal(squareCropX({ x: 0, y: 0, width: 50, height: 50 }, 1920, 1080), 0);
  assert.equal(squareCropX({ x: 1900, y: 0, width: 20, height: 20 }, 1920, 1080), 840);
  assert.equal(squareCropX({ x: 1000, y: 0, width: 100, height: 20 }, 1920, 1080), 510);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/geometry.test.mjs` — Expected: FAIL, module not found.

- [ ] **Step 3: Implement**

`video/intro/lib/geometry.mjs`:
```js
// Frame geometry shared by overlays.mjs and compose.mjs, so caption placement
// and zoom always agree.
export const FRAME = { w: 1920, h: 1080 };

export function scaleBox(b, f) {
  return { x: b.x * f, y: b.y * f, width: b.width * f, height: b.height * f };
}

export function applyTransform(b, t) {
  return { x: t.scale * b.x + t.tx, y: t.scale * b.y + t.ty, width: t.scale * b.width, height: t.scale * b.height };
}

export function zoomTransform(b, W, H, scale) {
  if (!(scale > 1)) return { scale: 1, tx: 0, ty: 0 };
  const cx = b.x + b.width / 2;
  const cy = b.y + b.height / 2;
  const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
  return {
    scale,
    tx: clamp(W / 2 - scale * cx, W - scale * W, 0),
    ty: clamp(H / 2 - scale * cy, H - scale * H, 0),
  };
}

export function focusPlan(bbox, viewport, zoom, frame = FRAME) {
  if (viewport?.mobile) return { box: null, zoom: null };
  if (!bbox) {
    if (zoom > 1) throw new Error('zoom needs a bbox');
    return { box: null, zoom: null };
  }
  const box = scaleBox(bbox, frame.w / viewport.width);
  if (!(zoom > 1)) return { box, zoom: null };
  const t = zoomTransform(box, frame.w, frame.h, zoom);
  return { box: applyTransform(box, t), zoom: { scale: zoom, xf: -t.tx / zoom, yf: -t.ty / zoom } };
}

export function captionPosition(box, frameH) {
  if (!box) return 'bottom';
  return box.y + box.height > (frameH * 2) / 3 ? 'top' : 'bottom';
}

export function squareCropX(box, frameW, side) {
  const cx = box ? box.x + box.width / 2 : frameW / 2;
  return Math.round(Math.min(frameW - side, Math.max(0, cx - side / 2)));
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/geometry.test.mjs` — Expected: 5 pass.

- [ ] **Step 5: Commit**

```bash
git add video/intro/lib/geometry.mjs video/intro/test/geometry.test.mjs
git commit -m "Intro video: zoom window, caption placement and square crop geometry"
```

---

### Task 4: Timeline helpers, YouTube chapters, caption review table

**Files:**
- Create: `video/intro/lib/timeline.mjs`, `video/intro/lib/chapters.mjs`, `video/intro/lib/review.mjs`
- Test: `video/intro/test/timeline.test.mjs`

**Interfaces:**
- Consumes: `fillTemplate` (Task 2).
- Produces:
  - `relSteps(tl) -> [{id, a, focus, b, bbox, values}]` — seconds relative to `tl.frames[0].ts`; throws if no frames.
  - `stepRecord(tl, id)` — throws `step <id> missing from timeline <chapter>`.
  - `readTimeline(outDir, chapterId)` — reads `out/rec/<id>/timeline.json`, throws with "run record.mjs first".
  - `fmtTime(sec)`, `mergeShortChapters(entries)` (entry < 10 s merged into the next; first stays at 0), `youtubeChapters(entries) -> string` (entries `{title,start,end}`).
  - `captionsReviewMarkdown(script, valuesById) -> string`.

- [ ] **Step 1: Write the failing test**

`video/intro/test/timeline.test.mjs`:
```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { relSteps, stepRecord } from '../lib/timeline.mjs';
import { fmtTime, mergeShortChapters, youtubeChapters } from '../lib/chapters.mjs';
import { captionsReviewMarkdown } from '../lib/review.mjs';

const tl = {
  chapter: 'c1',
  frames: [{ file: 'f0.jpg', ts: 1000 }, { file: 'f1.jpg', ts: 1000.5 }],
  tEnd: 1010,
  steps: [{ id: 's1', t0: 1001, tFocus: 1002, t1: 1004, bbox: null, values: { net: '+17.0' } }],
};

test('relSteps is relative to the first frame', () => {
  assert.deepEqual(relSteps(tl), [{ id: 's1', a: 1, focus: 2, b: 4, bbox: null, values: { net: '+17.0' } }]);
  assert.throws(() => relSteps({ ...tl, frames: [] }), /no frames/);
  assert.equal(stepRecord(tl, 's1').values.net, '+17.0');
  assert.throws(() => stepRecord(tl, 'zz'), /step zz missing from timeline c1/);
});

test('chapter times and YouTube rules', () => {
  assert.equal(fmtTime(0), '0:00');
  assert.equal(fmtTime(75.9), '1:15');
  assert.equal(fmtTime(3725), '1:02:05');
  const merged = mergeShortChapters([
    { title: 'Intro', start: 0, end: 8 },
    { title: 'Home', start: 8, end: 33 },
    { title: 'On/Off', start: 33, end: 90 },
    { title: 'Lineups', start: 90, end: 140 },
  ]);
  assert.deepEqual(merged.map((e) => [e.title, e.start]), [['Intro', 0], ['On/Off', 33], ['Lineups', 90]]);
  assert.equal(youtubeChapters(merged), '0:00 Intro\n0:33 On/Off\n1:30 Lineups\n');
  assert.throws(() => youtubeChapters(merged.slice(0, 2)), /at least 3/);
});

test('review table fills values and escapes pipes', () => {
  const script = { chapters: [{ id: 'c1', title_en: 'Q?', title_he: 'שאלה?', steps: [{ id: 's1', en: 'Net {net} | x', he: 'נטו {net}' }] }] };
  const md = captionsReviewMarkdown(script, { s1: { net: '+17.0' } });
  assert.match(md, /\| s1 \| Net \+17\.0 \\\| x \| נטו \+17\.0 \|/);
  assert.match(md, /## c1 — Q\? \/ שאלה\?/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/timeline.test.mjs` — Expected: FAIL, modules not found.

- [ ] **Step 3: Implement**

`video/intro/lib/timeline.mjs`:
```js
import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';

export function readTimeline(outDir, chapterId) {
  const p = join(outDir, 'rec', chapterId, 'timeline.json');
  if (!existsSync(p)) throw new Error(`no timeline for chapter ${chapterId} (${p}); run record.mjs first`);
  return JSON.parse(readFileSync(p, 'utf8'));
}

export function relSteps(tl) {
  if (!tl.frames?.length) throw new Error(`no frames recorded for chapter ${tl.chapter}`);
  const z = tl.frames[0].ts;
  return tl.steps.map((s) => ({ id: s.id, a: s.t0 - z, focus: s.tFocus - z, b: s.t1 - z, bbox: s.bbox, values: s.values }));
}

export function stepRecord(tl, id) {
  const s = tl.steps.find((x) => x.id === id);
  if (!s) throw new Error(`step ${id} missing from timeline ${tl.chapter}`);
  return s;
}
```

`video/intro/lib/chapters.mjs`:
```js
export function fmtTime(sec) {
  const s = Math.floor(sec);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  const p2 = (n) => String(n).padStart(2, '0');
  return h ? `${h}:${p2(m % 60)}:${p2(s % 60)}` : `${m}:${p2(s % 60)}`;
}

// YouTube ignores chapter lists with an entry under 10 s. A short entry is
// absorbed by the one after it; the first entry keeps its title and 0:00.
export function mergeShortChapters(entries) {
  const out = [];
  for (const e of entries) {
    const prev = out.at(-1);
    if (prev && prev.end - prev.start < 10) {
      if (out.length === 1) prev.end = e.end;
      else { out.pop(); out.push({ ...e, start: prev.start }); }
    } else out.push({ ...e });
  }
  return out;
}

export function youtubeChapters(entries) {
  if (entries.length < 3) throw new Error('YouTube needs at least 3 chapters');
  if (entries[0].start !== 0) throw new Error('first chapter must start at 0:00');
  entries.forEach((e, i) => {
    const end = entries[i + 1]?.start ?? e.end;
    if (end - e.start < 10) throw new Error(`chapter "${e.title}" is ${(end - e.start).toFixed(1)}s; YouTube needs >= 10s`);
  });
  return entries.map((e) => `${fmtTime(e.start)} ${e.title}`).join('\n') + '\n';
}
```

Note the expected test result: `Intro` (0-8, short) absorbs `Home` (8-33) as the first entry, so it keeps `Intro` at 0 and runs to 33; `On/Off` and `Lineups` follow unchanged.

`video/intro/lib/review.mjs`:
```js
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/timeline.test.mjs` — Expected: 3 pass.

- [ ] **Step 5: Commit**

```bash
git add video/intro/lib/timeline.mjs video/intro/lib/chapters.mjs video/intro/lib/review.mjs video/intro/test/timeline.test.mjs
git commit -m "Intro video: timeline, YouTube chapter and caption review helpers"
```

---

### Task 5: ffmpeg graph builders

**Files:**
- Create: `video/intro/lib/ffmpeg.mjs`
- Test: `video/intro/test/ffmpeg.test.mjs`

**Interfaces:**
- Produces:
  - `FPS=30`, `ENC` (x264 args), `ZOOM_IN=0.6`, `ZOOM_OUT=0.5`
  - `ff(args)` (spawns ffmpeg, throws on non-zero), `probeDuration(file) -> seconds`
  - `concatList(frames, tEnd) -> string` (ffconcat for `{file, ts}` frames, absolute forward-slash paths)
  - `zoompanFilter(zooms) -> string` — `zooms: [{a, b, scale, xf, yf}]` (seconds, frame px)
  - `cleanGraph(zooms, mobile) -> string` — full `[0:v]...[out]` graph
  - `captionGraph(caps) -> {graph, out}` — `caps: [{a, b}]`, caption PNGs are inputs 1..n
  - `writeGraph(path, graph)`

- [ ] **Step 1: Write the failing test**

`video/intro/test/ffmpeg.test.mjs`:
```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { concatList, zoompanFilter, cleanGraph, captionGraph, ff, probeDuration, writeGraph, ENC } from '../lib/ffmpeg.mjs';

const TMP = join(dirname(fileURLToPath(import.meta.url)), 'tmp', 'ffmpeg');

test('concatList gives each frame its time until the next', () => {
  const s = concatList([{ file: 'C:/a/f0.jpg', ts: 10 }, { file: 'C:/a/f1.jpg', ts: 10.5 }], 12);
  assert.equal(s, "ffconcat version 1.0\nfile 'C:/a/f0.jpg'\nduration 0.5000\nfile 'C:/a/f1.jpg'\nduration 1.5000\nfile 'C:/a/f1.jpg'\n");
  assert.throws(() => concatList([], 1), /no frames/);
});

test('zoompan without zooms is identity', () => {
  assert.equal(zoompanFilter([]), 'zoompan=z=1:x=0:y=0:d=1:s=1920x1080:fps=30');
  const f = zoompanFilter([{ a: 1, b: 4, scale: 1.5, xf: 300, yf: 200 }]);
  assert.match(f, /^zoompan=z='1\/\(1-\(/);
  assert.match(f, /iw\/1920/);
  assert.match(f, /ih\/1080/);
});

test('ffmpeg accepts the clean graph with a zoom and the caption graph', () => {
  rmSync(TMP, { recursive: true, force: true });
  mkdirSync(TMP, { recursive: true });
  const src = join(TMP, 'src.mp4');
  ff(['-f', 'lavfi', '-i', 'testsrc2=s=1920x1080:r=30:d=3', ...ENC, src]);
  const g = join(TMP, 'clean.txt');
  writeGraph(g, cleanGraph([{ a: 0.5, b: 2.8, scale: 1.5, xf: 400, yf: 200 }], false));
  const clean = join(TMP, 'clean.mp4');
  ff(['-i', src, '-/filter_complex', g, '-map', '[out]', ...ENC, clean]);
  assert.ok(Math.abs(probeDuration(clean) - 3) < 0.15);

  const png = join(TMP, 'cap.png');
  ff(['-f', 'lavfi', '-i', 'color=c=red@0.5:s=1920x1080,format=rgba', '-frames:v', '1', png]);
  const { graph, out } = captionGraph([{ a: 0.2, b: 2.5 }]);
  const cg = join(TMP, 'cap.txt');
  writeGraph(cg, graph);
  const final = join(TMP, 'final.mp4');
  ff(['-i', clean, '-loop', '1', '-framerate', '30', '-t', '3', '-i', png, '-/filter_complex', cg, '-map', out, ...ENC, final]);
  assert.ok(Math.abs(probeDuration(final) - 3) < 0.15);
});

test('mobile chapters are padded, not zoomed', () => {
  assert.match(cleanGraph([], true), /pad=1920:1080/);
  assert.throws(() => cleanGraph([{ a: 0, b: 2, scale: 1.2, xf: 0, yf: 0 }], true), /mobile/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/ffmpeg.test.mjs` — Expected: FAIL, module not found.

- [ ] **Step 3: Implement**

`video/intro/lib/ffmpeg.mjs`:
```js
import { spawnSync } from 'node:child_process';
import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export const FPS = 30;
export const ZOOM_IN = 0.6;
export const ZOOM_OUT = 0.5;
export const ENC = ['-c:v', 'libx264', '-preset', 'medium', '-crf', '18', '-pix_fmt', 'yuv420p', '-r', String(FPS), '-an'];

export function ff(args) {
  const r = spawnSync('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', ...args], { stdio: 'inherit' });
  if (r.status !== 0) throw new Error(`ffmpeg failed (${r.status}): ffmpeg ${args.join(' ')}`);
}

export function probeDuration(file) {
  const r = spawnSync('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'csv=p=0', file], { encoding: 'utf8' });
  const d = parseFloat(r.stdout);
  if (r.status !== 0 || Number.isNaN(d)) throw new Error(`ffprobe failed on ${file}: ${r.stderr}`);
  return d;
}

export function writeGraph(path, graph) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, graph);
}

export function concatList(frames, tEnd) {
  if (!frames.length) throw new Error('no frames captured');
  const lines = ['ffconcat version 1.0'];
  frames.forEach((f, i) => {
    const next = i + 1 < frames.length ? frames[i + 1].ts : Math.max(tEnd, f.ts + 1 / FPS);
    lines.push(`file '${f.file}'`, `duration ${Math.max(next - f.ts, 0.001).toFixed(4)}`);
  });
  lines.push(`file '${frames.at(-1).file}'`);
  return lines.join('\n') + '\n';
}

const smooth = (u) => `(${u})*(${u})*(3-2*(${u}))`;

function progress(z) {
  const T = `(in/${FPS})`;
  const a = z.a.toFixed(3);
  const b = z.b.toFixed(3);
  const up = `clip((${T}-${a})/${ZOOM_IN},0,1)`;
  const down = `clip((${b}-${T})/${ZOOM_OUT},0,1)`;
  return `between(${T},${a},${b})*min(${smooth(up)},${smooth(down)})`;
}

// Progress p eases 0 -> 1 -> 0 over [a, b]. The window's top-left moves
// linearly from (0,0) to (xf,yf) and its size from the frame to frame/scale,
// which is zoom z = 1 / (1 - p(1 - 1/scale)).
export function zoompanFilter(zooms) {
  const tail = `d=1:s=1920x1080:fps=${FPS}`;
  if (!zooms.length) return `zoompan=z=1:x=0:y=0:${tail}`;
  const P = zooms.map(progress);
  const k = zooms.map((z, i) => `${P[i]}*${(1 - 1 / z.scale).toFixed(5)}`).join('+');
  const x = zooms.map((z, i) => `${P[i]}*${z.xf.toFixed(1)}`).join('+');
  const y = zooms.map((z, i) => `${P[i]}*${z.yf.toFixed(1)}`).join('+');
  return `zoompan=z='1/(1-(${k}))':x='(iw/1920)*(${x})':y='(ih/1080)*(${y})':${tail}`;
}

export function cleanGraph(zooms, mobile) {
  if (mobile) {
    if (zooms.length) throw new Error('mobile chapters cannot zoom');
    return `[0:v]fps=${FPS},scale=-2:1080,pad=1920:1080:(ow-iw)/2:0:color=0x14100C,setsar=1[out]`;
  }
  return `[0:v]fps=${FPS},scale=3840:2160:flags=lanczos,${zoompanFilter(zooms)},setsar=1[out]`;
}

export function captionGraph(caps) {
  const parts = ['[0:v]null[v0]'];
  caps.forEach((c, i) => {
    const n = i + 1;
    const a = c.a.toFixed(3);
    const b = c.b.toFixed(3);
    const fo = Math.max(c.a, c.b - 0.25).toFixed(3);
    parts.push(`[${n}:v]format=rgba,fade=t=in:st=${a}:d=0.25:alpha=1,fade=t=out:st=${fo}:d=0.25:alpha=1[c${n}]`);
    parts.push(`[v${i}][c${n}]overlay=0:0:enable='between(t,${a},${b})'[v${n}]`);
  });
  return { graph: parts.join(';\n'), out: `[v${caps.length}]` };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/ffmpeg.test.mjs` — Expected: 4 pass. If ffmpeg rejects `-/filter_complex` ("Unrecognized option"), the build predates 7.0: switch `ff` call sites to `-filter_complex_script` and re-run.

- [ ] **Step 5: Commit**

```bash
git add video/intro/lib/ffmpeg.mjs video/intro/test/ffmpeg.test.mjs
git commit -m "Intro video: ffmpeg zoom, caption and concat builders"
```

---

### Task 6: In-page director (cursor, ring, DataTables lookup)

**Files:**
- Create: `video/intro/page/director.js`, `video/intro/test/fixtures/table.html`
- Test: `video/intro/test/director.test.mjs`

**Interfaces:**
- Produces `window.__dir` in every recorded page:
  - `cursorTo(x, y, ms=600)`, `ring(rect, pad=6)`, `clearRing()`
  - `tag(target, id) -> boolean` — finds `{cell:{table,row,col}}` / `{header:{table,col}}` via the DataTables API (visible column whose header text contains `col`, case/space-insensitive; first current-page row whose text contains `row`) and sets `data-dir-target=id`
  - `busy() -> boolean` — `html.shiny-busy` or any `.recalculating`

- [ ] **Step 1: Create the fixture**

`video/intro/test/fixtures/table.html`:
```html
<!doctype html>
<html><head><meta charset="utf-8"><title>fixture</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.7.1/jquery.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/datatables/1.10.21/css/jquery.dataTables.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/datatables/1.10.21/js/jquery.dataTables.min.js"></script>
</head><body style="background:#14100C;color:#eee;font:16px sans-serif">
<div id="dt"><table id="t" class="display"><thead><tr><th>Player</th><th>Hidden</th><th>Net RTG Diff</th></tr></thead>
<tbody>
<tr><td>JIMMY CLARK III</td><td>x</td><td>+17.0</td></tr>
<tr><td>ROMAN SORKIN</td><td>y</td><td>-3.1</td></tr>
</tbody></table></div>
<button id="late" style="display:none">Late</button>
<script>
$(function () {
  $('#t').DataTable({ paging: false, searching: false, info: false, columnDefs: [{ targets: 1, visible: false }] });
  setTimeout(function () { $('#late').show(); }, 1200);
});
</script>
</body></html>
```

- [ ] **Step 2: Write the failing test**

`video/intro/test/director.test.mjs`:
```js
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
    await page.evaluate(() => document.documentElement.classList.add('shiny-busy'));
    assert.equal(await page.evaluate(() => window.__dir.busy()), true);
  } finally {
    await browser.close();
  }
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `node --test test/director.test.mjs` — Expected: FAIL, `addInitScript` cannot read `page/director.js`.

- [ ] **Step 4: Implement**

`video/intro/page/director.js`:
```js
// Injected into every recorded page (page.addInitScript). Draws the visible
// cursor and the highlight ring, and finds DataTables cells by row/column text.
(() => {
  const AMBER = '#e8a435';
  const CURSOR_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><path d="M3 2l17 10.5-7.4 1.6L9 21z" fill="#fff" stroke="#14100C" stroke-width="1.6" stroke-linejoin="round"/></svg>';

  function node(id, style) {
    let e = document.getElementById(id);
    if (!e) {
      e = document.createElement('div');
      e.id = id;
      Object.assign(e.style, { position: 'fixed', pointerEvents: 'none', zIndex: '2147483647', boxSizing: 'border-box' }, style);
      document.documentElement.appendChild(e);
    }
    return e;
  }
  const cursor = () => node('__dir_cursor', {
    left: '0', top: '0', width: '30px', height: '30px',
    transform: 'translate(800px, 450px)',
    transition: 'transform 600ms cubic-bezier(.4,0,.2,1)',
    background: `url("data:image/svg+xml;utf8,${encodeURIComponent(CURSOR_SVG)}") no-repeat 0 0 / contain`,
    filter: 'drop-shadow(0 2px 4px rgba(0,0,0,.6))',
  });
  const ring = () => node('__dir_ring', {
    border: `4px solid ${AMBER}`, borderRadius: '10px', opacity: '0',
    boxShadow: '0 0 0 9999px rgba(0,0,0,.18), 0 0 24px rgba(232,164,53,.55)',
    transition: 'opacity 250ms ease',
  });

  const norm = (s) => String(s ?? '').replace(/\s+/g, ' ').trim().toUpperCase();

  function dtApi(sel) {
    const host = document.querySelector(sel);
    const $ = window.jQuery;
    if (!host || !$ || !$.fn.dataTable) return null;
    const tbl = host.matches('table') ? host : host.querySelector('table.dataTable');
    return tbl && $.fn.dataTable.isDataTable(tbl) ? $(tbl).DataTable() : null;
  }

  function find(target) {
    const spec = target.cell ?? target.header;
    if (!spec) return null;
    const api = dtApi(spec.table);
    if (!api) return null;
    let col = -1;
    api.columns().every(function (i) {
      if (col < 0 && this.visible() && norm(this.header()?.textContent).includes(norm(spec.col))) col = i;
    });
    if (col < 0) return null;
    if (target.header) return api.column(col).header();
    let row = -1;
    api.rows({ page: 'current' }).every(function (i) {
      if (row < 0 && norm(this.node()?.textContent).includes(norm(spec.row))) row = i;
    });
    return row < 0 ? null : api.cell(row, col).node();
  }

  window.__dir = {
    cursorTo(x, y, ms = 600) {
      const c = cursor();
      c.style.transitionDuration = `${ms}ms`;
      c.style.transform = `translate(${x - 4}px, ${y - 2}px)`;
    },
    ring(r, pad = 6) {
      Object.assign(ring().style, {
        left: `${r.x - pad}px`, top: `${r.y - pad}px`,
        width: `${r.width + 2 * pad}px`, height: `${r.height + 2 * pad}px`, opacity: '1',
      });
    },
    clearRing() { ring().style.opacity = '0'; },
    tag(target, id) {
      const el = find(target);
      if (!el) return false;
      el.setAttribute('data-dir-target', id);
      return true;
    },
    busy() {
      return document.documentElement.classList.contains('shiny-busy') || !!document.querySelector('.recalculating');
    },
  };
})();
```

Note the column headers in the fixture: `'net rtg  diff'` (double space, lower case) must still match — `norm` collapses whitespace and upper-cases both sides.

- [ ] **Step 5: Run test to verify it passes**

Run: `node --test test/director.test.mjs` — Expected: 1 pass.

- [ ] **Step 6: Commit**

```bash
git add video/intro/page/director.js video/intro/test/fixtures/table.html video/intro/test/director.test.mjs
git commit -m "Intro video: in-page cursor, highlight ring and DataTables lookup"
```

---

### Task 7: Overlay renderer (captions and cards)

**Files:**
- Create: `video/intro/page/overlay.html`, `video/intro/lib/overlay-render.mjs`
- Test: `video/intro/test/overlay.test.mjs`

**Interfaces:**
- Produces: `openOverlayPage(browser) -> page`, `renderOverlay(page, o, outPath)` where `o = {kind:'caption'|'card', lang, html, position?, icon?, sub?, n?, w, h}` (`html`/`sub` already passed through `captionHtml`). Captions are transparent PNGs; cards are opaque.

- [ ] **Step 1: Write the failing test**

`video/intro/test/overlay.test.mjs`:
```js
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/overlay.test.mjs` — Expected: FAIL, module not found.

- [ ] **Step 3: Implement**

`video/intro/page/overlay.html`:
```html
<!doctype html>
<html><head><meta charset="utf-8">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@500;600;700&family=Heebo:wght@500;600;700&family=JetBrains+Mono:wght@600&display=block">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<style>
  html, body { margin: 0; background: transparent; }
  #stage { position: relative; overflow: hidden; }
  .cap { position: absolute; left: 50%; transform: translateX(-50%); max-width: 78%; width: max-content;
         padding: 22px 34px; background: rgba(20,16,12,.9); border-left: 6px solid #e8a435; border-radius: 10px;
         color: #f4ede4; font: 600 40px/1.3 'DM Sans', sans-serif; box-shadow: 0 8px 30px rgba(0,0,0,.45); }
  .cap.bottom { bottom: 64px; }
  .cap.top { top: 64px; }
  .cap[dir=rtl] { border-left: none; border-right: 6px solid #e8a435; font-family: 'Heebo', 'DM Sans', sans-serif; }
  .square .cap { max-width: 88%; font-size: 42px; }
  .card { position: absolute; inset: 0; background: #14100C; display: flex; flex-direction: column; align-items: center;
          justify-content: center; color: #f4ede4; font-family: 'DM Sans', sans-serif; text-align: center; padding: 0 8%; }
  .card[dir=rtl] { font-family: 'Heebo', 'DM Sans', sans-serif; }
  .card i { font-size: 96px; color: #e8a435; line-height: 1; }
  .card h1 { font-size: 84px; margin: 28px 0 0; font-weight: 700; line-height: 1.15; }
  .card p { font-size: 40px; color: #b9ab98; margin: 22px 0 0; }
  .card .n { position: absolute; top: 56px; right: 72px; font: 600 30px 'JetBrains Mono', monospace; color: #e8a435; }
  .card[dir=rtl] .n { right: auto; left: 72px; }
  .card .bar { width: 120px; height: 6px; background: #e8a435; border-radius: 3px; margin-top: 36px; }
  .square .card h1 { font-size: 68px; }
  .square .card p { font-size: 34px; }
</style></head>
<body><div id="stage"></div>
<script>
  window.__render = async (o) => {
    const st = document.getElementById('stage');
    st.style.width = o.w + 'px';
    st.style.height = o.h + 'px';
    st.className = o.w === o.h ? 'square' : '';
    const dir = o.lang === 'he' ? 'rtl' : 'ltr';
    if (o.kind === 'caption') {
      st.innerHTML = `<div class="cap ${o.position || 'bottom'}" dir="${dir}" lang="${o.lang}">${o.html}</div>`;
    } else {
      st.innerHTML = `<div class="card" dir="${dir}" lang="${o.lang}">`
        + (o.icon ? `<i class="bi ${o.icon}"></i>` : '')
        + `<h1>${o.html}</h1>`
        + (o.sub ? `<p>${o.sub}</p>` : '')
        + '<div class="bar"></div>'
        + (o.n ? `<div class="n">${o.n}</div>` : '')
        + '</div>';
    }
    await document.fonts.ready;
    return true;
  };
</script></body></html>
```

`video/intro/lib/overlay-render.mjs`:
```js
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/overlay.test.mjs` — Expected: 1 pass. Then open `test/tmp/overlay/he.png` with the Read tool and confirm by eye: amber edge on the right, `+17.0` reads with the plus sign before the digits.

- [ ] **Step 5: Commit**

```bash
git add video/intro/page/overlay.html video/intro/lib/overlay-render.mjs video/intro/test/overlay.test.mjs
git commit -m "Intro video: caption and chapter card renderer"
```

---

### Task 8: Recorder

**Files:**
- Create: `video/intro/record.mjs`, `video/intro/probe.mjs`, `video/intro/test/fixtures/script.fixture.json`
- Test: `video/intro/test/record.test.mjs`

**Interfaces:**
- Consumes: `validateScript` (T1), `extractValue`, `fillTemplate` (T2), `window.__dir` (T6).
- Produces: CLI `node record.mjs [--check] [--chapter <id>] [--url <u>] [--script <path>] [--out <dir>] [--no-shiny]`. Writes `<out>/rec/<chapter>/frames/f000000.jpg...` and `<out>/rec/<chapter>/timeline.json`:
  `{chapter, viewport:{width,height,dsf,mobile}, frames:[{file, ts}], tEnd, steps:[{id, t0, tFocus, t1, bbox|null, values}]}` (times = epoch seconds; `bbox` in viewport CSS px). Exit 1 on any failure; `--check` prints `ok`/`FAIL` per step without recording.
- `node probe.mjs <chapterId|-> <selector>...` prints match count and the first match's outerHTML after running global + chapter setup.

- [ ] **Step 1: Create the fixture script**

`video/intro/test/fixtures/script.fixture.json`:
```json
{
  "url": "https://example.test/",
  "title_card": { "en": { "title": "T", "sub": "S" }, "he": { "title": "כותרת", "sub": "משנה" } },
  "outro_card": { "en": { "title": "Try it" }, "he": { "title": "נסו" } },
  "short": { "hook_en": "Hook", "hook_he": "פתיח" },
  "setup": [],
  "chapters": [
    {
      "id": "fx", "icon": "bi-person-fill", "title_en": "Fixture?", "title_he": "בדיקה?",
      "steps": [
        { "id": "fx-cell", "do": "hover", "target": { "cell": { "table": "#dt", "row": "CLARK III", "col": "Net RTG Diff" } },
          "read": { "net": { "target": { "cell": { "table": "#dt", "row": "CLARK III", "col": "Net RTG Diff" } }, "pattern": "[+\\-−]?\\d+(\\.\\d+)?" } },
          "zoom": 1.4, "hold": 2, "short": 1, "en": "Clark III: {net}", "he": "קלארק: {net}" },
        { "id": "fx-late", "do": "click", "target": "#late", "hold": 1.5, "en": "Late button", "he": "כפתור מאוחר" }
      ]
    },
    {
      "id": "fx-missing", "icon": "bi-x", "title_en": "Missing?", "title_he": "חסר?",
      "steps": [ { "id": "fx-gone", "do": "click", "target": "#does-not-exist", "hold": 1.5, "en": "Gone", "he": "נעלם" } ]
    }
  ]
}
```

- [ ] **Step 2: Write the failing test**

`video/intro/test/record.test.mjs`:
```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync, readdirSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, '..');
const OUT = join(HERE, 'tmp', 'rec');
const URL = pathToFileURL(join(HERE, 'fixtures', 'table.html')).href;
const run = (...extra) => spawnSync(process.execPath, [join(ROOT, 'record.mjs'), '--no-shiny', '--url', URL,
  '--script', join(HERE, 'fixtures', 'script.fixture.json'), '--out', OUT, ...extra], { encoding: 'utf8', timeout: 120000 });

test('records frames and a timeline, waiting for a late element', () => {
  rmSync(OUT, { recursive: true, force: true });
  const r = run('--chapter', 'fx');
  assert.equal(r.status, 0, r.stderr + r.stdout);
  const tl = JSON.parse(readFileSync(join(OUT, 'rec', 'fx', 'timeline.json'), 'utf8'));
  assert.ok(tl.frames.length >= 5, `only ${tl.frames.length} frames`);
  assert.ok(readdirSync(join(OUT, 'rec', 'fx', 'frames')).length === tl.frames.length);
  const [cell, late] = tl.steps;
  assert.equal(cell.values.net, '+17.0');
  assert.ok(cell.bbox && cell.bbox.width > 0);
  assert.ok(cell.t0 <= cell.tFocus && cell.tFocus < cell.t1);
  assert.ok(cell.t1 - cell.tFocus >= 1.95);
  assert.ok(late.t0 >= cell.t1);
  assert.ok(tl.frames[0].ts <= cell.t0 + 1, 'frame clock and step clock disagree');
});

test('a missing target fails the run with the step id', () => {
  const r = run('--chapter', 'fx-missing');
  assert.notEqual(r.status, 0);
  assert.match(r.stderr + r.stdout, /fx-gone/);
});

test('--check reports per step without recording', () => {
  rmSync(OUT, { recursive: true, force: true });
  const r = run('--check');
  assert.equal(r.status, 1);
  assert.match(r.stdout, /ok\s+fx-cell .*net=\+17\.0/);
  assert.match(r.stdout, /FAIL\s+fx-gone/);
  assert.throws(() => readdirSync(join(OUT, 'rec')));
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `node --test test/record.test.mjs` — Expected: FAIL, `record.mjs` not found (non-zero status in test 1).

- [ ] **Step 4: Implement**

`video/intro/record.mjs`:
```js
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

export const DEFAULT_VIEWPORT = { width: 1600, height: 900, dsf: 1.2, mobile: false };
const now = () => Date.now() / 1000;
const pause = (page, s) => page.waitForTimeout(Math.round(s * 1000));
let tagSeq = 0;

async function settle(page) {
  if (opt['no-shiny']) return pause(page, 0.2);
  await pause(page, 0.25);
  await page.waitForFunction(() => !window.__dir.busy(), null, { timeout: 60000, polling: 100 });
  await pause(page, 0.35);
}

async function locate(page, target) {
  if (typeof target === 'string') {
    const loc = page.locator(target).first();
    await loc.waitFor({ state: 'visible', timeout: 15000 });
    return loc;
  }
  const id = `t${tagSeq++}`;
  const deadline = Date.now() + 15000;
  while (!(await page.evaluate(([t, i]) => window.__dir.tag(t, i), [target, id]))) {
    if (Date.now() > deadline) throw new Error(`target not found: ${JSON.stringify(target)}`);
    await pause(page, 0.25);
  }
  return page.locator(`[data-dir-target="${id}"]`).first();
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
    case 'selectize': {
      const ctl = await locate(page, `${s.target} + .selectize-control .selectize-input`);
      await moveTo(page, ctl, fast);
      await ctl.click();
      await page.keyboard.type(s.value, { delay: fast ? 0 : 70 });
      const option = page.locator(`${s.target} + .selectize-control .selectize-dropdown .option`, { hasText: s.value }).first();
      await option.waitFor({ state: 'visible', timeout: 15000 });
      if (!fast) await pause(page, 0.3);
      await option.click();
      return page.keyboard.press('Escape');
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

async function openChapter(browser, script, ch) {
  const vp = { ...DEFAULT_VIEWPORT, ...(ch.viewport ?? {}) };
  const ctx = await browser.newContext({
    viewport: { width: vp.width, height: vp.height }, deviceScaleFactor: vp.dsf,
    isMobile: vp.mobile, hasTouch: vp.mobile, locale: 'en-US',
  });
  await ctx.addInitScript({ path: join(HERE, 'page', 'director.js') });
  const page = await ctx.newPage();
  await page.goto(opt.url, { timeout: 120000, waitUntil: 'load' });
  if (!opt['no-shiny']) {
    await page.waitForFunction(() => window.Shiny?.shinyapp?.isConnected?.(), null, { timeout: 120000 });
  }
  await settle(page);
  for (const s of [...(script.setup ?? []), ...(ch.setup ?? [])]) {
    await perform(page, s, true);
    await settle(page);
  }
  await page.mouse.move(vp.width / 2, vp.height / 2);
  return { ctx, page, vp };
}

async function checkChapter(browser, script, ch) {
  const { ctx, page } = await openChapter(browser, script, ch);
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
  await ctx.close();
  return failures;
}

async function recordChapter(browser, script, ch) {
  const { ctx, page, vp } = await openChapter(browser, script, ch);
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
  await ctx.close();
  return 0;
}

const script = JSON.parse(readFileSync(opt.script, 'utf8'));
const errors = validateScript(script);
if (errors.length) {
  console.error(errors.join('\n'));
  process.exit(1);
}
const browser = await chromium.launch();
let failures = 0;
try {
  for (const ch of script.chapters) {
    if (opt.chapter && ch.id !== opt.chapter) continue;
    try {
      failures += opt.check ? await checkChapter(browser, script, ch) : await recordChapter(browser, script, ch);
    } catch (e) {
      failures++;
      console.error(`chapter ${ch.id}: ${e.message}`);
    }
  }
} finally {
  await browser.close();
}
process.exit(failures ? 1 : 0);
```

`video/intro/probe.mjs`:
```js
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `node --test test/record.test.mjs` — Expected: 3 pass. Then look at one recorded frame (`test/tmp/rec/rec/fx/frames/` — pick one ~2 s in) with the Read tool: the cursor and amber ring must be visible around the `+17.0` cell.

- [ ] **Step 6: Commit**

```bash
git add video/intro/record.mjs video/intro/probe.mjs video/intro/test/fixtures/script.fixture.json video/intro/test/record.test.mjs
git commit -m "Intro video: screencast recorder with check mode and selector probe"
```

---

### Task 9: Overlays, compose and verify CLIs

**Files:**
- Create: `video/intro/overlays.mjs`, `video/intro/compose.mjs`, `video/intro/verify.mjs`, `video/intro/review.mjs`, `video/intro/lib/plan.mjs`
- Test: `video/intro/test/pipeline.test.mjs`

**Interfaces:**
- Consumes: everything above; timelines from Task 8.
- Produces:
  - `lib/plan.mjs`: `CARD = {chapter:1.5, title:2.5, outro:3.5, hook:2.0, shortOutro:3.0}`; `shortPlan(script, timelines) -> [{chapter, id, a, b}]` (sorted by `short`, window `[max(a, focus-1.0), b]`), throws `short cut would run Xs (> 60s)` when windows + `hook` + `shortOutro` exceed 60; `captionWindows(tl) -> [{id, a, b}]`.
  - `overlays.mjs [--out d] [--script p]`: writes `out/overlays/<lang>/wide/<step>.png`, `square/<step>.png` (short steps only), `cards/<chapter>.png`, `cards/title.png`, `cards/outro.png`, `square/hook.png`, `square/outro.png`.
  - `compose.mjs [--out d] [--script p] [--chapter id]`: writes `out/build/clean/<ch>.mp4`, `out/build/<lang>/<ch>.mp4`, and (without `--chapter`) `out/intro_<lang>.mp4`, `out/short_<lang>.mp4`, `out/chapters_<lang>.txt`, `out/index_<lang>.json` (`{stepId: {t0, focus, t1}}` in final-video seconds).
  - `verify.mjs [--out d] [--still <stepId>]`: frame per step to `out/verify/<lang>/<nn>-<step>.jpg`; with `--still`, writes `out/stills/<lang>-<step>.png` from `out/build/<lang>/<ch>.mp4`. Exits 1 if the tutorial is outside 210-360 s or the short is over 60 s.
  - `review.mjs [--out d]`: writes `out/captions_review.md`.

- [ ] **Step 1: Write the failing test**

`video/intro/test/pipeline.test.mjs`:
```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { shortPlan, captionWindows } from '../lib/plan.mjs';
import { probeDuration } from '../lib/ffmpeg.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, '..');
const OUT = join(HERE, 'tmp', 'pipe');
const SCRIPT = join(HERE, 'fixtures', 'script.fixture.json');
const node = (file, ...a) => spawnSync(process.execPath, [join(ROOT, file), '--out', OUT, '--script', SCRIPT, ...a], { encoding: 'utf8', timeout: 600000 });

test('shortPlan orders by short, trims to focus, refuses > 60 s', () => {
  const script = { chapters: [{ id: 'c', steps: [{ id: 'x', short: 2 }, { id: 'y', short: 1 }, { id: 'z' }] }] };
  const tl = { c: { chapter: 'c', frames: [{ ts: 100 }], steps: [
    { id: 'x', t0: 101, tFocus: 105, t1: 108 }, { id: 'y', t0: 110, tFocus: 110.5, t1: 113 }, { id: 'z', t0: 1, tFocus: 1, t1: 2 }] } };
  assert.deepEqual(shortPlan(script, tl), [{ chapter: 'c', id: 'y', a: 10, b: 13 }, { chapter: 'c', id: 'x', a: 4, b: 8 }]);
  tl.c.steps[0].t1 = 170;
  assert.throws(() => shortPlan(script, tl), /short cut would run/);
  assert.deepEqual(captionWindows(tl.c).map((w) => w.id), ['x', 'y', 'z']);
});

test('fixture chapter goes through overlays, compose and verify', () => {
  rmSync(OUT, { recursive: true, force: true });
  const url = pathToFileURL(join(HERE, 'fixtures', 'table.html')).href;
  const rec = node('record.mjs', '--no-shiny', '--url', url, '--chapter', 'fx');
  assert.equal(rec.status, 0, rec.stderr);
  const ov = node('overlays.mjs', '--only-recorded');
  assert.equal(ov.status, 0, ov.stderr);
  assert.ok(existsSync(join(OUT, 'overlays', 'he', 'wide', 'fx-cell.png')));
  assert.ok(existsSync(join(OUT, 'overlays', 'he', 'square', 'fx-cell.png')));
  const cp = node('compose.mjs', '--chapter', 'fx');
  assert.equal(cp.status, 0, cp.stderr);
  const clean = probeDuration(join(OUT, 'build', 'clean', 'fx.mp4'));
  assert.ok(Math.abs(probeDuration(join(OUT, 'build', 'he', 'fx.mp4')) - clean) < 0.2);
  const st = node('verify.mjs', '--still', 'fx-cell');
  assert.equal(st.status, 0, st.stderr);
  assert.ok(existsSync(join(OUT, 'stills', 'en-fx-cell.png')));
  assert.ok(existsSync(join(OUT, 'stills', 'he-fx-cell.png')));
  const rv = node('review.mjs', '--only-recorded');
  assert.equal(rv.status, 0, rv.stderr);
  assert.match(readFileSync(join(OUT, 'captions_review.md'), 'utf8'), /\| fx-cell \| Clark III: \+17\.0 \| קלארק: \+17\.0 \|/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/pipeline.test.mjs` — Expected: FAIL, `../lib/plan.mjs` not found.

- [ ] **Step 3: Implement `lib/plan.mjs`**

```js
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
```

(`a` values in the test: y focus 110.5-1 = 109.5 < t0 110 -> a = 10; x focus 105-1 = 104 > 101 -> a = 4.)

- [ ] **Step 4: Implement `overlays.mjs`**

```js
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
```

- [ ] **Step 5: Implement `compose.mjs`**

```js
#!/usr/bin/env node
// Builds clean (zoomed) chapter video from frames, overlays captions per
// language, and concatenates the tutorial and the short.
// Usage: node compose.mjs [--out d] [--script p] [--chapter id]
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { validateScript } from './lib/script.mjs';
import { focusPlan, squareCropX } from './lib/geometry.mjs';
import { readTimeline, relSteps } from './lib/timeline.mjs';
import { ff, probeDuration, concatList, cleanGraph, captionGraph, writeGraph, ENC, FPS } from './lib/ffmpeg.mjs';
import { CARD, shortPlan, captionWindows } from './lib/plan.mjs';
import { mergeShortChapters, youtubeChapters } from './lib/chapters.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const { values: opt } = parseArgs({ options: {
  out: { type: 'string', default: join(HERE, 'out') },
  script: { type: 'string', default: join(HERE, 'script.json') },
  chapter: { type: 'string' },
} });
const script = JSON.parse(readFileSync(opt.script, 'utf8'));
const errors = validateScript(script);
if (errors.length) { console.error(errors.join('\n')); process.exit(1); }

const OUT = opt.out;
const B = join(OUT, 'build');
const fwd = (p) => p.replace(/\\/g, '/');
const LANGS = ['en', 'he'];

function cleanVideo(ch, tl) {
  const out = join(B, 'clean', `${ch.id}.mp4`);
  const recDir = join(OUT, 'rec', ch.id, 'frames');
  const list = join(B, 'lists', `${ch.id}.ffconcat`);
  writeGraph(list, concatList(tl.frames.map((f) => ({ file: fwd(join(recDir, f.file)), ts: f.ts })), tl.tEnd));
  const rel = relSteps(tl);
  const zooms = ch.steps.flatMap((st) => {
    const r = rel.find((x) => x.id === st.id);
    const { zoom } = focusPlan(r.bbox, tl.viewport, st.zoom);
    return zoom ? [{ a: r.focus, b: r.b, scale: zoom.scale, xf: zoom.xf, yf: zoom.yf }] : [];
  });
  const g = join(B, 'graphs', `clean-${ch.id}.txt`);
  writeGraph(g, cleanGraph(zooms, !!tl.viewport.mobile));
  ff(['-f', 'concat', '-safe', '0', '-i', list, '-/filter_complex', g, '-map', '[out]', ...ENC, out]);
  return out;
}

function captioned(ch, tl, clean, lang) {
  const out = join(B, lang, `${ch.id}.mp4`);
  const dur = probeDuration(clean);
  const caps = captionWindows(tl);
  const inputs = caps.flatMap((c) => ['-loop', '1', '-framerate', String(FPS), '-t', dur.toFixed(3), '-i', join(OUT, 'overlays', lang, 'wide', `${c.id}.png`)]);
  const { graph, out: label } = captionGraph(caps);
  const g = join(B, 'graphs', `cap-${lang}-${ch.id}.txt`);
  writeGraph(g, graph);
  mkdirSync(dirname(out), { recursive: true });
  ff(['-i', clean, ...inputs, '-/filter_complex', g, '-map', label, ...ENC, out]);
  return out;
}

function cardVideo(png, dur, out) {
  mkdirSync(dirname(out), { recursive: true });
  ff(['-loop', '1', '-framerate', String(FPS), '-t', String(dur), '-i', png,
    '-vf', `fade=t=in:st=0:d=0.3,fade=t=out:st=${(dur - 0.3).toFixed(2)}:d=0.3,format=yuv420p,setsar=1`, ...ENC, out]);
  return out;
}

function concat(files, out) {
  const list = `${out}.ffconcat`;
  writeFileSync(list, 'ffconcat version 1.0\n' + files.map((f) => `file '${fwd(f)}'`).join('\n') + '\n');
  ff(['-f', 'concat', '-safe', '0', '-i', list, '-c', 'copy', out]);
}

const chapters = script.chapters.filter((ch) => !opt.chapter || ch.id === opt.chapter);
const timelines = Object.fromEntries(chapters.map((ch) => [ch.id, readTimeline(OUT, ch.id)]));
const clean = Object.fromEntries(chapters.map((ch) => [ch.id, cleanVideo(ch, timelines[ch.id])]));
for (const lang of LANGS) for (const ch of chapters) captioned(ch, timelines[ch.id], clean[ch.id], lang);

if (!opt.chapter) {
  for (const lang of LANGS) {
    const ov = join(OUT, 'overlays', lang);
    const segs = [{ file: cardVideo(join(ov, 'cards', 'title.png'), CARD.title, join(B, lang, 'card-title.mp4')), title: script.title_card[lang].title }];
    for (const ch of chapters) {
      if (ch.card !== false) segs.push({ file: cardVideo(join(ov, 'cards', `${ch.id}.png`), CARD.chapter, join(B, lang, `card-${ch.id}.mp4`)), title: ch[`title_${lang}`] });
      segs.push({ file: join(B, lang, `${ch.id}.mp4`), chapter: ch });
    }
    segs.push({ file: cardVideo(join(ov, 'cards', 'outro.png'), CARD.outro, join(B, lang, 'card-outro.mp4')) });

    const index = {};
    const entries = [];
    let t = 0;
    for (const s of segs) {
      const d = probeDuration(s.file);
      if (s.title) entries.push({ title: s.title, start: t, end: t + d });
      else if (entries.length) entries.at(-1).end = t + d;
      if (s.chapter) {
        for (const r of relSteps(timelines[s.chapter.id])) index[r.id] = { t0: t + r.a, focus: t + r.focus, t1: t + r.b };
      }
      t += d;
    }
    concat(segs.map((s) => s.file), join(OUT, `intro_${lang}.mp4`));
    writeFileSync(join(OUT, `index_${lang}.json`), JSON.stringify(index, null, 1));
    writeFileSync(join(OUT, `chapters_${lang}.txt`), youtubeChapters(mergeShortChapters(entries)));

    const picks = shortPlan(script, timelines);
    const shortSegs = [cardVideo(join(ov, 'square', 'hook.png'), CARD.hook, join(B, lang, 'sq-hook.mp4'))];
    for (const p of picks) {
      const ch = script.chapters.find((c) => c.id === p.chapter);
      const st = ch.steps.find((x) => x.id === p.id);
      const tl = timelines[p.chapter];
      const rec = tl.steps.find((x) => x.id === p.id);
      const x = squareCropX(focusPlan(rec.bbox, tl.viewport, st.zoom).box, 1920, 1080);
      const len = p.b - p.a;
      const out = join(B, lang, `sq-${p.id}.mp4`);
      const g = join(B, 'graphs', `sq-${lang}-${p.id}.txt`);
      writeGraph(g, `[0:v]crop=1080:1080:${x}:0,setpts=PTS-STARTPTS[b];[1:v]format=rgba,fade=t=in:st=0:d=0.25:alpha=1,fade=t=out:st=${(len - 0.25).toFixed(3)}:d=0.25:alpha=1[c];[b][c]overlay=0:0,setsar=1[out]`);
      ff(['-ss', p.a.toFixed(3), '-to', p.b.toFixed(3), '-i', clean[p.chapter], '-loop', '1', '-framerate', String(FPS), '-t', len.toFixed(3),
        '-i', join(ov, 'square', `${p.id}.png`), '-/filter_complex', g, '-map', '[out]', ...ENC, out]);
      shortSegs.push(out);
    }
    shortSegs.push(cardVideo(join(ov, 'square', 'outro.png'), CARD.shortOutro, join(B, lang, 'sq-outro.mp4')));
    concat(shortSegs, join(OUT, `short_${lang}.mp4`));
    console.log(`${lang}: intro ${probeDuration(join(OUT, `intro_${lang}.mp4`)).toFixed(1)}s, short ${probeDuration(join(OUT, `short_${lang}.mp4`)).toFixed(1)}s`);
  }
}
```

- [ ] **Step 6: Implement `verify.mjs` and `review.mjs`**

`video/intro/verify.mjs`:
```js
#!/usr/bin/env node
// Extracts a frame per step from the finished videos for inspection and checks
// durations. --still <stepId> grabs one captioned frame per language from the
// chapter build instead (the look-and-feel approval checkpoint).
import { readFileSync, mkdirSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { ff, probeDuration } from './lib/ffmpeg.mjs';
import { readTimeline, relSteps } from './lib/timeline.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const { values: opt } = parseArgs({ options: {
  out: { type: 'string', default: join(HERE, 'out') },
  script: { type: 'string', default: join(HERE, 'script.json') },
  still: { type: 'string' },
} });
const script = JSON.parse(readFileSync(opt.script, 'utf8'));
const OUT = opt.out;
const grab = (video, t, png) => { mkdirSync(dirname(png), { recursive: true }); ff(['-ss', t.toFixed(3), '-i', video, '-frames:v', '1', png]); };

if (opt.still) {
  const ch = script.chapters.find((c) => c.steps.some((s) => s.id === opt.still));
  if (!ch) { console.error(`no step ${opt.still}`); process.exit(1); }
  const r = relSteps(readTimeline(OUT, ch.id)).find((s) => s.id === opt.still);
  for (const lang of ['en', 'he']) grab(join(OUT, 'build', lang, `${ch.id}.mp4`), (r.focus + r.b) / 2, join(OUT, 'stills', `${lang}-${opt.still}.png`));
  console.log(`stills in ${join(OUT, 'stills')}`);
  process.exit(0);
}

let bad = 0;
for (const lang of ['en', 'he']) {
  const intro = join(OUT, `intro_${lang}.mp4`);
  const short = join(OUT, `short_${lang}.mp4`);
  if (!existsSync(intro) || !existsSync(short)) { console.error(`${lang}: run compose.mjs first`); process.exit(1); }
  const di = probeDuration(intro);
  const ds = probeDuration(short);
  console.log(`${lang}: intro ${di.toFixed(1)}s, short ${ds.toFixed(1)}s`);
  if (di < 210 || di > 360) { console.error(`${lang}: intro ${di.toFixed(1)}s outside 210-360s`); bad++; }
  if (ds > 60) { console.error(`${lang}: short ${ds.toFixed(1)}s over 60s`); bad++; }
  const index = JSON.parse(readFileSync(join(OUT, `index_${lang}.json`), 'utf8'));
  Object.entries(index).forEach(([id, w], i) => grab(intro, (w.focus + w.t1) / 2, join(OUT, 'verify', lang, `${String(i).padStart(2, '0')}-${id}.jpg`)));
}
console.log(`frames in ${join(OUT, 'verify')} -- inspect every one`);
process.exit(bad ? 1 : 0);
```

`video/intro/review.mjs`:
```js
#!/usr/bin/env node
// Writes out/captions_review.md: every caption in both languages, values filled.
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { readTimeline } from './lib/timeline.mjs';
import { captionsReviewMarkdown } from './lib/review.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const { values: opt } = parseArgs({ options: {
  out: { type: 'string', default: join(HERE, 'out') },
  script: { type: 'string', default: join(HERE, 'script.json') },
  'only-recorded': { type: 'boolean', default: false },
} });
const script = JSON.parse(readFileSync(opt.script, 'utf8'));
const chapters = script.chapters.filter((ch) => !opt['only-recorded'] || existsSync(join(opt.out, 'rec', ch.id, 'timeline.json')));
const values = {};
for (const ch of chapters) for (const s of readTimeline(opt.out, ch.id).steps) values[s.id] = s.values;
mkdirSync(opt.out, { recursive: true });
writeFileSync(join(opt.out, 'captions_review.md'), captionsReviewMarkdown({ ...script, chapters }, values));
console.log(`wrote ${join(opt.out, 'captions_review.md')}`);
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `node --test` (whole suite) — Expected: all pass. Read `test/tmp/pipe/stills/he-fx-cell.png` with the Read tool: caption on the right-to-left panel, ring visible, the zoom has tightened on the cell.

- [ ] **Step 8: Commit**

```bash
git add video/intro/lib/plan.mjs video/intro/overlays.mjs video/intro/compose.mjs video/intro/verify.mjs video/intro/review.mjs video/intro/test/pipeline.test.mjs
git commit -m "Intro video: overlays, compose, verify and caption review CLIs"
```

---

### Task 10: The film script, checked against the real app

**Files:**
- Create: `video/intro/script.json`, `video/intro/README.md`
- Test: `video/intro/test/script-content.test.mjs`

**Interfaces:**
- Consumes: the schema from Task 1; `record.mjs --check` and `probe.mjs` from Task 8.

- [ ] **Step 1: Write the failing content test**

`video/intro/test/script-content.test.mjs`:
```js
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateScript } from '../lib/script.mjs';

const script = JSON.parse(readFileSync(join(dirname(fileURLToPath(import.meta.url)), '..', 'script.json'), 'utf8'));

test('the real script is valid', () => {
  assert.deepEqual(validateScript(script), []);
});

test('it covers every chapter the spec lists, in order', () => {
  assert.deepEqual(script.chapters.map((c) => c.id),
    ['cold-open', 'home', 'onoff', 'lineups', 'team', 'gamelogs', 'players', 'compare', 'euro', 'tips', 'tips-mobile']);
});

test('planned length fits the spec', () => {
  const holds = script.chapters.flatMap((c) => c.steps).reduce((t, s) => t + s.hold + 1.6, 0);
  assert.ok(holds > 200 && holds < 330, `planned ${holds.toFixed(0)}s`);
  const short = script.chapters.flatMap((c) => c.steps).filter((s) => s.short !== undefined);
  assert.ok(short.length >= 6 && short.length <= 8);
  assert.ok(short.reduce((t, s) => t + s.hold + 1.0, 5) <= 55);
});

test('captions never hard-code a stat', () => {
  for (const st of script.chapters.flatMap((c) => c.steps)) {
    assert.doesNotMatch(st.en, /[+\-−]\d|\d+\.\d/, `${st.id} has a typed number; use read`);
  }
});
```

- [ ] **Step 2: Run it to verify it fails**

Run: `node --test test/script-content.test.mjs` — Expected: FAIL, `script.json` not found.

- [ ] **Step 3: Write `script.json`**

```json
{
  "url": "https://arieltaieb-basketball-israel-analytics.share.connect.posit.cloud/",
  "setup": [ { "do": "selectize", "target": "#game_year", "value": "25-26" } ],
  "title_card": {
    "en": { "title": "IBPL Analytics", "sub": "A 4-minute tour" },
    "he": { "title": "IBPL Analytics", "sub": "סיור של 4 דקות" }
  },
  "outro_card": { "en": { "title": "Try it now" }, "he": { "title": "נסו עכשיו" } },
  "short": { "hook_en": "What really happens when your best player sits?", "hook_he": "מה באמת קורה כשהשחקן הכי טוב שלכם יושב?" },
  "chapters": [
    {
      "id": "cold-open", "icon": "bi-activity", "card": false,
      "title_en": "Intro", "title_he": "פתיחה",
      "steps": [
        { "id": "open-hook", "do": "none", "hold": 3.5,
          "en": "What really happens when your best player sits?",
          "he": "מה באמת קורה כשהשחקן הכי טוב שלכם יושב על הספסל?" },
        { "id": "open-promise", "do": "none", "hold": 3.5,
          "en": "This site answers questions like that, in a few clicks.",
          "he": "האתר הזה עונה על שאלות כאלה, בכמה קליקים." }
      ]
    },
    {
      "id": "home", "icon": "bi-house-fill",
      "title_en": "Finding your way", "title_he": "איך מתמצאים",
      "steps": [
        { "id": "home-league", "do": "hover", "target": "button[data-league-btn=\"il\"]", "ringTarget": ".league-chooser", "hold": 3,
          "en": "First, pick a league: Israeli League, EuroLeague or EuroCup.",
          "he": "קודם בוחרים ליגה: ליגת העל, יורוליג או יורוקאפ." },
        { "id": "home-season", "do": "hover", "target": "#game_year + .selectize-control", "hold": 3,
          "en": "The season picker at the top drives every page.",
          "he": "בחירת העונה למעלה קובעת את כל העמודים." },
        { "id": "home-team", "do": "selectize", "target": "#home_team", "value": "MACCABI TEL AVIV", "zoom": 1.3, "hold": 3.5,
          "en": "Choose your team to see its story right here.",
          "he": "בחרו קבוצה כדי לראות את הסיפור שלה כאן." },
        { "id": "home-default", "do": "hover", "target": "#home_set_default", "ringTarget": ".home-team-default", "hold": 3,
          "en": "Tick “Set as default” and it opens next time too.",
          "he": "סמנו \"Set as default\" והיא תיפתח גם בפעם הבאה." },
        { "id": "home-cards", "do": "hover", "target": ".league-only-il .home-nav-card:has-text(\"Who is helping my team?\")", "hold": 3.5,
          "en": "Each card is a question. Tap one to get the answer.",
          "he": "כל כרטיס הוא שאלה. לוחצים עליו ומקבלים תשובה." },
        { "id": "home-glossary", "do": "hover", "target": "#open_glossary", "hold": 3,
          "en": "Unsure what a stat means? The glossary explains every term.",
          "he": "לא בטוחים מה מדד אומר? המילון מסביר כל מונח." }
      ]
    },
    {
      "id": "onoff", "icon": "bi-person-fill",
      "title_en": "Who is helping my team?", "title_he": "מי עוזר לקבוצה שלי?",
      "steps": [
        { "id": "onoff-open", "do": "click", "target": ".league-only-il .home-nav-card:has-text(\"Who is helping my team?\")", "ring": false, "hold": 2,
          "en": "Open “Who is helping my team?”",
          "he": "פותחים את \"Who is helping my team?\"" },
        { "id": "onoff-header", "do": "hover", "target": { "header": { "table": "#onoff_dt", "col": "Net RTG Diff" } }, "zoom": 1.4, "hold": 4,
          "en": "Net RTG Diff: how much better the team is with him on.",
          "he": "Net RTG Diff: בכמה הקבוצה טובה יותר כשהוא על המגרש." },
        { "id": "onoff-tag", "do": "hover", "target": "#onoff_dt .onoff-net-tip:has(.onoff-luck-tag)", "zoom": 1.3, "hold": 4.5, "short": 2,
          "en": "A tag like “3PT luck” warns when a number may mislead.",
          "he": "תגית כמו \"3PT luck\" מזהירה כשמספר עלול להטעות." },
        { "id": "onoff-team", "do": "selectize", "target": "#teams", "value": "MACCABI TEL AVIV", "hold": 2.5,
          "en": "Filter to Maccabi Tel Aviv.",
          "he": "מסננים למכבי תל אביב." },
        { "id": "onoff-clark", "do": "hover", "target": { "cell": { "table": "#onoff_dt", "row": "CLARK III", "col": "Net RTG Diff" } },
          "read": { "net": { "target": { "cell": { "table": "#onoff_dt", "row": "CLARK III", "col": "Net RTG Diff" } }, "pattern": "[+\\-−]?\\d+(\\.\\d+)?" } },
          "zoom": 1.5, "hold": 4.5, "short": 1,
          "en": "Jimmy Clark III: {net} points per 100 possessions.",
          "he": "ג'ימי קלארק: {net} נקודות ל־100 פוזשנים." },
        { "id": "onoff-colors", "do": "none", "ringTarget": { "cell": { "table": "#onoff_dt", "row": "CLARK III", "col": "Net RTG Diff" } }, "hold": 3.5,
          "en": "Green helps, red hurts. The brighter, the bigger.",
          "he": "ירוק עוזר, אדום מזיק. ככל שהצבע חזק יותר, ההשפעה גדולה יותר." },
        { "id": "onoff-sorkin", "do": "hover", "target": { "cell": { "table": "#onoff_dt", "row": "SORKIN", "col": "Net RTG Diff" } },
          "read": { "sorkin": { "target": { "cell": { "table": "#onoff_dt", "row": "SORKIN", "col": "Net RTG Diff" } }, "pattern": "[+\\-−]?\\d+(\\.\\d+)?" } },
          "zoom": 1.5, "hold": 4.5,
          "en": "Roman Sorkin plays more, yet sits at {sorkin}.",
          "he": "רומן סורקין משחק יותר, ובכל זאת עומד על {sorkin}." },
        { "id": "onoff-lesson", "do": "none", "hold": 4,
          "en": "On a dominant team even the bench wins. Minutes aren't impact.",
          "he": "בקבוצה דומיננטית גם הספסל מנצח. דקות הן לא השפעה." },
        { "id": "onoff-ff", "do": "click", "target": "label:has(input[name=\"onoff_view_mode\"][value=\"Four Factors\"])", "hold": 4,
          "en": "Four Factors shows why: shooting, turnovers, rebounds, free throws.",
          "he": "Four Factors מראה למה: קליעה, איבודים, ריבאונדים וזריקות עונשין." },
        { "id": "onoff-est", "do": "hover", "target": "#onoff_dt td:has-text(\"est.\")", "zoom": 1.5, "hold": 4,
          "en": "“est. ±X pts” turns each edge into points per 100.",
          "he": "\"est. ±X pts\" מתרגם כל יתרון לנקודות ל־100 פוזשנים." },
        { "id": "onoff-filters", "do": "hover", "target": "#date_range", "hold": 3.5,
          "en": "Narrow it by dates, last games or opponent in the sidebar.",
          "he": "אפשר לצמצם לפי תאריכים, משחקים אחרונים או יריבה בסרגל הצד." }
      ]
    },
    {
      "id": "lineups", "icon": "bi-people-fill",
      "title_en": "Which lineups are working?", "title_he": "אילו חמישיות עובדות?",
      "steps": [
        { "id": "ld-open", "do": "click", "target": ".league-only-il .home-nav-card:has-text(\"Which lineups are working?\")", "ring": false, "hold": 2,
          "en": "Open “Which lineups are working?”",
          "he": "פותחים את \"Which lineups are working?\"" },
        { "id": "ld-team", "do": "selectize", "target": "#ld_lineup_filter-team", "value": "MACCABI TEL AVIV", "hold": 3,
          "en": "Pick Maccabi and its players appear as chips.",
          "he": "בוחרים מכבי והשחקנים שלה מופיעים כצ'יפים." },
        { "id": "ld-size", "do": "click", "target": "label:has(input[name=\"ld_num\"][value=\"5\"])", "ringTarget": "#ld_num", "hold": 3,
          "en": "Group size: full five-man units, or pairs and trios.",
          "he": "גודל קבוצה: חמישיות מלאות, או זוגות ושלישיות." },
        { "id": "ld-on", "do": "click", "target": ".lineup-chip:text-matches(\"clark\", \"i\")", "zoom": 1.3, "hold": 3.5,
          "en": "Tap a player to see only lineups with him on.",
          "he": "לוחצים על שחקן ורואים רק חמישיות שהוא בהן." },
        { "id": "ld-anyof-mode", "do": "click", "target": ".lineup-chips-mode[data-mode=\"any\"]", "hold": 3,
          "en": "Switch to “Any of” to build a group.",
          "he": "עוברים ל־\"Any of\" כדי לבנות קבוצה." },
        { "id": "ld-anyof-1", "do": "click", "target": ".lineup-chip:text-matches(\"hoard\", \"i\")", "hold": 1.5,
          "en": "Add players to the group...",
          "he": "מוסיפים שחקנים לקבוצה..." },
        { "id": "ld-anyof-2", "do": "click", "target": ".lineup-chip:text-matches(\"sorkin\", \"i\")", "hold": 1.5,
          "en": "...one at a time...",
          "he": "...אחד אחרי השני..." },
        { "id": "ld-anyof-3", "do": "click", "target": ".lineup-chip:text-matches(\"brissett\", \"i\")", "ringTarget": ".lineup-chips-summary", "zoom": 1.3, "hold": 4, "short": 3,
          "en": "...and the sentence reads your filter back in plain words.",
          "he": "...והמשפט מקריא את הסינון במילים פשוטות." },
        { "id": "ld-count", "do": "click", "target": ".lineup-chips-quant", "ringTarget": ".lineup-chips-summary", "hold": 3.5,
          "en": "Tap “at least” to switch to “exactly”.",
          "he": "לוחצים על \"at least\" כדי לעבור ל־\"exactly\"." },
        { "id": "ld-clutch", "do": "hover", "target": "#ld_clutch_enabled", "hold": 3,
          "en": "Clutch filter: only close games, late in the fourth.",
          "he": "מסנן קלאץ': רק משחקים צמודים, בסוף הרבע הרביעי." },
        { "id": "ld-row", "do": "click", "target": "#ld_table [onclick*=\"handleLineupLinkClick\"]", "ringTarget": ".modal-content", "hold": 4.5,
          "en": "Click any lineup to see it game by game.",
          "he": "לוחצים על חמישייה ורואים אותה משחק אחרי משחק." }
      ]
    },
    {
      "id": "team", "icon": "bi-bar-chart-fill",
      "title_en": "How is my team performing?", "title_he": "איך הקבוצה שלי מתפקדת?",
      "steps": [
        { "id": "tr-open", "do": "click", "target": ".league-only-il .home-nav-card:has-text(\"How is my team performing?\")", "ring": false, "hold": 2,
          "en": "Open “How is my team performing?”",
          "he": "פותחים את \"How is my team performing?\"" },
        { "id": "tr-maccabi", "do": "hover", "target": { "cell": { "table": "#tr_table", "row": "MACCABI TEL AVIV", "col": "Net" } },
          "read": { "net": { "target": { "cell": { "table": "#tr_table", "row": "MACCABI TEL AVIV", "col": "Net" } }, "pattern": "[+\\-−]?\\d+(\\.\\d+)?" } },
          "zoom": 1.4, "hold": 4.5, "short": 4,
          "en": "Maccabi's net rating: {net}. Offense, defense and net vs the league.",
          "he": "הנט רייטינג של מכבי: {net}. התקפה, הגנה ונטו מול הליגה." },
        { "id": "tr-ranks", "do": "none", "ringTarget": { "cell": { "table": "#tr_table", "row": "MACCABI TEL AVIV", "col": "Net" } }, "zoom": 1.4, "hold": 3.5,
          "en": "Each cell shows the value and its rank in the league.",
          "he": "כל תא מציג את הערך ואת הדירוג בליגה." },
        { "id": "tr-views", "do": "hover", "target": "#tr_view_mode", "hold": 3.5,
          "en": "Switch views: Four Factors, Shot Profile, Traditional.",
          "he": "מחליפים תצוגה: Four Factors, Shot Profile, Traditional." },
        { "id": "tr-shot", "do": "click", "target": "label:has(input[name=\"tr_view_mode\"][value=\"Shot Profile\"])", "ringTarget": "#tr_table", "hold": 4,
          "en": "Shot Profile: where the team shoots from, and how well.",
          "he": "Shot Profile: מאיפה הקבוצה זורקת, ובאיזו הצלחה." }
      ]
    },
    {
      "id": "gamelogs", "icon": "bi-calendar-day-fill",
      "title_en": "What happened last night?", "title_he": "מה קרה אתמול במשחק?",
      "steps": [
        { "id": "gl-open", "do": "click", "target": ".league-only-il .home-nav-card:has-text(\"What happened in last night\")", "ring": false, "hold": 2,
          "en": "Open “What happened in last night's game?”",
          "he": "פותחים את \"What happened in last night's game?\"" },
        { "id": "gl-team", "do": "selectize", "target": "#gl_team", "value": "MACCABI TEL AVIV", "hold": 3,
          "en": "Pick a team to list every game it played.",
          "he": "בוחרים קבוצה ורואים את כל המשחקים שלה." },
        { "id": "gl-row", "do": "hover", "target": "#gl_table tbody tr", "zoom": 1.3, "hold": 3.5,
          "en": "Each row: result, score, and how its lineups performed.",
          "he": "כל שורה: ניצחון או הפסד, התוצאה, ואיך החמישיות תפקדו." },
        { "id": "gl-flow", "do": "click", "target": "#gl_table a:has-text(\"View\")", "ringTarget": ".modal-content", "hold": 5, "short": 5,
          "en": "“View” opens the game flow: who was on court, minute by minute.",
          "he": "\"View\" פותח את מהלך המשחק: מי היה על המגרש, דקה אחרי דקה." }
      ]
    },
    {
      "id": "players", "icon": "bi-bar-chart-line",
      "title_en": "How are individual players doing?", "title_he": "איך כל שחקן מתפקד?",
      "steps": [
        { "id": "ts-open", "do": "click", "target": ".league-only-il .home-nav-card:has-text(\"How are individual players performing?\")", "ring": false, "hold": 2,
          "en": "Open “How are individual players performing?”",
          "he": "פותחים את \"How are individual players performing?\"" },
        { "id": "ts-team", "do": "selectize", "target": "#ts_teams", "value": "MACCABI TEL AVIV", "ringTarget": "#ts_table", "hold": 3,
          "en": "Per-player stats: points, rebounds, assists, shooting.",
          "he": "סטטיסטיקה לכל שחקן: נקודות, ריבאונדים, אסיסטים, קליעה." },
        { "id": "ts-modes", "do": "hover", "target": ".navbar a[data-value=\"traditional_stats\"]", "ringTarget": ".tab-hover-menu", "hold": 4,
          "en": "Hover the tab to switch: Totals, Per Game, Per 60 Possessions.",
          "he": "מעבירים עכבר על הלשונית ומחליפים: סה״כ, למשחק, ל־60 פוזשנים." },
        { "id": "ts-per60", "do": "click", "target": ".tab-hover-menu >> text=\"Per 60 Possessions\"", "ringTarget": "#ts_table", "hold": 4.5,
          "en": "Per-possession numbers are fairest: fast and slow games count the same.",
          "he": "המספרים לפי פוזשן הכי הוגנים: משחק מהיר ואיטי נספרים אותו דבר." },
        { "id": "ts-clutch", "do": "hover", "target": "#ts_clutch_enabled", "hold": 3,
          "en": "Clutch filter: who delivers when the game is close.",
          "he": "מסנן קלאץ': מי מספק כשהמשחק צמוד." }
      ]
    },
    {
      "id": "compare", "icon": "bi-arrow-left-right",
      "title_en": "Starters vs bench", "title_he": "פותחים מול ספסל",
      "setup": [
        { "do": "click", "target": ".league-only-il .home-nav-card:has-text(\"How do starters compare to the bench?\")" },
        { "do": "selectize", "target": "#cmp_a_teams", "value": "MACCABI TEL AVIV" },
        { "do": "selectize", "target": "#cmp_b_teams", "value": "MACCABI TEL AVIV" }
      ],
      "steps": [
        { "id": "cmp-mode", "do": "hover", "target": "#cmp_mode", "hold": 3,
          "en": "Compare any two situations: teams, lineups or players.",
          "he": "משווים כל שני מצבים: קבוצות, חמישיות או שחקנים." },
        { "id": "cmp-preset", "do": "selectize", "target": "#cmp_preset", "value": "Starters vs Bench", "hold": 3.5,
          "en": "Quick preset: Starters vs Bench.",
          "he": "בחירה מהירה: Starters vs Bench." },
        { "id": "cmp-result", "do": "hover", "target": "#cmp_summary_a", "zoom": 1.3, "hold": 5, "short": 6,
          "en": "Side A vs side B: the gap shows who carries the team.",
          "he": "צד A מול צד B: הפער מראה מי סוחב את הקבוצה." },
        { "id": "cmp-more", "do": "hover", "target": "#cmp_preset + .selectize-control", "hold": 3.5,
          "en": "More presets: home vs away, clutch, wins vs losses.",
          "he": "עוד מצבים מוכנים: בית מול חוץ, קלאץ', ניצחונות מול הפסדים." }
      ]
    },
    {
      "id": "euro", "icon": "bi-globe",
      "title_en": "EuroLeague & EuroCup", "title_he": "יורוליג ויורוקאפ",
      "steps": [
        { "id": "el-pick", "do": "click", "target": "button[data-league-btn=\"E\"]", "ringTarget": ".league-chooser", "hold": 3.5, "short": 7,
          "en": "Switch to EuroLeague: the same questions, the same pages.",
          "he": "עוברים ליורוליג: אותן שאלות, אותם עמודים." },
        { "id": "el-onoff", "do": "click", "target": ".league-only-el .home-nav-card:has-text(\"Who is helping my team?\")", "ring": false, "hold": 4,
          "en": "On/off, lineups, team ratings and game logs, for every club.",
          "he": "און/אוף, חמישיות, דירוג קבוצות ויומני משחק, לכל קבוצה." },
        { "id": "el-note", "do": "none", "hold": 3.5,
          "en": "Leagues are never ranked against each other.",
          "he": "הליגות אף פעם לא מדורגות זו מול זו." }
      ]
    },
    {
      "id": "tips", "icon": "bi-lightbulb",
      "title_en": "Pro tips", "title_he": "טיפים",
      "setup": [
        { "do": "click", "target": ".league-only-il .home-nav-card:has-text(\"Who is helping my team?\")" },
        { "do": "selectize", "target": "#teams", "value": "MACCABI TEL AVIV" }
      ],
      "steps": [
        { "id": "tip-chip", "do": "hover", "target": ".filter-chips .filter-chip.chip-focusable", "ringTarget": ".filter-chips", "hold": 4,
          "en": "Every active filter shows as a chip. Tap it to clear.",
          "he": "כל סינון פעיל מופיע כצ'יפ. לוחצים עליו כדי לנקות." },
        { "id": "tip-header", "do": "hover", "target": { "header": { "table": "#onoff_dt", "col": "Off ON Diff" } }, "hold": 3.5,
          "en": "Hover a column header to see what it means.",
          "he": "מעבירים עכבר על כותרת עמודה ורואים מה היא אומרת." },
        { "id": "tip-csv", "do": "hover", "target": "#onoff_dt .buttons-csv", "hold": 3,
          "en": "Download any table as CSV for your own analysis.",
          "he": "אפשר להוריד כל טבלה כ־CSV לניתוח משלכם." }
      ]
    },
    {
      "id": "tips-mobile", "icon": "bi-phone", "card": false,
      "title_en": "On your phone", "title_he": "בטלפון",
      "viewport": { "width": 390, "height": 844, "dsf": 2.5, "mobile": true },
      "setup": [
        { "do": "click", "target": ".league-only-il .home-nav-card:has-text(\"Who is helping my team?\")" }
      ],
      "steps": [
        { "id": "tip-mobile", "do": "click", "target": "button:has-text(\"Show Filters\") >> visible=true", "hold": 4,
          "en": "On a phone, tap “Show Filters” to open the sidebar.",
          "he": "בטלפון, לוחצים \"Show Filters\" כדי לפתוח את סרגל הצד." }
      ]
    }
  ]
}
```

- [ ] **Step 4: Run the content test**

Run: `node --test test/script-content.test.mjs` — Expected: 4 pass. If "planned length" fails, adjust holds (not chapters) until it passes.

- [ ] **Step 5: Start the local app (Bash tool, background) and health-check it**

Follow the `run-shiny-local` skill exactly: disk check, port 3838 free, launch with `IBPL_CACHE_UI=false`, poll the log for `Listening on`, run the health check (non-zero `nav-link`/`nav-item`, all assets 200). Do not continue on a failed health check.

- [ ] **Step 6: Run the check and fix selectors with evidence**

Run (in `video/intro`): `node record.mjs --check`
Expected on first run: some `FAIL` lines. For each one, run `node probe.mjs <chapterId> "<candidate selector>"` until the probe shows exactly the intended element, then edit only that step's `target`/`ringTarget`/`read` in `script.json`. Candidates most likely to need a fix, and what to probe:

| Step | Probe |
|---|---|
| `onoff-tag` | `#onoff_dt .onoff-luck-tag` — if 0 matches on the league-wide table, move the step after `onoff-team` and probe again; if still 0, delete the step and give `short: 2` to `onoff-colors` |
| `ld-team` | `[id^="ld_lineup_filter"][id$="team"]` |
| `ld-anyof-mode` | `.lineup-chips-mode` (read the `data-mode` values) |
| `ld-clutch`, `ts-clutch` | `#ld_clutch_enabled`, `#ts_clutch_enabled` — if hidden, ring the parent `.checkbox` |
| `tr-maccabi` | `#tr_table thead th` (exact Net column header text) |
| `gl-flow`, `ld-row` | `.modal-content` after the click, and `#gl_table a` |
| `ts-modes`, `ts-per60` | `.tab-hover-menu` and its children after hovering |
| `cmp-result` | `#cmp_summary_a`, and whether the preset already sets the teams (if so, drop the two team `setup` actions) |
| `tip-chip` | `.filter-chips .filter-chip` |

Re-run `node record.mjs --check` until it exits 0 with every step `ok`, and the `net`/`sorkin` values printed match the 2025-26 numbers in the spec (+17.0 / -3.1) — or the data changed since, in which case trust the app and tell the user.

- [ ] **Step 7: Write the README**

`video/intro/README.md`:
```markdown
# IBPL intro video

Everything the film says and does is in `script.json`. Edit that, then re-run.

1. Start the app locally (project skill `run-shiny-local`, `IBPL_CACHE_UI=false`, port 3838).
2. `node record.mjs --check` — every step must print `ok`. Fix selectors with `node probe.mjs <chapter> "<selector>"`.
3. `node record.mjs` (or `--chapter <id>` to redo one chapter)
4. `node overlays.mjs`
5. `node review.mjs` — `out/captions_review.md`, both languages side by side
6. `node compose.mjs` (or `--chapter <id>` + `node verify.mjs --still <step>` for a quick look)
7. `node verify.mjs` — durations, plus a frame per step in `out/verify/` to inspect

Outputs: `out/intro_{en,he}.mp4`, `out/short_{en,he}.mp4`, `out/chapters_{en,he}.txt` (paste into the YouTube description).
Numbers in captions are read from the page at record time, so re-recording after an ETL run updates them.
Tests: `node --test` (needs network for CDN fonts/DataTables in fixtures, and ffmpeg on PATH).
```

- [ ] **Step 8: Commit**

```bash
git add video/intro/script.json video/intro/README.md video/intro/test/script-content.test.mjs
git commit -m "Intro video: film script for Maccabi Tel Aviv 2025-26, checked against the app"
```

---

### Task 11: Record, look-and-feel approval, Hebrew review (user gates)

**Files:**
- Modify: `video/intro/script.json` (only in response to user feedback)

- [ ] **Step 1: Record every chapter**

App still running and healthy. Run: `node record.mjs` — Expected: one `recorded <id>: N frames, Xs` line per chapter, exit 0.

- [ ] **Step 2: Build the approval stills**

```bash
node overlays.mjs && node compose.mjs --chapter onoff && node verify.mjs --still onoff-clark
```
Expected: `out/stills/en-onoff-clark.png`, `out/stills/he-onoff-clark.png`. Read both with the Read tool yourself first: ring on Clark's Net RTG Diff cell, zoomed, caption readable, Hebrew panel mirrored, `+` before the digits. Fix anything wrong before showing the user.

- [ ] **Step 3: USER GATE — look and feel.** Show the user both stills (paths) and ask for sign-off on the look. Apply requested style changes in `page/overlay.html` / `page/director.js`, rerun Step 2, ask again. Do not proceed without an explicit yes.

- [ ] **Step 4: Build the caption review table**

Run: `node review.mjs` — Expected: `out/captions_review.md`.

- [ ] **Step 5: USER GATE — Hebrew review.** Give the user `out/captions_review.md` and ask them to correct the Hebrew column. Apply edits to `script.json` (keep placeholders identical in both languages), run `node --test test/script-content.test.mjs`, and re-record only the chapters whose captions changed *word count materially* — caption text alone does not need a re-record, only `node overlays.mjs`.

- [ ] **Step 6: Commit script changes, if any**

```bash
git add video/intro/script.json
git commit -m "Intro video: caption fixes from review"
```

---

### Task 12: Final render and verification

**Files:** none new (outputs in `video/intro/out/`, gitignored)

- [ ] **Step 1: Render everything**

```bash
node overlays.mjs && node compose.mjs
```
Expected: per language `intro Xs, short Ys` lines; exit 0. If `shortPlan` throws, reduce `hold` on the short steps named in the message and repeat from `node record.mjs --chapter <id>` for those chapters.

- [ ] **Step 2: Run verify**

Run: `node verify.mjs` — Expected: exit 0; tutorial 210-360 s and short <= 60 s for both languages.

- [ ] **Step 3: Inspect every extracted frame**

Read every image in `out/verify/en/` and `out/verify/he/` with the Read tool. For each frame check: ring on the element the caption talks about; caption not clipped and not covering the ringed element; no loading state (dimmed `.recalculating` table, empty dropdown, skeleton); Hebrew right-to-left with numbers intact. Also play-check the short by grabbing 3 frames from it:
```bash
for t in 3 20 40; do ffmpeg -hide_banner -loglevel error -y -ss $t -i out/short_he.mp4 -frames:v 1 out/verify/short-he-$t.jpg; done
```
Any bad frame: fix the cause (selector, hold, ringTarget), re-record that chapter, re-run Steps 1-3.

- [ ] **Step 4: Check the chapter files**

Read `out/chapters_en.txt` and `out/chapters_he.txt`: first line `0:00`, at least 3 lines, titles match the chapter cards.

- [ ] **Step 5: Run the full test suite once more**

Run: `node --test` — Expected: all pass.

- [ ] **Step 6: Stop the local app** (PowerShell, per the skill's Stop section).

- [ ] **Step 7: Report to the user** with the four video paths, durations, the two chapter files, and anything cut or changed versus the spec (e.g. a step dropped because the tag did not exist). Then hand off to superpowers:finishing-a-development-branch.
