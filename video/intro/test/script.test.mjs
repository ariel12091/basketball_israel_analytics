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
