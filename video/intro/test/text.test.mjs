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

test('a quoted English label is one isolate, quotes and inner punctuation included', () => {
  // Seen in the render: the closing ?" of a quoted card title jumped to the
  // far side of the label.
  assert.equal(
    captionHtml('פותחים את "How is my team performing?"', 'he'),
    'פותחים את <bdi dir="ltr">&quot;How is my team performing?&quot;</bdi>',
  );
  assert.equal(
    captionHtml('לוחצים על "at least" כדי לעבור ל־"exactly".', 'he'),
    'לוחצים על <bdi dir="ltr">&quot;at least&quot;</bdi> כדי לעבור ל־<bdi dir="ltr">&quot;exactly&quot;</bdi>.',
  );
});
