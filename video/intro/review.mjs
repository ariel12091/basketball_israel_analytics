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
