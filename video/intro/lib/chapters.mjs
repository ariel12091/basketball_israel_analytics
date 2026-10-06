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
