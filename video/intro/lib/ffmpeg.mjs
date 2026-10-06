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

// Each caption PNG is an input only as long as its own window, shifted to its
// start with setpts. Looping every PNG for the whole chapter instead cost
// N x chapter-length 1080p decodes and ran ffmpeg out of memory (ENOMEM).
export function captionInputs(caps, pngFor) {
  return caps.flatMap((c) => ['-loop', '1', '-framerate', String(FPS), '-t', (c.b - c.a).toFixed(3), '-i', pngFor(c.id)]);
}

export function captionGraph(caps) {
  const parts = ['[0:v]null[v0]'];
  caps.forEach((c, i) => {
    const n = i + 1;
    const a = c.a.toFixed(3);
    const b = c.b.toFixed(3);
    const fo = Math.max(0, c.b - c.a - 0.25).toFixed(3);
    parts.push(`[${n}:v]format=rgba,fade=t=in:st=0:d=0.25:alpha=1,fade=t=out:st=${fo}:d=0.25:alpha=1,setpts=PTS+${a}/TB[c${n}]`);
    parts.push(`[v${i}][c${n}]overlay=0:0:eof_action=pass:enable='between(t,${a},${b})'[v${n}]`);
  });
  return { graph: parts.join(';\n'), out: `[v${caps.length}]` };
}
