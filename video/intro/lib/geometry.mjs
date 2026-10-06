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
