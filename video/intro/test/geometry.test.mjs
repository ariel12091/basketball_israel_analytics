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
