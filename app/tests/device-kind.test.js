const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const test = require("node:test");

const source = fs.readFileSync("app/www/mobile.js", "utf8");
const start = source.indexOf("window.ibplDeviceKindFor = function");
const end = source.indexOf("window.ibplDeviceKind = function", start);
assert.ok(start >= 0 && end > start);
const context = { window: {} };
vm.runInNewContext(source.slice(start, end), context);
const classify = context.window.ibplDeviceKindFor;

test("tablets keep the tablet view across user agents and orientations", () => {
  assert.equal(classify("Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X)", "iPad", 5), "tablet");
  assert.equal(classify("Mozilla/5.0 (Macintosh; Intel Mac OS X)", "MacIntel", 5), "tablet");
  assert.equal(classify("Mozilla/5.0 (Linux; Android 14; SM-X710)", "Linux armv8l", 5), "tablet");
  assert.equal(classify("Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Win32", 10, true), "tablet");
});

test("phones and desktops keep their own views", () => {
  assert.equal(classify("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)", "iPhone", 5), "phone");
  assert.equal(classify("Mozilla/5.0 (Linux; Android 14; Pixel 8) Mobile", "Linux armv8l", 5), "phone");
  assert.equal(classify("Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Win32", 0), "desktop");
});
