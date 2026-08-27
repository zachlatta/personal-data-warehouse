import test from "node:test";
import assert from "node:assert/strict";
import { fmtTime, dayOf, formatWhen, isReal, pretty, truncate, hue } from "../format.js";

test("fmtTime and dayOf render in the local timezone and survive garbage", () => {
  const d = new Date(2026, 7, 27, 9, 5);
  assert.equal(fmtTime(d.toISOString()), "09:05");
  assert.equal(dayOf(d.toISOString()), "2026-08-27");
  assert.equal(fmtTime(""), "—");
  assert.equal(dayOf("not a date"), "unknown");
});

test("formatWhen reads as relative age, then a date", () => {
  const now = new Date("2026-08-27T12:00:00Z");
  assert.equal(formatWhen("2026-08-27T11:59:40Z", now), "just now");
  assert.equal(formatWhen("2026-08-27T11:30:00Z", now), "30m ago");
  assert.equal(formatWhen("2026-08-27T08:00:00Z", now), "4h ago");
  assert.equal(formatWhen("2026-08-24T12:00:00Z", now), "3d ago");
  assert.equal(formatWhen(null, now), "");
});

test("isReal treats the epoch sentinel as absent", () => {
  assert.equal(isReal("1970-01-01T00:00:00Z"), false);
  assert.equal(isReal("2026-08-27T00:00:00Z"), true);
  assert.equal(isReal(null), false);
});

test("pretty flattens scalars and lists, and keeps objects readable", () => {
  assert.equal(pretty(["a", "b"]), "a, b");
  assert.equal(pretty({ a: 1 }), '{\n  "a": 1\n}');
  assert.equal(pretty(null), "");
});

test("truncate keeps short text and marks cut text", () => {
  assert.equal(truncate("hello", 10), "hello");
  assert.equal(truncate("hello world", 6), "hello…");
});

test("every known source has a distinct hue and unknown ones share the fallback", () => {
  assert.notEqual(hue("gmail"), hue("slack"));
  assert.equal(hue("nope"), hue("also-nope"));
});
