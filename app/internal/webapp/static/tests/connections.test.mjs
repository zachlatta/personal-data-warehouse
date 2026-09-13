import test from "node:test";
import assert from "node:assert/strict";
import { clientNames } from "../connections.js";
test("connection client access names are trimmed and deduplicated", () => {
  assert.deepEqual(clientNames(" codex, claude, codex, , web "), ["codex", "claude", "web"]);
  assert.deepEqual(clientNames(""), []);
});
