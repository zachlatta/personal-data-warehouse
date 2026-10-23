import assert from 'node:assert/strict';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import test from 'node:test';
import { MATERIAL_ICON } from './icons.ts';

const materialGlyphs = JSON.parse(readFileSync(new URL('../../node_modules/expo-symbols/build/android/symbols.json', import.meta.url), 'utf8'));
const srcRoot = new URL('..', import.meta.url).pathname;

function sourceFiles(dir) {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return sourceFiles(path);
    return /\.tsx?$/.test(name) ? [path] : [];
  });
}

test('every icon names a glyph the Android Material Symbols font has', () => {
  for (const [sf, material] of Object.entries(MATERIAL_ICON)) {
    assert.ok(material in materialGlyphs, `${sf} -> ${material} is not a Material Symbol`);
  }
});

// A SymbolView given a bare SF Symbol name renders nothing on Android, so an
// icon drawn anywhere but the Icon component is an icon Android never shows.
test('only the Icon component draws SymbolView', () => {
  const offenders = sourceFiles(srcRoot)
    .filter((path) => /from 'expo-symbols'/.test(readFileSync(path, 'utf8')))
    .map((path) => relative(srcRoot, path))
    .filter((path) => path !== join('components', 'icon.tsx'));
  assert.deepEqual(offenders, []);
});
