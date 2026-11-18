import { readFileSync } from 'node:fs';
import ts from 'typescript';
import test from 'node:test';
import assert from 'node:assert/strict';

// The "N new" pill is absolutely positioned, so it anchors to its parent. On
// the launcher's home-screen Feed the screen's own top is under the status
// bar; the pill must share a wrapper with the list, below the header.
const file = new URL('./feed-screen.tsx', import.meta.url);
const source = ts.createSourceFile('feed-screen.tsx', readFileSync(file, 'utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);

function find(node, predicate) {
  if (predicate(node)) return node;
  return ts.forEachChild(node, (child) => find(child, predicate));
}

const tagName = (node) => (ts.isJsxElement(node) ? node.openingElement : node).tagName.getText();
const styleText = (node) => {
  const opening = ts.isJsxElement(node) ? node.openingElement : node;
  const style = opening.attributes.properties.find((p) => ts.isJsxAttribute(p) && p.name.getText() === 'style');
  return style?.initializer?.getText() ?? '';
};
const isElement = (node) => ts.isJsxElement(node) || ts.isJsxSelfClosingElement(node);

function enclosingElement(node) {
  for (let p = node.parent; p; p = p.parent) if (ts.isJsxElement(p)) return p;
  return null;
}

test('the new-cards pill is anchored inside the list wrapper, not the screen that holds the header', () => {
  const pill = find(source, (n) => isElement(n) && tagName(n) === 'Pressable' && /styles\.pill\b/.test(styleText(n)));
  assert.ok(pill, 'pill Pressable not found');
  const wrapper = enclosingElement(pill);
  assert.equal(tagName(wrapper), 'View');
  assert.match(styleText(wrapper), /^\{styles\.fill\}$/);
  assert.ok(find(wrapper, (n) => isElement(n) && tagName(n) === 'FlatList'), 'pill wrapper must also hold the FlatList');
  assert.ok(!find(wrapper, (n) => ts.isJsxExpression(n) && n.expression?.getText() === 'header'), 'header must stay outside the pill wrapper');
});
