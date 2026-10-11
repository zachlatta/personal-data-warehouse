import test from 'node:test';
import assert from 'node:assert/strict';

import {
  EMAIL_BODY_COLLAPSED_HEIGHT,
  EMAIL_SIZER_SCRIPT,
  emailBodyForDisplay,
  emailHtmlDocument,
  emailNavigation,
  looksLikeHtml,
  sanitizeEmailHtml,
} from './email-html.ts';

// The shape of the bulk mail in the 2026-10-11 screen recording: a Word/MSO
// head, a style block, an analytics script, an HTML comment banner, and the
// message itself in a table. Synthetic, so no real mail is checked in.
const BULK = `<!DOCTYPE html>
<html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" lang="">
<head>
<title></title>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<!--[if mso]><xml><w:WordDocument xmlns:w="urn:schemas-microsoft-com:office:word"><w:DontUseAdvancedTypographyReadingMail/></w:WordDocument></xml><![endif]-->
<style>body{margin:0;padding:0}.desktop_hide{display:none}</style>
<script>(function(){window.BOOMR_config={};var o=document.createElement("iframe");})();</script>
</head>
<body onload="track()">
<!-- ==================== CONTENT BLOCK: Transactional Campaign ==================== -->
<table width="600"><tr><td><a href="https://example.test/order/1">View your order</a>
<img src="http://cdn.example.test/logo.png" width="600"></td></tr></table>
<iframe src="https://tracker.example.test/frame"></iframe>
<a href="javascript:alert(1)">bad</a>
<form action="https://phish.example.test"><input name="password"></form>
<meta http-equiv="refresh" content="0;url=https://elsewhere.example.test">
</body>
</html>`;

test('a bulk HTML email is recognized as HTML even when it arrived as the text part', () => {
  assert.equal(looksLikeHtml(BULK), true);
  assert.equal(looksLikeHtml('<!--\n=====\nCONTENT BLOCK\n-->\n<div>x</div>'), true);
  assert.equal(looksLikeHtml('Hi Zach,\n\nSee you at 3 <3\n\nThanks'), false);
  assert.equal(looksLikeHtml('Use the <b> tag'), false);
  assert.equal(looksLikeHtml(''), false);
});

test('the HTML part is what a message renders, the text part only when there is no HTML', () => {
  assert.deepEqual(emailBodyForDisplay({ bodyHtml: '<p>Hello</p>', text: 'Hello', hasFullBody: true }), { kind: 'html', html: '<p>Hello</p>' });
  // Senders that put their HTML in text/plain (the hotel and bank mail in the
  // recording) render as HTML too, not as source.
  assert.equal(emailBodyForDisplay({ bodyHtml: '', text: BULK, hasFullBody: true }).kind, 'html');
  assert.deepEqual(emailBodyForDisplay({ bodyHtml: '', text: 'Plain words.', hasFullBody: true }), { kind: 'text', text: 'Plain words.' });
  assert.deepEqual(emailBodyForDisplay({ bodyHtml: '  ', text: '', hasFullBody: false }), { kind: 'none' });
});

test('sanitizing removes everything that runs or navigates, and keeps what is read', () => {
  const clean = sanitizeEmailHtml(BULK);
  assert.doesNotMatch(clean, /<script/i);
  assert.doesNotMatch(clean, /BOOMR/);
  assert.doesNotMatch(clean, /<iframe/i);
  assert.doesNotMatch(clean, /onload=/i);
  assert.doesNotMatch(clean, /javascript:/i);
  assert.doesNotMatch(clean, /<form/i);
  assert.doesNotMatch(clean, /http-equiv="refresh"/i);
  assert.match(clean, /View your order/);
  assert.match(clean, /href="https:\/\/example\.test\/order\/1"/);
  // Plain-http images are blocked by App Transport Security; nearly every mail
  // CDN serves the same path over https.
  assert.match(clean, /src="https:\/\/cdn\.example\.test\/logo\.png"/);
  // A style block is how the mail lays itself out; it stays.
  assert.match(clean, /\.desktop_hide\{display:none\}/);
});

test('the rendered document forbids page scripts and fits the phone', () => {
  const doc = emailHtmlDocument(BULK);
  assert.match(doc, /Content-Security-Policy/);
  assert.match(doc, /script-src 'none'/);
  assert.match(doc, /name="viewport"[^>]*width=device-width/);
  assert.match(doc, /img\{max-width:100%/);
  assert.match(doc, /View your order/);
  assert.equal((doc.match(/<head/gi) ?? []).length, 1, 'one head, with the policy first in it');
  assert.ok(doc.indexOf('Content-Security-Policy') < doc.indexOf('<style>body{margin:0'), 'the policy precedes the mail\'s own head');

  const fragment = emailHtmlDocument('<div dir="ltr">Hi Zach</div>');
  assert.match(fragment, /^<!DOCTYPE html><html><head>/);
  assert.match(fragment, /<body><div dir="ltr">Hi Zach<\/div><\/body><\/html>$/);
});

test('the sizing script reports a height and ends the way react-native-webview requires', () => {
  assert.match(EMAIL_SIZER_SCRIPT, /ReactNativeWebView\.postMessage/);
  assert.match(EMAIL_SIZER_SCRIPT, /true;\s*$/);
  assert.ok(EMAIL_BODY_COLLAPSED_HEIGHT >= 300 && EMAIL_BODY_COLLAPSED_HEIGHT <= 700);
});

test('the email itself loads in place; a tapped link opens outside it; nothing else navigates', () => {
  assert.equal(emailNavigation('about:blank'), 'load');
  assert.equal(emailNavigation('about:srcdoc'), 'load');
  assert.equal(emailNavigation('https://example.test/order/1'), 'open');
  assert.equal(emailNavigation('http://example.test/x'), 'open');
  assert.equal(emailNavigation('mailto:someone@example.test'), 'open');
  assert.equal(emailNavigation('tel:+15555550100'), 'open');
  assert.equal(emailNavigation('javascript:alert(1)'), 'block');
  assert.equal(emailNavigation('file:///etc/passwd'), 'block');
  assert.equal(emailNavigation('superhuman://x'), 'block');
});
