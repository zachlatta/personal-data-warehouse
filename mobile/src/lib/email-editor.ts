// Editable rich-HTML email composer document for react-native-webview.
// The server's DOM sanitizer is authoritative for editor_html. We embed it
// directly inside a strict Content-Security-Policy (script-src 'none') document
// on a light email surface (#FFFFFF / #111111, matching EmailHtmlBody) so
// attribute entities (&amp;, &#39;, &#34; in href/src/style/alt) are never
// double-escaped. Inside the WebView, a single DOM allow-list cleans both the
// loaded DOM and pasted HTML, and edits are compared against the editor's own
// initial innerHTML so focus/blur without typing never marks the draft dirty.

const EDITOR_POLICY = [
  "default-src 'none'",
  "script-src 'none'",
  'img-src https: http: data: cid:',
  "style-src 'unsafe-inline' https:",
  'font-src https: data:',
].join('; ');

// Trailing empty blocks in a contenteditable are cursor room, not content.
export function stripTrailingEmptyBlocksHtml(html: string): string {
  let out = html.trim();
  const trailingEmpty = /(?:<(?:div|p)(?:\s[^>]*)?>\s*(?:<br\s*\/?>|\u00a0|&nbsp;|\s)*<\/(?:div|p)>\s*)+$/i;
  while (trailingEmpty.test(out)) {
    const next = out.replace(trailingEmpty, '').trim();
    if (!next || next === out) break;
    out = next;
  }
  return out || '<div><br></div>';
}

// Key that forces the WebView editor to reload whenever the variant, revert
// counter, or server baseline changes (so a background refresh from the server
// never leaves stale body HTML on screen while metadata updates).
export function emailEditorResetKey(draftKey: string, variantId: string, revertCount: number, serverBaseline: string): string {
  return `${draftKey}:${variantId}:${revertCount}:${serverBaseline}`;
}

export type EmailEditorMessage =
  | { type: 'height'; height: number }
  | { type: 'change'; html: string }
  | { type: 'revert' }
  | { type: 'snapshot'; id: number; html: string; isBaseline: boolean }
  | { type: 'focus' }
  | { type: 'blur' };

// Resolves the HTML value for a WebView editor message using the HTML the
// document was built from (never the live draft prop).
export function emailEditorHtmlFromMessage(msg: EmailEditorMessage, builtHtml: string): string | null {
  if (msg.type === 'change') return msg.html;
  if (msg.type === 'revert') return builtHtml;
  if (msg.type === 'snapshot') return msg.isBaseline ? builtHtml : msg.html;
  return null;
}

export function emailEditorDocument(html: string, editable: boolean): string {
  const body = String(html ?? '').trim() || '<div><br></div>';
  const css = [
    'html,body{margin:0;padding:0;height:auto;background:#FFFFFF;-webkit-text-size-adjust:100%}',
    'body{font:16px/1.5 -apple-system,BlinkMacSystemFont,Roboto,system-ui,sans-serif;color:#111111;background:#FFFFFF;word-wrap:break-word;overflow-wrap:anywhere}',
    '#editor{height:auto;min-height:140px;padding:12px 14px;outline:none;-webkit-user-select:text;user-select:text}',
    '#editor:empty:before{content:attr(data-placeholder);color:#6B7280;pointer-events:none}',
    '#editor p{margin:0 0 12px}',
    '#editor p:last-child{margin-bottom:0}',
    '#editor ul,#editor ol{margin:0 0 12px;padding-left:24px}',
    '#editor li{margin:0 0 6px}',
    '#editor h1,#editor h2,#editor h3,#editor h4,#editor h5,#editor h6{margin:12px 0 6px;line-height:1.3}',
    '#editor blockquote{margin:8px 0;padding-left:12px;border-left:3px solid #D1D5DB;color:#4B5563}',
    '#editor table{border-collapse:collapse;margin:8px 0;max-width:100%}',
    '#editor td,#editor th{border:1px solid #D1D5DB;padding:4px 8px;text-align:left}',
    '#editor a{color:#1D4ED8;text-decoration:underline}',
    '#editor img{max-width:100%;height:auto}',
    '#editor pre{white-space:pre-wrap;margin:8px 0}',
  ].join('');
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="${EDITOR_POLICY}"><meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no"><style>${css}</style></head><body><div id="editor" contenteditable="${editable ? 'true' : 'false'}" spellcheck="true" data-placeholder="Write the email">${body}</div></body></html>`;
}

// Injected script (runs as a native WebView user script, outside page CSP):
// - Cleans loaded and pasted HTML with one DOM allow-list (preserving entities).
// - Captures baselineHtml after load and never posts 'change' on focus/blur
//   without real input; typing then undoing back to baseline posts 'revert'.
// - Measures ONLY editor.getBoundingClientRect().height so deleting text shrinks
//   the WebView back down (never pinned by documentElement.scrollHeight).
export const EMAIL_EDITOR_SCRIPT = `(function () {
  var editor = document.getElementById('editor');
  if (!editor || window.__pdwEditorReady) return;
  window.__pdwEditorReady = true;
  var lastHeight = 0;
  var DROP = {script:1,style:1,svg:1,math:1,iframe:1,frame:1,frameset:1,object:1,embed:1,applet:1,noscript:1,template:1,form:1,input:1,button:1,select:1,textarea:1,base:1,meta:1,link:1,xmp:1,noembed:1,plaintext:1,audio:1,video:1,source:1,canvas:1,marquee:1};
  var ALLOW_TAG = {p:1,div:1,br:1,span:1,strong:1,b:1,em:1,i:1,u:1,s:1,strike:1,del:1,ins:1,sub:1,sup:1,a:1,ul:1,ol:1,li:1,h1:1,h2:1,h3:1,h4:1,h5:1,h6:1,blockquote:1,pre:1,code:1,hr:1,table:1,thead:1,tbody:1,tfoot:1,tr:1,th:1,td:1,caption:1,colgroup:1,col:1,img:1,font:1,center:1,small:1,big:1,abbr:1,cite:1,dl:1,dt:1,dd:1,section:1,article:1,header:1,footer:1};
  var ALLOW_ATTR = {href:1,src:1,alt:1,title:1,target:1,rel:1,style:1,class:1,dir:1,align:1,valign:1,width:1,height:1,colspan:1,rowspan:1,cellpadding:1,cellspacing:1,border:1,color:1,face:1,size:1,start:1,type:1,role:1,bgcolor:1,background:1,nowrap:1};
  function post(payload) {
    try { window.ReactNativeWebView.postMessage(JSON.stringify(payload)); } catch (e) {}
  }
  function safeUrl(raw, isSrc) {
    var c = String(raw || '').replace(/[\\u0000-\\u0020\\u007f]+/g, '').toLowerCase();
    if (!c || c[0] === '#' || c[0] === '/') return true;
    if (/^(https?:|mailto:|tel:|cid:)/.test(c)) return true;
    if (isSrc && /^data:image\\/(png|jpeg|jpg|gif|webp)/.test(c)) return true;
    return c.indexOf(':') < 0;
  }
  function safeStyle(raw) {
    var l = String(raw || '').replace(/[\\u0000-\\u0020\\u007f]+/g, '').toLowerCase();
    if (/javascript:|vbscript:|expression\\(|@import|behavior:|-moz-binding/.test(l)) return false;
    var idx = l.indexOf('url(');
    if (idx >= 0) {
      var rest = l.slice(idx + 4).replace(/^["']+/, '');
      if (!/^https?:\\/\\//.test(rest)) return false;
    }
    return true;
  }
  function cleanTree(parent, stripClass) {
    for (var child = parent.firstChild; child;) {
      var next = child.nextSibling;
      if (child.nodeType === 8) {
        parent.removeChild(child);
      } else if (child.nodeType === 1) {
        var tag = child.tagName.toLowerCase();
        if (DROP[tag]) {
          parent.removeChild(child);
        } else if (!ALLOW_TAG[tag]) {
          cleanTree(child, stripClass);
          while (child.firstChild) parent.insertBefore(child.firstChild, child);
          parent.removeChild(child);
        } else {
          var attrs = Array.prototype.slice.call(child.attributes);
          for (var k = 0; k < attrs.length; k++) {
            var a = attrs[k];
            var n = a.name.toLowerCase();
            if (!ALLOW_ATTR[n] || (stripClass && n === 'class')) { child.removeAttribute(a.name); continue; }
            if (n === 'href' || n === 'src' || n === 'background') {
              if (!safeUrl(a.value, n !== 'href')) child.setAttribute(a.name, '#');
            } else if (n === 'style' && !safeStyle(a.value)) {
              child.removeAttribute(a.name);
            }
          }
          cleanTree(child, stripClass);
        }
      }
      child = next;
    }
  }
  function sanitizePasted(raw) {
    var doc = new DOMParser().parseFromString(String(raw || ''), 'text/html');
    cleanTree(doc.body, true);
    return doc.body.innerHTML.trim();
  }
  cleanTree(editor, false);
  var baselineHtml = editor.innerHTML;
  var lastPostedHtml = baselineHtml;
  var dirty = false;
  function reportHeight() {
    var rect = editor.getBoundingClientRect();
    var h = Math.ceil(Math.max(rect.height, 140));
    if (h > 0 && Math.abs(h - lastHeight) > 1) {
      lastHeight = h;
      post({ type: 'height', height: h });
    }
  }
  function reportChange() {
    var cur = editor.innerHTML;
    if (cur === baselineHtml) {
      if (dirty) {
        dirty = false;
        lastPostedHtml = baselineHtml;
        post({ type: 'revert' });
      }
    } else {
      dirty = true;
      if (cur !== lastPostedHtml) {
        lastPostedHtml = cur;
        post({ type: 'change', html: cur });
      }
    }
    reportHeight();
  }
  editor.addEventListener('input', reportChange);
  editor.addEventListener('compositionend', reportChange);
  editor.addEventListener('focus', function () { post({ type: 'focus' }); });
  editor.addEventListener('blur', function () { if (dirty) reportChange(); post({ type: 'blur' }); });
  editor.addEventListener('paste', function (ev) {
    var dt = ev.clipboardData;
    if (!dt) return;
    ev.preventDefault();
    var html = dt.getData('text/html');
    if (html) {
      var clean = sanitizePasted(html);
      if (clean) document.execCommand('insertHTML', false, clean);
    } else {
      var text = dt.getData('text/plain');
      if (text) document.execCommand('insertText', false, text);
    }
    reportChange();
  });
  editor.addEventListener('click', function (ev) {
    if (editor.getAttribute('contenteditable') === 'true') {
      var target = ev.target;
      while (target && target !== editor) {
        if (target.tagName === 'A') { ev.preventDefault(); return; }
        target = target.parentNode;
      }
    }
  });
  window.__pdwExec = function (cmd) {
    if (editor.getAttribute('contenteditable') !== 'true') return;
    editor.focus();
    document.execCommand(cmd, false, null);
    reportChange();
  };
  window.__pdwSetEditable = function (flag) {
    editor.setAttribute('contenteditable', flag ? 'true' : 'false');
  };
  window.__pdwReadSnapshot = function (id) {
    var cur = editor.innerHTML;
    post({ type: 'snapshot', id: id, html: cur, isBaseline: cur === baselineHtml });
  };
  if (typeof ResizeObserver !== 'undefined') {
    new ResizeObserver(reportHeight).observe(editor);
  }
  window.addEventListener('load', reportHeight);
  setTimeout(reportHeight, 100);
  setTimeout(reportHeight, 400);
  reportHeight();
})();
true;`;
