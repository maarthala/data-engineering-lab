// Mock exams — rendering shared by exams.html (members) and exams-admin.html (managers).
// Question texts, options, case studies and explanations are Markdown with inline HTML (bank
// schema 1.1): paragraphs, # headings, - / 1. lists (nested by indent), | tables |, ``` code ```,
// **bold**, *italic*, `code`, [text](url) — and HTML tags, of which clean() keeps only harmless ones.
(function () {
  var style = document.createElement('style');
  style.textContent = `
.md p{margin:0 0 10px} .md ul, .md ol{margin:0 0 10px; padding-left:22px} .md h1, .md h2, .md h3, .md h4{font-size:1rem; margin:12px 0 6px}
.md pre{background:var(--panel2); border:1px solid var(--line); border-radius:8px; padding:8px 10px; overflow:auto; font-size:.86rem; line-height:1.45; margin:0 0 10px}
.md pre code{background:none; padding:0; color:inherit}
.md table{width:auto; max-width:100%; margin:0 0 10px; font-size:.9rem; display:block; overflow:auto}
.md th, .md td{border:1px solid var(--line); padding:4px 8px; text-align:left; vertical-align:top}
.md td pre{margin:0}
.case{border:1px solid var(--line); border-radius:10px; margin:6px 0 12px; background:var(--panel2)}
.case summary{cursor:pointer; padding:8px 12px; font-weight:600}
.case .md{max-height:340px; overflow:auto; padding:2px 14px 8px; font-size:.92rem; line-height:1.5; border-top:1px solid var(--line); background:var(--panel)}
.slots{border-collapse:collapse; width:100%; margin-bottom:8px} .slots td{padding:8px 10px; border-top:1px solid var(--line); vertical-align:middle}
.slots td:first-child{width:40%; font-weight:600} .slots select{max-width:100%; min-width:220px}
.slots label{margin-right:16px; cursor:pointer} .slots .ok{color:var(--accent2)} .slots .bad{color:var(--bad-ink)}
`;
  document.head.appendChild(style);
})();
function esc(s) { return String(s || '').replace(/[&<>"]/g, function (c) { return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[c]; }); }
var TAGS = 'table|thead|tbody|tfoot|tr|th|td|caption|br|hr|pre|code|b|strong|i|em|u|s|del|sub|sup|kbd|ul|ol|li|p|div|span|blockquote|h[1-6]|a';
var KEEP = new RegExp('^(' + TAGS + ')$'), DROP = /^(script|style|iframe|frame|object|embed|svg|math|template|noscript|textarea|select|option|input|button|form|link|meta|base|img|video|audio|source|title|head)$/;
function clean(html) {                                // only TAGS stay; attributes only colspan/rowspan/start/href (http)
  var t = document.createElement('template'); t.innerHTML = html;
  (function walk(n) {
    Array.prototype.slice.call(n.childNodes).forEach(function (c) {
      if (c.nodeType === 3) return;
      if (c.nodeType !== 1) return c.remove();
      var tag = c.tagName.toLowerCase();
      if (DROP.test(tag)) return c.remove();
      walk(c);
      if (!KEEP.test(tag)) return c.replaceWith.apply(c, Array.prototype.slice.call(c.childNodes));
      Array.prototype.slice.call(c.attributes).forEach(function (x) {
        var ok = /^(colspan|rowspan)$/.test(x.name) && /^\d+$/.test(x.value) || x.name === 'start' && tag === 'ol' && /^\d+$/.test(x.value) ||
                 x.name === 'href' && tag === 'a' && /^https?:\/\//i.test(x.value);
        if (!ok) c.removeAttribute(x.name);
      });
      if (tag === 'a') { c.setAttribute('target', '_blank'); c.setAttribute('rel', 'noopener'); }
    });
  })(t.content);
  return t.innerHTML;
}
var TAG_START = new RegExp('<(?!\\/?(' + TAGS + ')[\\s>/])', 'gi');
function mdText(t) {                                  // plain text: & and < escaped, except the HTML tags we keep
  return t.replace(/&(?![a-zA-Z]+;|#\d+;|#x[0-9a-fA-F]+;)/g, '&amp;').replace(TAG_START, '&lt;');
}
function inline(t) {
  var codes = [];
  t = t.replace(/`([^`\n]+)`/g, function (_, c) { codes.push('<code>' + esc(c) + '</code>'); return '\u0001' + (codes.length - 1) + '\u0001'; });
  return mdText(t).replace(/\*\*(.+?)\*\*/g, '<b>$1</b>').replace(/(^|[\s(])[*_]([^*_\s](?:[^*_\n]*[^*_\s])?)[*_](?=[\s).,;:!?]|$)/g, '$1<i>$2</i>')
    .replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, '<a href="$2">$1</a>')
    .replace(/\u0001(\d+)\u0001/g, function (_, n) { return codes[n]; });
}
function mdList(lines) {                                 // - / 1. items, nested by indent
  var out = '', stack = [];
  lines.forEach(function (l) {
    var m = /^(\s*)([-*+]|(\d+)[.)])\s+(.*)$/.exec(l);
    if (!m) { out += ' ' + inline(l.trim()); return; }                 // a wrapped line of the item above
    var ind = m[1].length, tag = m[3] ? 'ol' : 'ul';
    while (stack.length && ind < stack[stack.length - 1].ind) out += '</li></' + stack.pop().tag + '>';
    if (!stack.length || ind > stack[stack.length - 1].ind) {
      stack.push({ind: ind, tag: tag});
      out += '<' + tag + (m[3] && m[3] !== '1' ? ' start="' + m[3] + '"' : '') + '><li>';
    } else out += '</li><li>';
    out += inline(m[4]);
  });
  while (stack.length) out += '</li></' + stack.pop().tag + '>';
  return out;
}
function mdTable(lines) {
  var cells = function (l) { return l.trim().replace(/^\||\|$/g, '').split('|').map(function (c) { return inline(c.trim()); }); };
  return '<table><thead><tr>' + cells(lines[0]).map(function (c) { return '<th>' + c + '</th>'; }).join('') + '</tr></thead><tbody>' +
    lines.slice(2).map(function (l) { return '<tr>' + cells(l).map(function (c) { return '<td>' + c + '</td>'; }).join('') + '</tr>'; }).join('') + '</tbody></table>';
}
function md(s) {
  var held = [];
  var hold = function (h) { held.push(h); return '\n\n\u0002' + (held.length - 1) + '\u0002\n\n'; };
  s = String(s || '').replace(/\r\n?/g, '\n')
    .replace(/^ *```[^\n]*\n([\s\S]*?)\n? *```[^\n]*$/gm, function (_, c) { return hold('<pre><code>' + esc(c) + '</code></pre>'); })
    .replace(/^ *<(table|pre|div|blockquote|ul|ol)\b[\s\S]*?<\/\1>/gim, function (h) { return hold(mdText(h)); });
  var out = s.split(/\n{2,}/).map(function (block) {
    var lines = block.split('\n').filter(function (l) { return l.trim(); });
    if (!lines.length) return '';
    var m = /^\u0002(\d+)\u0002$/.exec(lines[0].trim());
    if (m && lines.length === 1) return held[m[1]];
    if (lines.length === 1 && (m = /^(#{1,6})\s+(.*)$/.exec(lines[0]))) return '<h' + m[1].length + '>' + inline(m[2]) + '</h' + m[1].length + '>';
    if (lines.length >= 2 && /^\s*\|/.test(lines[0]) && /^\s*\|?\s*:?-{2,}/.test(lines[1])) return mdTable(lines);
    if (/^\s*([-*+]|\d+[.)])\s+/.test(lines[0])) return mdList(lines);
    return '<p>' + lines.map(inline).join('<br>') + '</p>';
  }).join('');
  return clean(out);
}
function mdLine(s) { return clean(inline(String(s || '').replace(/\n/g, ' '))); }   // an option: one line
function caseBox(a, q) {                               // the case study this question refers to
  var c = q.case && (a.cases || {})[q.case];
  if (!c) return null;
  var d = document.createElement('details'); d.className = 'case'; d.open = true;
  d.innerHTML = '<summary></summary><div class="md"></div>';
  d.firstChild.textContent = '📄 Case study — ' + c.title; d.lastChild.innerHTML = md(c.content);
  return d;
}
var SLOT_HINT = {hotspot: 'pick a value in each box', 'drag-drop': 'pick a value for each target or step', 'yes-no': 'Yes or No for each statement'};
function fmt(n) { return String(Math.round(n * 10) / 10); }
