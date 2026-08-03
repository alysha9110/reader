/* Reader — incremental reading, phone-first, local-only.
   Data: sources (books) + items (extracts and notes). One queue. */

var DAY = 86400000;
var MULT = { 1: 1.3, 2: 1.5, 3: 1.7, 4: 2.1, 5: 2.8 };
var CHUNK = 30;
var SHELF_MAX = 5;

var S = { sources: [], items: [], cfg: {}, view: null, read: null, cur: null };

/* ---------- tiny helpers ---------- */
function $(id) { return document.getElementById(id); }
function el(tag, cls, txt) {
  var n = document.createElement(tag);
  if (cls) n.className = cls;
  if (txt != null) n.textContent = txt;
  return n;
}
function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 7); }
function esc(s) {
  return String(s).replace(/[&<>"]/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
  });
}
function slug(s) {
  return String(s).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 48) || 'untitled';
}
function days(ms) { return Math.round(ms / DAY); }
function toast(msg) {
  var t = $('toast');
  t.textContent = msg;
  t.classList.add('on');
  clearTimeout(toast._t);
  toast._t = setTimeout(function () { t.classList.remove('on'); }, 1900);
}
function plural(n, word) { return n + ' ' + word + (n === 1 ? '' : 's'); }

/* ---------- storage ---------- */
var DB = null;
function open() {
  return new Promise(function (res, rej) {
    var r = indexedDB.open('reader', 1);
    r.onupgradeneeded = function () {
      var d = r.result;
      if (!d.objectStoreNames.contains('sources')) d.createObjectStore('sources', { keyPath: 'id' });
      if (!d.objectStoreNames.contains('items')) d.createObjectStore('items', { keyPath: 'id' });
      if (!d.objectStoreNames.contains('cfg')) d.createObjectStore('cfg', { keyPath: 'k' });
    };
    r.onsuccess = function () { DB = r.result; res(); };
    r.onerror = function () { rej(r.error); };
  });
}
function all(store) {
  return new Promise(function (res, rej) {
    var r = DB.transaction(store).objectStore(store).getAll();
    r.onsuccess = function () { res(r.result || []); };
    r.onerror = function () { rej(r.error); };
  });
}
function put(store, obj) {
  return new Promise(function (res, rej) {
    var tx = DB.transaction(store, 'readwrite');
    tx.objectStore(store).put(obj);
    tx.oncomplete = function () { res(); };
    tx.onerror = function () { rej(tx.error); };
  });
}
function del(store, id) {
  return new Promise(function (res) {
    var tx = DB.transaction(store, 'readwrite');
    tx.objectStore(store).delete(id);
    tx.oncomplete = function () { res(); };
  });
}
function saveCfg() { return put('cfg', { k: 'cfg', v: S.cfg }); }

/* ---------- relative priority ----------
   Priority is a position in one global ordering, not a bucket. The 1-5 chips
   are an input; what is stored is `rank`, a float in 0..1, and what matters is
   where you sit relative to everything else. Two hundred items all set to 3
   would make buckets meaningless; ranking spreads them and keeps the top of
   the queue the actual top. */
var BAND = { 1: [0, 0.10], 2: [0.10, 0.30], 3: [0.30, 0.60], 4: [0.60, 0.85], 5: [0.85, 1] };

function ranked(exclude) {
  return S.items.concat(S.sources).filter(function (o) {
    return inQueue(o) && (!exclude || o.id !== exclude.id);
  }).sort(function (a, b) { return (a.rank || 0.5) - (b.rank || 0.5); });
}

/* Place an item at the top of its band, so a fresh judgement outranks a stale
   one at the same level. Fractional insertion keeps a real total order. */
function place(o, bucket) {
  bucket = Math.max(1, Math.min(5, Math.round(bucket)));
  var list = ranked(o);
  var at = Math.floor(BAND[bucket][0] * list.length);
  var prev = at > 0 ? list[at - 1].rank : 0;
  var next = at < list.length ? list[at].rank : 1;
  o.priority = bucket;
  o.rank = (prev + next) / 2;
  if (next - prev < 1e-6) renormalise();
}

/* Repeated insertion in one spot eventually exhausts float precision.
   Spread everything evenly when gaps get too small. */
function renormalise() {
  var list = ranked();
  list.forEach(function (o, i) {
    o.rank = (i + 0.5) / list.length;
    put(o.kind === 'source' ? 'sources' : 'items', o);
  });
}

function percentile(o) {
  var list = ranked();
  var i = list.indexOf(o);
  if (i < 0 || !list.length) return null;
  return Math.max(1, Math.round((i + 1) / list.length * 100));
}
function pctLabel(o) {
  var p = percentile(o);
  return p == null ? 'p' + Math.round(o.priority) : 'top ' + p + '%';
}

/* Existing collections have buckets but no ranks. Lay them out once. */
function migrateRanks() {
  var need = S.items.concat(S.sources).filter(function (o) { return typeof o.rank !== 'number'; });
  if (!need.length) return false;
  var list = S.items.concat(S.sources).sort(function (a, b) {
    var d = (a.priority || 3) - (b.priority || 3);
    return d || (a.createdAt || a.addedAt || 0) - (b.createdAt || b.addedAt || 0);
  });
  list.forEach(function (o, i) {
    if (typeof o.rank !== 'number') o.rank = (i + 0.5) / list.length;
    put(o.kind === 'source' ? 'sources' : 'items', o);
  });
  return true;
}

/* ---------- scheduling ---------- */
function clampP(p) { return Math.max(1, Math.min(5, p)); }
function multFor(p) { return MULT[Math.max(1, Math.min(5, Math.round(p)))]; }

function bump(o, factor) {
  var m = Math.pow(multFor(o.priority), factor || 1);
  var base = o.interval || 0.7;
  /* max(base+1, ...) so priority 1 still expands. round(1 * 1.3) is 1,
     which would otherwise trap urgent items at a one-day interval forever. */
  o.interval = Math.max(1, Math.floor(base) + 1, Math.round(base * m));
  o.dueAt = Date.now() + o.interval * DAY;
  o.reps = (o.reps || 0) + 1;
}

/* stable per-day jitter so ordering does not jump around mid-session */
function jitter(id) {
  var h = 0, i;
  for (i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) | 0;
  h = (h ^ (Math.floor(Date.now() / DAY) * 2654435761)) | 0;
  return ((h >>> 0) % 1000) / 1000 * 0.45;
}

/* Finished books stay in the queue. They come back rarely, opened at a random
   stretch, so you can skim for what you missed the first time. */
function inQueue(o) {
  if (o.kind === 'source') return !o.archived;
  return o.state === 'open';
}
function isRevisit(s) { return !!s.finishedAt; }

function queue(includeNotDue) {
  var now = Date.now();
  var out = [];
  S.sources.forEach(function (s) {
    if (inQueue(s) && (includeNotDue || s.dueAt <= now)) out.push(s);
  });
  S.items.forEach(function (it) {
    if (inQueue(it) && (includeNotDue || it.dueAt <= now)) out.push(it);
  });
  /* jitter is scaled to rank space: a few percent of shuffle, enough to stop
     eleven extracts from the same book arriving in a row */
  out.sort(function (a, b) {
    var d = ((a.rank || 0.5) + jitter(a.id) * 0.08) - ((b.rank || 0.5) + jitter(b.id) * 0.08);
    if (Math.abs(d) > 1e-9) return d;
    return a.dueAt - b.dueAt;
  });
  return out;
}

/* The queue should never be empty while there is something to read.
   When nothing is due, offer the highest-priority book you have not finished. */
function nextUp() {
  var q = queue();
  if (q.length) return { item: q[0], due: q.length };
  var books = S.sources.filter(function (s) {
    return !s.archived && !s.finishedAt && s.position < s.blocks.length;
  }).sort(function (a, b) { return a.priority - b.priority; });
  return { item: books[0] || null, due: 0 };
}

/* When the due pile outgrows what you can clear, spread it out.
   Lowest priority gets pushed hardest, and drifts down a notch. */
function autoPostpone() {
  var limit = S.cfg.overload || 40;
  var due = queue();
  if (due.length <= limit) return 0;
  /* Pushed out by rank, so the bottom of the collection absorbs the overload
     and the top of the queue is untouched. Each pushed item also slides down
     the ordering a little, which is the pile-up doing its own triage. */
  var excess = due.slice(limit);
  excess.forEach(function (o, i) {
    o.dueAt = Date.now() + (1 + Math.floor(i / 12)) * DAY;
    o.rank = Math.min(0.9999, (o.rank || 0.5) + 0.02);
    put(o.kind === 'source' ? 'sources' : 'items', o);
  });
  return excess.length;
}

/* ---------- EPUB ---------- */
function textOf(doc, tag) {
  var n = doc.getElementsByTagNameNS('*', tag)[0];
  return n ? n.textContent.trim() : '';
}
function joinPath(dir, href) {
  var p = (dir + href).split('/');
  var out = [];
  p.forEach(function (seg) {
    if (seg === '.' || seg === '') return;
    if (seg === '..') out.pop(); else out.push(seg);
  });
  return out.join('/');
}
/* Inline formatting we keep. Everything else is unwrapped to its text,
   so nothing arbitrary from an EPUB ever reaches innerHTML. */
var INLINE = { EM: 'em', I: 'em', STRONG: 'strong', B: 'strong', CITE: 'em',
  SUP: 'sup', SUB: 'sub', CODE: 'code', SMALL: 'small', U: 'u', MARK: 'mark' };

function inlineHtml(node) {
  var out = '';
  var kids = node.childNodes;
  for (var i = 0; i < kids.length; i++) {
    var c = kids[i];
    if (c.nodeType === 3) { out += esc(c.nodeValue); continue; }
    if (c.nodeType !== 1) continue;
    var tag = INLINE[c.tagName.toUpperCase()];
    var inner = inlineHtml(c);
    out += tag ? '<' + tag + '>' + inner + '</' + tag + '>' : inner;
  }
  return out;
}
function tidyHtml(h) {
  return h.replace(/\s+/g, ' ')
    .replace(/<(em|strong|sup|sub|code|small|u|mark)>\s*<\/\1>/g, '')
    .trim();
}
/* html is stored only when it adds something over the plain text */
function richOf(node, plain) {
  var h = tidyHtml(inlineHtml(node));
  return h === esc(plain) ? null : h;
}
function renderRich(node, o) {
  if (o.h || o.html) node.innerHTML = o.h || o.html;
  else node.textContent = o.x != null ? o.x : o.text;
}

function blocksFromHtml(html) {
  var doc = new DOMParser().parseFromString(html, 'text/html');
  ['script', 'style', 'nav'].forEach(function (t) {
    Array.prototype.slice.call(doc.getElementsByTagName(t)).forEach(function (n) { n.remove(); });
  });
  var sel = 'h1,h2,h3,h4,h5,h6,p,blockquote,li';
  var nodes = Array.prototype.slice.call(doc.querySelectorAll(sel));
  var set = new Set(nodes);
  var out = [];
  nodes.forEach(function (n) {
    var a = n.parentElement;
    while (a) { if (set.has(a)) return; a = a.parentElement; }
    var t = (n.textContent || '').replace(/\s+/g, ' ').trim();
    if (t.length < 2) return;
    var tag = n.tagName.toLowerCase();
    var b = { t: /^h[1-6]$/.test(tag) ? 'h' : (tag === 'blockquote' ? 'q' : 'p'), x: t };
    var h = richOf(n, t);
    if (h) b.h = h;
    out.push(b);
  });
  return out;
}

/* Contents comes from the headings we already parsed. An NCX or nav document
   would give the same chapter names with far more machinery, and books that
   have neither still get a usable list this way. */
function buildToc(blocks, starts) {
  var toc = blocks.map(function (b, i) {
    return b.t === 'h' ? { i: i, label: b.x.slice(0, 70) } : null;
  }).filter(Boolean);
  if (toc.length >= 3 && toc.length <= 400) return toc;
  return (starts || []).map(function (i, n) {
    var b = blocks[i];
    return { i: i, label: b ? b.x.slice(0, 70) : 'Section ' + (n + 1) };
  }).filter(function (t, n, a) { return n === 0 || t.i !== a[n - 1].i; });
}

/* html for the current selection, so shortening keeps its italics */
function selRich(container) {
  var sel = window.getSelection();
  if (!sel || sel.isCollapsed) return null;
  if (!container.contains(sel.anchorNode)) return null;
  var frag = sel.getRangeAt(0).cloneContents();
  var plain = sel.toString().replace(/\s+/g, ' ').trim();
  var h = tidyHtml(inlineHtml(frag));
  return { text: plain, html: h === esc(plain) ? null : h };
}

function parseEpub(file) {
  return JSZip.loadAsync(file).then(function (zip) {
    var cf = zip.file('META-INF/container.xml');
    if (!cf) throw new Error('Not a valid EPUB (no container.xml)');
    return cf.async('string').then(function (xml) {
      var cdoc = new DOMParser().parseFromString(xml, 'application/xml');
      var rf = cdoc.getElementsByTagNameNS('*', 'rootfile')[0];
      var opfPath = rf.getAttribute('full-path');
      var dir = opfPath.indexOf('/') >= 0 ? opfPath.slice(0, opfPath.lastIndexOf('/') + 1) : '';
      return zip.file(opfPath).async('string').then(function (opfXml) {
        var o = new DOMParser().parseFromString(opfXml, 'application/xml');
        var title = textOf(o, 'title') || file.name.replace(/\.epub$/i, '');
        var author = textOf(o, 'creator') || '';
        var map = {};
        Array.prototype.slice.call(o.getElementsByTagNameNS('*', 'item')).forEach(function (n) {
          map[n.getAttribute('id')] = {
            href: decodeURIComponent(n.getAttribute('href') || ''),
            type: n.getAttribute('media-type') || ''
          };
        });
        var order = [];
        Array.prototype.slice.call(o.getElementsByTagNameNS('*', 'itemref')).forEach(function (n) {
          var m = map[n.getAttribute('idref')];
          if (m && /xhtml|html|xml/.test(m.type)) order.push(joinPath(dir, m.href));
        });
        var blocks = [];
        var starts = [];
        var chain = Promise.resolve();
        order.forEach(function (p) {
          chain = chain.then(function () {
            var f = zip.file(p);
            if (!f) return;
            return f.async('string').then(function (h) {
              starts.push(blocks.length);
              blocksFromHtml(h).forEach(function (b) { blocks.push(b); });
            }).catch(function () {});
          });
        });
        return chain.then(function () {
          if (!blocks.length) throw new Error('No readable text found in that EPUB');
          return { title: title, author: author, blocks: blocks, toc: buildToc(blocks, starts) };
        });
      });
    });
  });
}

function parsePlain(file) {
  return file.text().then(function (txt) {
    var blocks = txt.split(/\n\s*\n/).map(function (p) {
      var t = p.replace(/\s+/g, ' ').trim();
      if (!t) return null;
      return { t: /^#{1,6}\s/.test(p.trim()) ? 'h' : 'p', x: t.replace(/^#{1,6}\s*/, '') };
    }).filter(Boolean);
    if (!blocks.length) throw new Error('That file looks empty');
    return { title: file.name.replace(/\.(txt|md)$/i, ''), author: '', blocks: blocks };
  });
}

function addSource(file) {
  toast('Reading file...');
  var p = /\.epub$/i.test(file.name) ? parseEpub(file) : parsePlain(file);
  return p.then(function (d) {
    var s = {
      id: uid(), kind: 'source', title: d.title, author: d.author, blocks: d.blocks,
      toc: d.toc || [], position: 0, priority: 3, rank: 0.5,
      interval: 0, dueAt: Date.now(), reps: 0,
      archived: false, addedAt: Date.now(), createdAt: Date.now(), finishedAt: null
    };
    S.sources.push(s);
    place(s, 3);
    return put('sources', s).then(function () {
      toast(d.title + ' added, ' + plural(d.blocks.length, 'paragraph'));
      home();
    });
  }).catch(function (e) {
    toast(e.message || 'Could not read that file');
  });
}

/* ---------- items ---------- */
function srcOf(o) {
  for (var i = 0; i < S.sources.length; i++) if (S.sources[i].id === o.sourceId) return S.sources[i];
  return null;
}
function notesOf(id) {
  return S.items.filter(function (n) { return n.type === 'note' && n.parentId === id; });
}
function byId(id) {
  for (var i = 0; i < S.items.length; i++) if (S.items[i].id === id) return S.items[i];
  return null;
}

function makeItem(type, text, source, parentId, blockIdx, html) {
  /* Extracts come back the same evening, not tomorrow. A queue that is empty
     on the day you made everything in it looks broken. */
  var firstGap = type === 'note' ? 0.75 : 0.25;
  var it = {
    id: uid(), kind: 'item', type: type, sourceId: source ? source.id : null,
    parentId: parentId || null, text: text, html: html || null, history: [],
    blockIdx: blockIdx == null ? null : blockIdx,
    priority: 3, rank: 0.5,
    interval: 1, reps: 0,
    dueAt: Date.now() + firstGap * DAY,
    timesShortened: 0, state: 'open', cardType: null, cloze: '', front: '', back: '',
    createdAt: Date.now()
  };
  S.items.push(it);
  place(it, type === 'note'
    ? (source ? source.priority : 3)
    : (S.cfg.defaultPriority || 4));
  put('items', it);
  return it;
}

function save(o) { return put(o.kind === 'source' ? 'sources' : 'items', o); }

function removeItem(it) {
  notesOf(it.id).forEach(function (n) { removeItem(n); });
  S.items = S.items.filter(function (x) { return x.id !== it.id; });
  del('items', it.id);
}

/* ---------- navigation ---------- */
function show(id) {
  ['s-home', 's-read', 's-item', 's-list'].forEach(function (x) {
    $(x).classList.toggle('on', x === id);
  });
  S.view = id;
  window.scrollTo(0, 0);
}

/* ---------- home ---------- */
function home() {
  show('s-home');
  S.back = null;
  var q = queue();
  var d = new Date();
  $('today').textContent = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][d.getDay()];
  $('duecount').textContent = q.length ? q.length + ' due' : 'all clear';

  var nc = $('nextcard');
  nc.innerHTML = '';
  var up = nextUp();
  if (!up.item) {
    nc.style.background = 'transparent';
    nc.style.padding = '0 0 8px';
    var e = el('p', 'empty', S.sources.length ? 'Nothing left to read. Add a book.' : 'Add a book to begin.');
    nc.appendChild(e);
  } else {
    nc.style.background = '';
    nc.style.padding = '';
    var top = up.item;
    var head = el('div', 'row');
    head.appendChild(el('span', 'eyebrow', up.due ? 'Up next' : 'Nothing due'));
    var kindLabel = top.kind === 'source' ? (isRevisit(top) ? 'revisit' : 'read') : top.type;
    head.appendChild(el('span', 'meta', kindLabel + ' · ' + pctLabel(top)));
    nc.appendChild(head);
    var body = el('p', 'body', top.kind === 'source' ? top.title : top.text.slice(0, 150) + (top.text.length > 150 ? '…' : ''));
    nc.appendChild(body);
    var b = el('button', '', up.due ? 'Start queue' : 'Keep reading');
    b.onclick = function () { openQ(top); };
    nc.appendChild(b);
    if (q.length > 1) {
      var rest = q.slice(1, 6).filter(function (x) { return x.kind !== 'source'; }).length;
      var books = q.slice(1).filter(function (x) { return x.kind === 'source'; }).length;
      var bits = [];
      if (rest) bits.push(plural(rest, 'extract') + ' after this');
      if (books) bits.push(plural(books, 'book') + ' waiting');
      if (bits.length) {
        var m = el('p', 'meta', bits.join(' · '));
        m.style.marginTop = '10px';
        m.style.textAlign = 'center';
        nc.appendChild(m);
      }
    }
  }

  var active = S.sources.filter(function (s) { return !s.archived; });
  $('shelfcount').textContent = active.length + ' of ' + SHELF_MAX;
  var sh = $('shelf');
  sh.innerHTML = '';
  if (!active.length) {
    sh.appendChild(el('p', 'empty', 'No books yet.'));
  }
  active.sort(function (a, b) { return a.priority - b.priority; }).forEach(function (s) {
    var pending = S.items.filter(function (i) {
      return i.sourceId === s.id && i.state === 'open';
    }).length;
    var row = el('div', 'shelfitem');
    var r1 = el('div', 'row');
    r1.appendChild(el('span', 'serif', s.title));
    var p = el('span', 'pill' + (pending >= 8 ? ' warm' : ''), String(pending));
    r1.appendChild(p);
    row.appendChild(r1);
    var bar = el('div', 'bar');
    var fill = el('i');
    var pct = s.blocks.length ? Math.round(s.position / s.blocks.length * 100) : 0;
    fill.style.width = pct + '%';
    bar.appendChild(fill);
    row.appendChild(bar);
    row.appendChild(el('span', 'ghost',
      (s.author ? s.author + ' · ' : '') + pct + '% · priority ' + Math.round(s.priority)));
    row.onclick = function () { openReader(s); };
    sh.appendChild(row);
  });
}

function openQ(o) {
  if (o.kind === 'source') openReader(o);
  else openItem(o);
}

/* ---------- reader ---------- */
function openReader(s, at) {
  var revisit = at == null && isRevisit(s);
  var start = at != null ? at
    : revisit ? Math.floor(Math.random() * Math.max(1, s.blocks.length - CHUNK))
      : s.position;
  S.read = { src: s, start: start, end: 0, taken: {}, revisit: revisit };
  show('s-read');
  $('r-title').textContent = s.title + (revisit ? ' · revisit' : '');
  $('page').innerHTML = '';
  renderChunk(start);
  updateReadHead();
  $('r-hint').textContent = revisit
    ? 'Anything you missed?'
    : 'Tap a paragraph to extract it';
  $('r-hint').style.color = '';
}

var MAX_NODES = 500;

function makePara(b, i) {
  var p = el('p', 'para' + (b.t === 'h' ? ' h' : b.t === 'q' ? ' q' : ''));
  renderRich(p, b);
  p.dataset.i = i;
  p.onclick = onTapPara;
  return p;
}

function renderChunk(from) {
  var s = S.read.src;
  var end = Math.min(s.blocks.length, from + CHUNK);
  var page = $('page');
  for (var i = from; i < end; i++) page.appendChild(makePara(s.blocks[i], i));
  S.read.end = end;
  if (S.read.start == null || from < S.read.start) S.read.start = from;
  if (end >= s.blocks.length && !$('fin')) {
    var fin = el('p', 'empty', 'End of book.');
    fin.id = 'fin';
    page.appendChild(fin);
  }
  trimTop();
  refreshMarks();
}

/* Scrolling back needs earlier paragraphs put above what is on screen, and
   the viewport held still while that happens. Without the compensation the
   page jumps by the height of everything inserted. */
function prependChunk() {
  var s = S.read.src;
  var from = Math.max(0, S.read.start - CHUNK);
  if (from >= S.read.start) return false;
  var page = $('page');
  var frag = document.createDocumentFragment();
  for (var i = from; i < S.read.start; i++) frag.appendChild(makePara(s.blocks[i], i));
  var before = document.body.scrollHeight;
  page.insertBefore(frag, page.firstChild);
  window.scrollBy(0, document.body.scrollHeight - before);
  S.read.start = from;
  trimBottom();
  refreshMarks();
  return true;
}

/* Keep the DOM bounded on long books. Trimming happens at the far end from
   where you are reading, so nothing you can see moves. */
function trimTop() {
  var page = $('page');
  while (S.read.end - S.read.start > MAX_NODES && S.read.start < S.read.end - CHUNK) {
    var before = document.body.scrollHeight;
    for (var n = 0; n < CHUNK; n++) {
      var first = page.querySelector('.para');
      if (!first) break;
      first.remove();
    }
    S.read.start += CHUNK;
    window.scrollBy(0, document.body.scrollHeight - before);
  }
}
function trimBottom() {
  var page = $('page');
  while (S.read.end - S.read.start > MAX_NODES) {
    for (var n = 0; n < CHUNK; n++) {
      var ps = page.querySelectorAll('.para');
      if (!ps.length) break;
      ps[ps.length - 1].remove();
    }
    S.read.end -= CHUNK;
    var f = $('fin');
    if (f) f.remove();
  }
}

function onTapPara(e) {
  if (window.getSelection && !window.getSelection().isCollapsed) return;
  var p = e.currentTarget;
  var i = +p.dataset.i;
  var s = S.read.src;
  if (S.read.taken[i]) {
    removeItem(S.read.taken[i]);
    S.read.last = null;
    hidePriBar();
    refreshMarks();
  } else {
    var b = s.blocks[i];
    var it = makeItem('extract', b.x, s, null, i, b.h || null);
    refreshMarks();
    if (navigator.vibrate) navigator.vibrate(8);
    showPriBar(it);
  }
}

function countHint() {
  var n = 0;
  for (var k in S.read.taken) if (S.read.taken[k]) n++;
  var h = $('r-hint');
  h.textContent = n === 0 ? 'Tap a paragraph to extract it'
    : plural(n, 'extract') + ' here · tap again to undo';
  h.style.color = n === 0 ? '' : 'var(--green)';
}

/* Priority right where the extract was made. Cheap to promote something
   the moment you feel it matters, and invisible when you do not care. */
function showPriBar(item) {
  S.read.last = item;
  var bar = $('pribar');
  bar.innerHTML = '';
  bar.appendChild(el('span', 'ghost', 'Priority'));
  var chips = el('div', 'chips');
  [1, 2, 3, 4, 5].forEach(function (p) {
    var b = el('button', Math.round(item.priority) === p ? 'sel' : '', String(p));
    b.onclick = function (ev) {
      ev.stopPropagation();
      place(item, p);
      save(item);
      showPriBar(item);
      toast(pctLabel(item));
    };
    chips.appendChild(b);
  });
  bar.appendChild(chips);
  var grow = el('button', '', '+¶');
  grow.title = 'Add the next paragraph to this extract';
  grow.onclick = function (ev) { ev.stopPropagation(); growExtract(item); };
  bar.appendChild(grow);
  bar.classList.add('on');
  clearTimeout(showPriBar._t);
  showPriBar._t = setTimeout(hidePriBar, 6000);
}
function hidePriBar() {
  clearTimeout(showPriBar._t);
  $('pribar').classList.remove('on');
}

/* Swallow the next paragraph into this extract. Four taps beats four drags,
   and on a phone dragging across a paragraph break is genuinely awful. */
function growExtract(item) {
  var s = S.read.src;
  var last = item.lastBlock == null ? item.blockIdx : item.lastBlock;
  if (last == null) return toast('This extract is not tied to a paragraph');
  var n = last + 1;
  if (n >= s.blocks.length) return toast('End of the book');
  var b = s.blocks[n];
  var curHtml = item.html || esc(item.text);
  item.text = item.text + ' ' + b.x;
  var merged = curHtml + ' ' + (b.h || esc(b.x));
  item.html = merged === esc(item.text) ? null : merged;
  item.lastBlock = n;
  save(item);
  refreshMarks();
  showPriBar(item);
  if (navigator.vibrate) navigator.vibrate(6);
  var span = n - (item.blockIdx == null ? n : item.blockIdx) + 1;
  toast(plural(span, 'paragraph') + ' in this extract');
  var node = $('page').querySelector('.para[data-i="' + n + '"]');
  if (node) node.scrollIntoView({ block: 'center', behavior: 'smooth' });
}

/* One place that decides what every visible paragraph looks like */
function refreshMarks() {
  if (!S.read) return;
  var s = S.read.src;
  var whole = {}, part = {};
  S.items.forEach(function (it) {
    if (it.sourceId !== s.id || it.blockIdx == null) return;
    var to = it.lastBlock == null ? it.blockIdx : it.lastBlock;
    for (var i = it.blockIdx; i <= to; i++) {
      var b = s.blocks[i];
      if (to > it.blockIdx || (b && it.text.indexOf(b.x) !== -1)) whole[i] = it;
      else part[i] = it;
    }
  });
  S.read.taken = whole;
  Array.prototype.slice.call($('page').querySelectorAll('.para')).forEach(function (p) {
    var i = +p.dataset.i;
    p.classList.toggle('taken', !!whole[i]);
    p.classList.toggle('partial', !whole[i] && !!part[i]);
  });
  countHint();
}

function tocSheet() {
  var s = S.read.src;
  var toc = s.toc || [];
  if (!toc.length) return toast('No chapters found in this book');
  sheet(function (w) {
    w.appendChild(el('h2', '', 'Contents'));
    var here = 0;
    toc.forEach(function (t, n) { if (t.i <= s.position) here = n; });
    var list = el('div', '');
    list.style.marginTop = '12px';
    toc.forEach(function (t, n) {
      var r = el('div', 'shelfitem');
      var row = el('div', 'row');
      var lab = el('span', 'serif', t.label);
      if (n === here) lab.style.color = 'var(--amber)';
      row.appendChild(lab);
      row.appendChild(el('span', 'ghost',
        Math.round(t.i / s.blocks.length * 100) + '%'));
      r.appendChild(row);
      r.onclick = function () {
        closeSheet();
        s.position = t.i;
        save(s);
        openReader(s, t.i);
      };
      list.appendChild(r);
    });
    w.appendChild(list);
  });
}

function firstVisible() {
  var ps = $('page').querySelectorAll('.para');
  var top = window.scrollY + 90;
  var best = S.read.start;
  for (var i = 0; i < ps.length; i++) {
    if (ps[i].offsetTop <= top) best = +ps[i].dataset.i; else break;
  }
  return best;
}

function updateReadHead() {
  var s = S.read.src;
  var i = firstVisible();
  var pct = s.blocks.length ? Math.round(i / s.blocks.length * 100) : 0;
  $('r-pos').textContent = pct + '%';
  $('r-bar').style.width = pct + '%';
}

function endSession() {
  var s = S.read.src;
  if (S.read.revisit) {
    bump(s);
    toast('Back in ' + plural(s.interval, 'day'));
  } else {
    s.position = Math.min(Math.max(s.position, firstVisible()), s.blocks.length);
    if (s.position >= s.blocks.length && !s.finishedAt) {
      s.finishedAt = Date.now();
      place(s, clampP(s.priority + 1));
      s.interval = 21;
      s.dueAt = Date.now() + 21 * DAY;
      toast('Finished. It will resurface in three weeks.');
    } else {
      bump(s);
    }
  }
  save(s);
  S.read = null;
  home();
}

/* ---------- item review ---------- */
function openItem(it) {
  S.cur = it;
  show('s-item');
  drawItem();
}

function selText(container) {
  var sel = window.getSelection();
  if (!sel || sel.isCollapsed) return '';
  if (!container.contains(sel.anchorNode)) return '';
  return sel.toString().replace(/\s+/g, ' ').trim();
}

function drawItem() {
  var it = S.cur;
  var root = $('s-item');
  root.innerHTML = '';
  var src = srcOf(it);
  var notes = notesOf(it.id);

  var head = el('div', 'row');
  var pill = el('span', 'pill ' + (it.type === 'note' ? 'cool' : 'warm'), it.type);
  head.appendChild(pill);
  var close = el('button', 'plain', S.back ? 'Back' : 'Close');
  close.onclick = function () { (S.back || home)(); };
  head.appendChild(close);
  root.appendChild(head);

  var body = el('p', 'serif');
  body.style.cssText = 'font-size:18px;line-height:1.7;margin:18px 0 10px';
  renderRich(body, it);
  body.id = 'itemtext';
  root.appendChild(body);

  var meta = [];
  meta.push(it.reps ? 'seen ' + plural(it.reps, 'time') : 'first time');
  if (it.timesShortened) meta.push('shortened ' + (it.timesShortened === 1 ? 'once' : it.timesShortened + ' times'));
  meta.push(plural(Math.max(0, days(Date.now() - it.createdAt)), 'day') + ' old');
  if (src) meta.push(src.title);
  root.appendChild(el('p', 'ghost', meta.join(' · ')));

  if (it.history.length) {
    var hbtn = el('button', 'plain', 'Show original');
    hbtn.onclick = function () {
      sheet(function (w) {
        w.appendChild(el('h2', '', 'Earlier versions'));
        it.history.slice().reverse().forEach(function (h) {
          var c = el('div', 'card');
          c.style.marginTop = '12px';
          c.appendChild(el('p', 'serif', h));
          w.appendChild(c);
        });
      });
    };
    root.appendChild(hbtn);
  }

  var sel = el('p', 'ghost');
  sel.style.marginTop = '14px';
  sel.textContent = 'Select part of the text above to shorten or split it.';
  root.appendChild(sel);

  var narrow = el('div', 'chips');
  narrow.style.marginTop = '8px';
  var keep = el('button', '', 'Keep selection');
  keep.onclick = function () {
    var r = selRich(body);
    if (!r || !r.text) return toast('Select some text first');
    it.history.push(it.text);
    it.text = r.text;
    it.html = r.html;
    it.timesShortened++;
    save(it);
    drawItem();
    toast('Shortened');
  };
  var split = el('button', '', 'Split off');
  split.onclick = function () {
    var r = selRich(body);
    if (!r || !r.text) return toast('Select some text first');
    var rest = it.text.replace(r.text, '').replace(/\s+/g, ' ').trim();
    if (!rest) return toast('That is the whole thing, use Keep selection');
    var child = makeItem('extract', r.text, src, it.id, it.blockIdx, r.html);
    child.priority = it.priority;
    save(child);
    it.history.push(it.text);
    it.text = rest;
    it.html = null;
    it.timesShortened++;
    save(it);
    drawItem();
    toast('Split into two');
  };
  narrow.appendChild(keep);
  narrow.appendChild(split);
  root.appendChild(narrow);

  var nwrap = el('div', '');
  nwrap.style.marginTop = '20px';
  if (notes.length) {
    nwrap.appendChild(el('span', 'eyebrow', 'Your notes'));
    notes.forEach(function (n) {
      var c = el('div', 'note');
      var t = el('p', 't', n.text);
      c.appendChild(t);
      var r = el('div', 'row');
      r.style.marginTop = '6px';
      r.appendChild(el('span', 'ghost', new Date(n.createdAt).toLocaleDateString()));
      var dn = el('button', 'plain', 'Delete');
      dn.onclick = function () { removeItem(n); drawItem(); };
      r.appendChild(dn);
      c.appendChild(r);
      nwrap.appendChild(c);
    });
  }
  var addn = el('button', 'wide', notes.length ? '+ Another thought' : '+ Add your thought');
  addn.style.marginTop = '10px';
  addn.onclick = function () { noteSheet(it); };
  nwrap.appendChild(addn);
  root.appendChild(nwrap);

  var pr = el('div', '');
  pr.style.marginTop = '22px';
  pr.appendChild(el('span', 'eyebrow', 'Priority'));
  var chips = el('div', 'chips');
  chips.style.marginTop = '8px';
  [1, 2, 3, 4, 5].forEach(function (p) {
    var b = el('button', Math.round(it.priority) === p ? 'sel' : '', String(p));
    b.onclick = function () { place(it, p); save(it); drawItem(); };
    chips.appendChild(b);
  });
  pr.appendChild(chips);
  var pl = el('p', 'ghost', pctLabel(it) + ' of everything waiting');
  pl.style.marginTop = '6px';
  pr.appendChild(pl);
  root.appendChild(pr);

  var acts = el('div', '');
  acts.style.marginTop = '22px';

  var canCard = it.type === 'note' || notes.length > 0;
  var mk = el('button', 'solid wide', 'Make card');
  mk.disabled = !canCard;
  mk.onclick = function () { cardSheet(it); };
  acts.appendChild(mk);
  if (!canCard) {
    var why = el('p', 'ghost', 'Write a thought in your own words first. If you cannot, you do not understand it yet.');
    why.style.cssText = 'margin-top:8px;text-align:center;line-height:1.5';
    acts.appendChild(why);
  }

  var row2 = el('div', 'chips');
  row2.style.marginTop = '10px';
  var later = el('button', '', 'Later');
  later.onclick = function () { bump(it); save(it); toast('Back in ' + plural(it.interval, 'day')); nextInQueue(); };
  var push = el('button', '', 'Much later');
  push.onclick = function () { bump(it, 2); save(it); toast('Back in ' + plural(it.interval, 'day')); nextInQueue(); };
  row2.appendChild(later);
  row2.appendChild(push);
  acts.appendChild(row2);

  var row3 = el('div', 'chips');
  row3.style.marginTop = '10px';
  var keepn = el('button', '', 'Retire');
  keepn.onclick = function () {
    it.state = 'kept';
    save(it);
    toast('Kept as a note, out of the queue');
    nextInQueue();
  };
  var dele = el('button', '', 'Delete');
  dele.style.color = 'var(--red)';
  dele.onclick = function () { removeItem(it); toast('Deleted'); nextInQueue(); };
  row3.appendChild(keepn);
  row3.appendChild(dele);
  acts.appendChild(row3);

  if (src && it.blockIdx != null) {
    var ctx = el('button', 'plain wide', 'Show in book');
    ctx.style.marginTop = '12px';
    ctx.onclick = function () { openReader(src, Math.max(0, it.blockIdx - 3)); };
    acts.appendChild(ctx);
  }
  root.appendChild(acts);
}

function nextInQueue() {
  if (S.back) return S.back();
  var q = queue();
  if (!q.length) return home();
  var n = q[0];
  if (n.id === (S.cur && S.cur.id)) n = q[1];
  if (!n) return home();
  openQ(n);
}

/* ---------- sheets ---------- */
function sheet(build) {
  var w = $('sheet-body');
  w.innerHTML = '';
  build(w, closeSheet);
  $('sheet').classList.add('on');
}
function closeSheet() { $('sheet').classList.remove('on'); }
$('sheet').addEventListener('click', function (e) { if (e.target.id === 'sheet') closeSheet(); });

function noteSheet(parent) {
  sheet(function (w) {
    w.appendChild(el('h2', '', 'Your thought'));
    var p = el('p', 'ghost', 'In your own words. This becomes its own item and will come back to you.');
    p.style.margin = '6px 0 12px';
    w.appendChild(p);
    var ta = document.createElement('textarea');
    ta.rows = 5;
    ta.placeholder = 'This connects to…';
    w.appendChild(ta);
    var row = el('div', 'chips');
    row.style.marginTop = '12px';
    var cancel = el('button', '', 'Cancel');
    cancel.onclick = closeSheet;
    var ok = el('button', 'solid', 'Save');
    ok.onclick = function () {
      var t = ta.value.trim();
      if (!t) return closeSheet();
      makeItem('note', t, srcOf(parent), parent.id, parent.blockIdx);
      closeSheet();
      drawItem();
      toast('Noted');
    };
    row.appendChild(cancel);
    row.appendChild(ok);
    w.appendChild(row);
    setTimeout(function () { ta.focus(); }, 60);
  });
}

function cardSheet(it) {
  sheet(function (w) {
    w.appendChild(el('h2', '', 'Make a card'));
    var mode = { v: it.cardType || 'cloze' };
    var tabs = el('div', 'chips');
    tabs.style.margin = '12px 0';
    var box = el('div', '');

    function draw() {
      box.innerHTML = '';
      Array.prototype.slice.call(tabs.children).forEach(function (b) {
        b.className = b.textContent.toLowerCase() === mode.v ? 'sel' : '';
      });
      if (mode.v === 'cloze') {
        var lab = el('label', '', 'Select words in the box, then tap Blank it');
        box.appendChild(lab);
        var ta = document.createElement('textarea');
        ta.rows = 5;
        ta.value = it.cloze || it.text;
        ta.id = 'clozebox';
        box.appendChild(ta);
        var b = el('button', '', 'Blank it');
        b.style.marginTop = '10px';
        b.onclick = function () {
          var s = ta.selectionStart, e = ta.selectionEnd;
          if (s === e) return toast('Select the words to hide');
          var n = (ta.value.match(/\{\{c(\d+)::/g) || []).length + 1;
          ta.value = ta.value.slice(0, s) + '{{c' + n + '::' + ta.value.slice(s, e) + '}}' + ta.value.slice(e);
        };
        box.appendChild(b);
      } else {
        box.appendChild(el('label', '', 'Front'));
        var f = document.createElement('textarea');
        f.rows = 2; f.id = 'frontbox'; f.value = it.front || '';
        box.appendChild(f);
        var l2 = el('label', '', 'Back');
        l2.style.marginTop = '10px';
        box.appendChild(l2);
        var bk = document.createElement('textarea');
        bk.rows = 3; bk.id = 'backbox'; bk.value = it.back || it.text;
        box.appendChild(bk);
      }
    }

    ['Cloze', 'Basic'].forEach(function (name) {
      var b = el('button', '', name);
      b.onclick = function () { mode.v = name.toLowerCase(); draw(); };
      tabs.appendChild(b);
    });
    w.appendChild(tabs);
    w.appendChild(box);
    draw();

    var row = el('div', 'chips');
    row.style.marginTop = '14px';
    var cancel = el('button', '', 'Cancel');
    cancel.onclick = closeSheet;
    var ok = el('button', 'solid', 'Save card');
    ok.onclick = function () {
      if (mode.v === 'cloze') {
        var v = $('clozebox').value.trim();
        if (!/\{\{c\d+::/.test(v)) return toast('Blank out at least one thing');
        it.cloze = v;
      } else {
        var f = $('frontbox').value.trim(), b = $('backbox').value.trim();
        if (!f || !b) return toast('Both sides needed');
        it.front = f; it.back = b;
      }
      it.cardType = mode.v;
      it.state = 'card';
      save(it);
      closeSheet();
      toast('Card ready to export');
      nextInQueue();
    };
    row.appendChild(cancel);
    row.appendChild(ok);
    w.appendChild(row);
  });
}

/* ---------- export ---------- */
function download(name, blob) {
  var url = URL.createObjectURL(blob);
  var a = document.createElement('a');
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  setTimeout(function () { URL.revokeObjectURL(url); a.remove(); }, 2000);
}
function csvCell(s) { return '"' + String(s == null ? '' : s).replace(/"/g, '""') + '"'; }
function stamp() { return new Date().toISOString().slice(0, 10); }

function cardsToExport(type) {
  return S.items.filter(function (i) { return i.state === 'card' && i.cardType === type; });
}

function exportCards(type) {
  var rows = cardsToExport(type);
  if (!rows.length) return toast('No ' + type + ' cards waiting');
  var lines = rows.map(function (i) {
    var s = srcOf(i);
    var tag = s ? slug(s.title) : 'reader';
    if (type === 'cloze') return [csvCell(i.cloze), csvCell(s ? s.title : ''), csvCell(tag)].join(',');
    return [csvCell(i.front), csvCell(i.back), csvCell(tag)].join(',');
  });
  download('anki-' + type + '-' + stamp() + '.csv',
    new Blob([lines.join('\n')], { type: 'text/csv' }));
  rows.forEach(function (i) { i.state = 'exported'; i.exportedAt = Date.now(); save(i); });
  toast(plural(rows.length, 'card') + ' exported');
}

function exportMarkdown() {
  var notes = S.items.filter(function (i) { return i.type === 'note'; });
  if (!notes.length) return toast('No notes yet');
  var zip = new JSZip();
  notes.forEach(function (n) {
    var s = srcOf(n);
    var parent = n.parentId ? byId(n.parentId) : null;
    var book = s ? s.title : 'Unsourced';
    var fm = [
      '---',
      'source: "' + book.replace(/"/g, "'") + '"',
      'author: "' + ((s && s.author) || '').replace(/"/g, "'") + '"',
      'block: ' + (n.blockIdx == null ? 'null' : n.blockIdx),
      'created: ' + new Date(n.createdAt).toISOString().slice(0, 10),
      'priority: ' + Math.round(n.priority),
      'state: ' + n.state,
      'tags: [reader, ' + slug(book) + ']',
      '---',
      ''
    ].join('\n');
    var body = n.text + '\n';
    if (parent) body += '\n> ' + parent.text + '\n\n*from* [[' + book + ']]\n';
    zip.folder('notes/' + slug(book)).file(slug(n.text.slice(0, 40)) + '-' + n.id + '.md', fm + body);
  });
  S.sources.forEach(function (s) {
    var ex = S.items.filter(function (i) { return i.sourceId === s.id && i.type === 'extract'; });
    if (!ex.length) return;
    var lines = ['# ' + s.title, '', s.author ? '*' + s.author + '*' : '', ''];
    ex.sort(function (a, b) { return (a.blockIdx || 0) - (b.blockIdx || 0); }).forEach(function (i) {
      lines.push('> ' + i.text);
      lines.push('');
      lines.push('`block ' + i.blockIdx + ' · priority ' + Math.round(i.priority) + ' · ' + i.state + '`');
      notesOf(i.id).forEach(function (n) { lines.push('', n.text); });
      lines.push('', '---', '');
    });
    zip.folder('extracts').file(slug(s.title) + '.md', lines.join('\n'));
  });
  zip.generateAsync({ type: 'blob' }).then(function (b) {
    download('reader-notes-' + stamp() + '.zip', b);
    toast(plural(notes.length, 'note') + ' exported');
  });
}

function snapshot() {
  return JSON.stringify({ v: 1, at: Date.now(), sources: S.sources, items: S.items, cfg: S.cfg });
}

function backupJson() {
  download('reader-backup-' + stamp() + '.json', new Blob([snapshot()], { type: 'application/json' }));
  S.cfg.lastBackup = Date.now();
  saveCfg();
  toast('Backup saved to Downloads');
}

function restoreJson(file) {
  file.text().then(function (t) {
    var d = JSON.parse(t);
    if (!d.sources || !d.items) throw new Error('bad file');
    var tx = DB.transaction(['sources', 'items'], 'readwrite');
    tx.objectStore('sources').clear();
    tx.objectStore('items').clear();
    d.sources.forEach(function (s) { tx.objectStore('sources').put(s); });
    d.items.forEach(function (i) { tx.objectStore('items').put(i); });
    tx.oncomplete = function () {
      S.sources = d.sources; S.items = d.items;
      closeSheet(); home();
      toast('Restored ' + plural(d.sources.length, 'book'));
    };
  }).catch(function () { toast('That is not a valid backup file'); });
}

function githubBackup() {
  var c = S.cfg;
  if (!c.ghToken || !c.ghRepo) return toast('Add your token in Settings first');
  var path = 'reader-backup.json';
  var api = 'https://api.github.com/repos/' + c.ghRepo + '/contents/' + path;
  var head = {
    Authorization: 'Bearer ' + c.ghToken,
    Accept: 'application/vnd.github+json',
    'Content-Type': 'application/json'
  };
  toast('Pushing to GitHub…');
  fetch(api, { headers: head }).then(function (r) {
    return r.ok ? r.json() : null;
  }).then(function (cur) {
    var body = {
      message: 'reader backup ' + new Date().toISOString(),
      content: btoa(unescape(encodeURIComponent(snapshot())))
    };
    if (cur && cur.sha) body.sha = cur.sha;
    return fetch(api, { method: 'PUT', headers: head, body: JSON.stringify(body) });
  }).then(function (r) {
    if (!r.ok) throw new Error('http ' + r.status);
    S.cfg.lastBackup = Date.now();
    saveCfg();
    toast('Backed up to GitHub');
  }).catch(function (e) {
    toast('GitHub backup failed: ' + e.message);
  });
}

function exportSheet() {
  sheet(function (w) {
    w.appendChild(el('h2', '', 'Export'));
    var nc = cardsToExport('cloze').length, nb = cardsToExport('basic').length;
    var notes = S.items.filter(function (i) { return i.type === 'note'; }).length;
    var p = el('p', 'ghost', 'CSV files import straight into AnkiDroid: Import, pick the file, choose the notetype.');
    p.style.margin = '6px 0 14px';
    w.appendChild(p);

    function opt(label, sub, fn) {
      var b = el('button', 'wide', label);
      b.style.marginBottom = '4px';
      b.onclick = fn;
      w.appendChild(b);
      var s = el('p', 'ghost', sub);
      s.style.cssText = 'text-align:center;margin-bottom:14px';
      w.appendChild(s);
    }
    opt('Cloze cards → CSV', nc ? plural(nc, 'card') + ' waiting' : 'none waiting', function () { exportCards('cloze'); closeSheet(); });
    opt('Basic cards → CSV', nb ? plural(nb, 'card') + ' waiting' : 'none waiting', function () { exportCards('basic'); closeSheet(); });
    opt('Notes → Obsidian zip', plural(notes, 'note') + ', markdown with frontmatter', function () { exportMarkdown(); closeSheet(); });
    opt('Full backup → JSON', S.cfg.lastBackup ? 'last backup ' + plural(days(Date.now() - S.cfg.lastBackup), 'day') + ' ago' : 'never backed up', function () { backupJson(); closeSheet(); });
    if (S.cfg.ghToken) opt('Backup → GitHub', S.cfg.ghRepo, function () { githubBackup(); closeSheet(); });

    var rin = document.createElement('input');
    rin.type = 'file';
    rin.accept = '.json';
    rin.style.display = 'none';
    rin.onchange = function () { if (rin.files[0]) restoreJson(rin.files[0]); };
    w.appendChild(rin);
    var rb = el('button', 'plain wide', 'Restore from backup');
    rb.onclick = function () { rin.click(); };
    w.appendChild(rb);
  });
}

/* ---------- reading comfort ---------- */
var THEMES = {
  paper: { paper: '#FAF7F0', ink: '#2C2C2A', soft: '#5F5E5A', faint: '#888780', ghost: '#B4B2A9', rule: '#EDE7DA' },
  sepia: { paper: '#F2E6D0', ink: '#3B2F1E', soft: '#6B5B44', faint: '#8C7B62', ghost: '#B6A68C', rule: '#E2D2B6' },
  dark: { paper: '#1B1A17', ink: '#E9E3D6', soft: '#B6AF9F', faint: '#8C8677', ghost: '#645F54', rule: '#2E2C27' }
};
var FONTS = {
  serif: "Georgia,'Iowan Old Style','Palatino Linotype',serif",
  sans: "-apple-system,'Segoe UI',Roboto,sans-serif",
  mono: "ui-monospace,'SF Mono',Menlo,monospace"
};

function applyTheme() {
  var c = S.cfg;
  var t = THEMES[c.theme || 'paper'] || THEMES.paper;
  var r = document.documentElement.style;
  r.setProperty('--paper', t.paper);
  r.setProperty('--ink', t.ink);
  r.setProperty('--ink-soft', t.soft);
  r.setProperty('--ink-faint', t.faint);
  r.setProperty('--ink-ghost', t.ghost);
  r.setProperty('--rule', t.rule);
  r.setProperty('--serif', FONTS[c.font || 'serif'] || FONTS.serif);
  r.setProperty('--read-size', (c.fontSize || 17) + 'px');
  r.setProperty('--read-leading', String(c.leading || 1.75));
  var m = document.querySelector('meta[name=theme-color]');
  if (m) m.setAttribute('content', t.paper);
}

function readingSheet() {
  sheet(function (w) {
    w.appendChild(el('h2', '', 'Reading'));
    function group(label, opts, key, dflt, fmt) {
      var l = el('label', '', label);
      l.style.marginTop = '16px';
      w.appendChild(l);
      var row = el('div', 'chips');
      opts.forEach(function (o) {
        var b = el('button', (S.cfg[key] || dflt) === o ? 'sel' : '', fmt ? fmt(o) : o);
        b.onclick = function () {
          S.cfg[key] = o;
          saveCfg();
          applyTheme();
          Array.prototype.slice.call(row.children).forEach(function (x, i) {
            x.className = opts[i] === o ? 'sel' : '';
          });
        };
        row.appendChild(b);
      });
      w.appendChild(row);
    }
    group('Theme', ['paper', 'sepia', 'dark'], 'theme', 'paper');
    group('Typeface', ['serif', 'sans', 'mono'], 'font', 'serif');
    group('Size', [15, 17, 19, 21], 'fontSize', 17, function (n) { return n + 'px'; });
    group('Line height', [1.5, 1.75, 2], 'leading', 1.75, function (n) { return String(n); });

    var prev = el('p', '');
    prev.style.cssText = 'font-family:var(--serif);font-size:var(--read-size);line-height:var(--read-leading);margin:20px 0 0;color:var(--ink)';
    prev.textContent = 'Nothing in life is as important as you think it is while you are thinking about it.';
    w.appendChild(prev);

    var done = el('button', 'solid wide', 'Done');
    done.style.marginTop = '18px';
    done.onclick = closeSheet;
    w.appendChild(done);
  });
}

function settingsSheet() {
  sheet(function (w) {
    w.appendChild(el('h2', '', 'Settings'));
    var wrap = el('div', '');
    wrap.style.marginTop = '14px';

    wrap.appendChild(el('label', '', 'Default priority for new extracts'));
    var dp = el('div', 'chips');
    var dpv = { v: S.cfg.defaultPriority || 4 };
    [1, 2, 3, 4, 5].forEach(function (p) {
      var b = el('button', dpv.v === p ? 'sel' : '', String(p));
      b.onclick = function () {
        dpv.v = p;
        Array.prototype.slice.call(dp.children).forEach(function (x, i) {
          x.className = (i + 1) === p ? 'sel' : '';
        });
      };
      dp.appendChild(b);
    });
    wrap.appendChild(dp);
    var dph = el('p', 'ghost', '4 leaves room to demote to 5. If everything defaults to the bottom, priority stops telling the queue anything.');
    dph.style.margin = '6px 0 16px';
    wrap.appendChild(dph);

    wrap.appendChild(el('label', '', 'Queue overload limit'));
    var ov = document.createElement('input');
    ov.type = 'number'; ov.value = S.cfg.overload || 40; ov.min = 10; ov.max = 200;
    wrap.appendChild(ov);
    var ovh = el('p', 'ghost', 'When more than this is due, the lowest priority items get spread over the coming days.');
    ovh.style.margin = '6px 0 16px';
    wrap.appendChild(ovh);

    wrap.appendChild(el('label', '', 'GitHub repo (owner/name, private)'));
    var repo = document.createElement('input');
    repo.value = S.cfg.ghRepo || '';
    repo.placeholder = 'yourname/reader-backup';
    wrap.appendChild(repo);
    var l2 = el('label', '', 'Fine-grained token with Contents: read and write');
    l2.style.marginTop = '12px';
    wrap.appendChild(l2);
    var tok = document.createElement('input');
    tok.type = 'password';
    tok.value = S.cfg.ghToken || '';
    tok.placeholder = 'github_pat_…';
    wrap.appendChild(tok);
    var th = el('p', 'ghost', 'Stored on this device only. Optional.');
    th.style.margin = '6px 0 16px';
    wrap.appendChild(th);
    w.appendChild(wrap);

    if (navigator.storage && navigator.storage.estimate) {
      navigator.storage.estimate().then(function (e) {
        var mb = (e.usage / 1048576).toFixed(1);
        var s = el('p', 'ghost', 'Using ' + mb + ' MB. Storage is ' +
          (S.cfg.persisted ? 'persistent.' : 'not marked persistent.'));
        s.style.marginBottom = '14px';
        w.insertBefore(s, w.lastChild);
      });
    }

    var row = el('div', 'chips');
    var cancel = el('button', '', 'Cancel');
    cancel.onclick = closeSheet;
    var ok = el('button', 'solid', 'Save');
    ok.onclick = function () {
      S.cfg.defaultPriority = dpv.v;
      S.cfg.overload = Math.max(10, Math.min(200, +ov.value || 40));
      S.cfg.ghRepo = repo.value.trim();
      S.cfg.ghToken = tok.value.trim();
      saveCfg();
      closeSheet();
      toast('Saved');
    };
    row.appendChild(cancel);
    row.appendChild(ok);
    w.appendChild(row);
  });
}

/* ---------- browse extracts ---------- */
var BR = { q: '', filter: 'open', book: 'all', sort: 'rank', sel: {}, mode: false };

function selCount() { var n = 0; for (var k in BR.sel) if (BR.sel[k]) n++; return n; }
function clearSel() { BR.sel = {}; BR.mode = false; }

function stats() {
  var now = Date.now();
  var st = { due: 0, open: 0, note: 0, card: 0, kept: 0, oldest: 0 };
  S.items.forEach(function (i) {
    if (i.type === 'note') st.note++;
    if (i.state === 'open' && i.type === 'extract') {
      st.open++;
      if (i.dueAt <= now) st.due++;
      st.oldest = Math.max(st.oldest, now - i.createdAt);
    } else if (i.state === 'open' && i.dueAt <= now) st.due++;
    if (i.state === 'card') st.card++;
    if (i.state === 'kept') st.kept++;
  });
  return st;
}

function browseScreen() {
  show('s-list');
  $('l-title').textContent = 'Extracts';
  var b = $('l-body');
  b.innerHTML = '';

  var st = stats();
  var strip = el('div', 'stats');
  [['due', st.due, 'due', 'open'], ['open', st.open, 'open', 'open'],
   ['note', st.note, 'notes', 'note'], ['card', st.card, 'to export', 'card'],
   ['kept', st.kept, 'retired', 'kept']].forEach(function (s) {
    var cell = el('div', 'stat' + (BR.filter === s[3] ? ' on' : ''));
    cell.appendChild(el('span', 'n', String(s[1])));
    cell.appendChild(el('span', 'k', s[2]));
    cell.onclick = function () { BR.filter = s[3]; clearSel(); browseScreen(); };
    strip.appendChild(cell);
  });
  b.appendChild(strip);
  if (st.oldest > 3 * DAY) {
    var age = el('p', 'ghost', 'Oldest open extract is ' + plural(days(st.oldest), 'day') + ' old.');
    age.style.margin = '8px 0 0';
    b.appendChild(age);
  }

  var search = document.createElement('input');
  search.placeholder = 'Search extracts and notes';
  search.value = BR.q;
  search.style.marginTop = '14px';
  search.oninput = function () { BR.q = this.value; drawBrowse(); };
  b.appendChild(search);

  var ctl = el('div', 'row');
  ctl.style.marginTop = '10px';

  var sortSel = document.createElement('select');
  [['rank', 'By priority'], ['new', 'Newest first'], ['old', 'Oldest first'],
   ['due', 'Due soonest'], ['long', 'Longest first']].forEach(function (o) {
    var op = document.createElement('option');
    op.value = o[0]; op.textContent = o[1];
    if (BR.sort === o[0]) op.selected = true;
    sortSel.appendChild(op);
  });
  sortSel.onchange = function () { BR.sort = this.value; drawBrowse(); };
  ctl.appendChild(sortSel);

  if (S.sources.length > 1) {
    var bookSel = document.createElement('select');
    var o0 = document.createElement('option');
    o0.value = 'all'; o0.textContent = 'All books';
    bookSel.appendChild(o0);
    S.sources.forEach(function (s) {
      var o = document.createElement('option');
      o.value = s.id;
      o.textContent = s.title.length > 24 ? s.title.slice(0, 24) + '…' : s.title;
      if (BR.book === s.id) o.selected = true;
      bookSel.appendChild(o);
    });
    bookSel.onchange = function () { BR.book = this.value; drawBrowse(); };
    ctl.appendChild(bookSel);
  }
  b.appendChild(ctl);

  var selrow = el('div', 'row');
  selrow.style.margin = '12px 0 0';
  var cnt = el('span', 'ghost', '');
  cnt.id = 'browsecount';
  selrow.appendChild(cnt);
  var selbtn = el('button', 'plain', BR.mode ? 'Cancel' : 'Select');
  selbtn.onclick = function () {
    if (BR.mode) clearSel(); else BR.mode = true;
    browseScreen();
  };
  selrow.appendChild(selbtn);
  b.appendChild(selrow);

  var list = el('div', '');
  list.id = 'browselist';
  list.style.marginTop = '10px';
  b.appendChild(list);

  var bar = el('div', 'bulkbar');
  bar.id = 'bulkbar';
  b.appendChild(bar);

  drawBrowse();
}

function browseMatches() {
  var q = BR.q.trim().toLowerCase();
  var rows = S.items.filter(function (i) {
    if (BR.book !== 'all' && i.sourceId !== BR.book) return false;
    if (BR.filter === 'open' && !(i.state === 'open' && i.type === 'extract')) return false;
    if (BR.filter === 'note' && i.type !== 'note') return false;
    if (BR.filter === 'card' && i.state !== 'card' && i.state !== 'exported') return false;
    if (BR.filter === 'kept' && i.state !== 'kept') return false;
    if (q && i.text.toLowerCase().indexOf(q) === -1) return false;
    return true;
  });
  var by = {
    rank: function (a, b) { return (a.rank || 0.5) - (b.rank || 0.5); },
    new: function (a, b) { return b.createdAt - a.createdAt; },
    old: function (a, b) { return a.createdAt - b.createdAt; },
    due: function (a, b) { return a.dueAt - b.dueAt; },
    long: function (a, b) { return b.text.length - a.text.length; }
  };
  return rows.sort(by[BR.sort] || by.rank);
}

function drawBrowse() {
  var list = $('browselist');
  if (!list) return;
  list.innerHTML = '';
  var rows = browseMatches();
  var n = selCount();
  $('browsecount').textContent = BR.mode && n
    ? plural(n, 'item') + ' selected'
    : plural(rows.length, 'item');
  drawBulkBar(rows);

  if (!rows.length) {
    list.appendChild(el('p', 'empty', BR.q ? 'Nothing matches.' : 'Nothing here yet.'));
    return;
  }

  rows.slice(0, 300).forEach(function (i) {
    var s = srcOf(i);
    var c = el('div', 'card' + (BR.sel[i.id] ? ' picked' : ''));
    c.style.cursor = 'pointer';
    var top = el('div', 'row');
    var left = el('span', '');
    left.style.cssText = 'display:flex;gap:8px;align-items:center';
    if (BR.mode) {
      var box = el('span', 'box' + (BR.sel[i.id] ? ' on' : ''), BR.sel[i.id] ? '✓' : '');
      left.appendChild(box);
    }
    left.appendChild(el('span', 'pill ' + (i.type === 'note' ? 'cool' : ''), i.type));
    top.appendChild(left);
    var when = i.dueAt > Date.now()
      ? 'in ' + plural(Math.max(1, days(i.dueAt - Date.now())), 'day')
      : 'due';
    top.appendChild(el('span', 'ghost',
      (i.state === 'open' ? pctLabel(i) : 'p' + Math.round(i.priority))
      + ' · ' + (i.state === 'open' ? when : i.state)));
    c.appendChild(top);
    var t = el('p', 'serif');
    t.style.cssText = 'font-size:15px;line-height:1.6;margin:8px 0 6px';
    t.textContent = i.text.length > 220 ? i.text.slice(0, 220) + '…' : i.text;
    c.appendChild(t);
    var nn = notesOf(i.id).length;
    c.appendChild(el('span', 'ghost',
      (s ? s.title : 'no source') + (nn ? ' · ' + plural(nn, 'note') : '')));
    c.onclick = function () {
      if (BR.mode) {
        BR.sel[i.id] = !BR.sel[i.id];
        drawBrowse();
      } else {
        S.back = browseScreen;
        openItem(i);
      }
    };
    /* long-press anywhere gets you into selection mode without hunting for a button */
    var timer;
    c.addEventListener('touchstart', function () {
      timer = setTimeout(function () {
        if (!BR.mode) {
          BR.mode = true;
          BR.sel[i.id] = true;
          if (navigator.vibrate) navigator.vibrate(12);
          browseScreen();
        }
      }, 450);
    }, { passive: true });
    ['touchend', 'touchmove', 'touchcancel'].forEach(function (e) {
      c.addEventListener(e, function () { clearTimeout(timer); }, { passive: true });
    });
    list.appendChild(c);
  });
  if (rows.length > 300) list.appendChild(el('p', 'ghost', 'Showing the first 300.'));
}

function selectedItems() {
  return S.items.filter(function (i) { return BR.sel[i.id]; });
}

function drawBulkBar(rows) {
  var bar = $('bulkbar');
  if (!bar) return;
  bar.innerHTML = '';
  var n = selCount();
  bar.classList.toggle('on', BR.mode);
  if (!BR.mode) return;

  var top = el('div', 'row');
  var all = el('button', 'plain', n === rows.length ? 'Select none' : 'Select all ' + rows.length);
  all.onclick = function () {
    if (n === rows.length) BR.sel = {};
    else rows.forEach(function (i) { BR.sel[i.id] = true; });
    drawBrowse();
  };
  top.appendChild(all);
  top.appendChild(el('span', 'ghost', n ? plural(n, 'item') : 'nothing selected'));
  bar.appendChild(top);

  var pri = el('div', 'chips');
  pri.style.marginTop = '8px';
  [1, 2, 3, 4, 5].forEach(function (p) {
    var b = el('button', '', String(p));
    b.disabled = !n;
    b.onclick = function () {
      /* reverse so the first item you picked ends up highest inside the band */
      selectedItems().reverse().forEach(function (i) { place(i, p); save(i); });
      toast(plural(n, 'item') + ' moved to ' + (p === 1 ? 'the top' : 'priority ' + p));
      clearSel();
      browseScreen();
    };
    pri.appendChild(b);
  });
  bar.appendChild(pri);

  var acts = el('div', 'chips');
  acts.style.marginTop = '6px';
  function act(label, fn, danger) {
    var b = el('button', '', label);
    b.disabled = !n;
    if (danger) b.style.color = 'var(--red)';
    b.onclick = fn;
    acts.appendChild(b);
  }
  act('Later', function () {
    selectedItems().forEach(function (i) { bump(i); save(i); });
    toast(plural(n, 'item') + ' pushed back');
    clearSel(); browseScreen();
  });
  act('Much later', function () {
    selectedItems().forEach(function (i) { bump(i, 2); save(i); });
    toast(plural(n, 'item') + ' pushed back hard');
    clearSel(); browseScreen();
  });
  act('Retire', function () {
    selectedItems().forEach(function (i) { i.state = 'kept'; save(i); });
    toast(plural(n, 'item') + ' retired');
    clearSel(); browseScreen();
  });
  act('Delete', function () {
    if (!confirm('Delete ' + plural(n, 'item') + '? This cannot be undone.')) return;
    selectedItems().forEach(removeItem);
    toast(plural(n, 'item') + ' deleted');
    clearSel(); browseScreen();
  }, true);
  bar.appendChild(acts);
}

/* ---------- archive / list ---------- */
function archiveScreen() {
  S.back = null;
  show('s-list');
  $('l-title').textContent = 'All books';
  var b = $('l-body');
  b.innerHTML = '';
  if (!S.sources.length) { b.appendChild(el('p', 'empty', 'Nothing here yet.')); return; }
  S.sources.forEach(function (s) {
    var c = el('div', 'card');
    var r = el('div', 'row');
    r.appendChild(el('span', 'serif', s.title));
    r.appendChild(el('span', 'ghost', s.archived ? 'archived' : 'active'));
    c.appendChild(r);
    var pct = s.blocks.length ? Math.round(s.position / s.blocks.length * 100) : 0;
    var n = S.items.filter(function (i) { return i.sourceId === s.id; }).length;
    var m = el('p', 'ghost', (s.author ? s.author + ' · ' : '') + pct + '% · ' + plural(n, 'item'));
    m.style.margin = '6px 0 10px';
    c.appendChild(m);
    var row = el('div', 'chips');
    var t = el('button', '', s.archived ? 'Reactivate' : 'Archive');
    t.onclick = function () {
      var active = S.sources.filter(function (x) { return !x.archived; }).length;
      if (s.archived && active >= SHELF_MAX) return toast('Shelf is full. Archive one first.');
      s.archived = !s.archived;
      save(s);
      archiveScreen();
    };
    var o = el('button', '', 'Open');
    o.onclick = function () { openReader(s); };
    var d = el('button', '', 'Delete');
    d.style.color = 'var(--red)';
    d.onclick = function () {
      if (!confirm('Delete "' + s.title + '" and everything from it?')) return;
      S.items.filter(function (i) { return i.sourceId === s.id; }).forEach(removeItem);
      S.sources = S.sources.filter(function (x) { return x.id !== s.id; });
      del('sources', s.id);
      archiveScreen();
    };
    row.appendChild(o);
    row.appendChild(t);
    row.appendChild(d);
    c.appendChild(row);
    b.appendChild(c);
  });
}

/* ---------- wiring ---------- */
$('btn-add').onclick = function () {
  var active = S.sources.filter(function (s) { return !s.archived; }).length;
  if (active >= SHELF_MAX) return toast('Shelf is full at ' + SHELF_MAX + '. Archive one first.');
  $('file').click();
};
$('file').onchange = function () {
  if (this.files[0]) addSource(this.files[0]);
  this.value = '';
};
$('btn-export').onclick = exportSheet;
$('btn-settings').onclick = settingsSheet;
$('btn-archive').onclick = archiveScreen;
$('btn-browse').onclick = function () { BR.q = ''; clearSel(); browseScreen(); };
$('btn-reading').onclick = readingSheet;
$('r-font').onclick = readingSheet;
$('l-back').onclick = home;
$('r-done').onclick = function () { hidePriBar(); endSession(); };

$('r-sel').onclick = function () {
  var r = selRich($('page'));
  if (!r || !r.text) return;
  var sel = window.getSelection();
  var node = sel.anchorNode;
  while (node && !(node.classList && node.classList.contains('para'))) node = node.parentElement;
  var idx = node ? +node.dataset.i : null;
  var item = makeItem('extract', r.text, S.read.src, null, idx, r.html);
  sel.removeAllRanges();
  $('r-sel').classList.remove('on');
  refreshMarks();
  if (navigator.vibrate) navigator.vibrate(8);
  showPriBar(item);
};
$('r-toc').onclick = tocSheet;

document.addEventListener('selectionchange', function () {
  if (S.view !== 's-read') return;
  var t = selText($('page'));
  $('r-sel').classList.toggle('on', !!t && t.length > 3);
});

window.addEventListener('scroll', function () {
  if (S.view !== 's-read' || !S.read) return;
  updateReadHead();
  var bottom = document.body.scrollHeight - window.scrollY - window.innerHeight;
  if (bottom < 600 && S.read.end < S.read.src.blocks.length) renderChunk(S.read.end);
  if (window.scrollY < 400 && S.read.start > 0) prependChunk();
}, { passive: true });

window.addEventListener('beforeunload', function () {
  if (S.view === 's-read' && S.read && !S.read.revisit) {
    var s = S.read.src;
    s.position = Math.max(s.position, firstVisible());
    save(s);
  }
});
document.addEventListener('visibilitychange', function () {
  if (document.visibilityState === 'hidden' && S.view === 's-read' && S.read && !S.read.revisit) {
    var s = S.read.src;
    s.position = Math.max(s.position, firstVisible());
    save(s);
  }
});

/* ---------- boot ---------- */
open().then(function () {
  return Promise.all([all('sources'), all('items'), all('cfg')]);
}).then(function (r) {
  S.sources = r[0].map(function (s) { s.kind = 'source'; return s; });
  S.items = r[1].map(function (i) { i.kind = 'item'; return i; });
  var c = r[2].filter(function (x) { return x.k === 'cfg'; })[0];
  S.cfg = c ? c.v : { overload: 40 };
  applyTheme();
  migrateRanks();
  var moved = autoPostpone();
  home();
  if (moved) toast(plural(moved, 'item') + ' postponed to keep today manageable');
  if (navigator.storage && navigator.storage.persist) {
    navigator.storage.persist().then(function (ok) { S.cfg.persisted = ok; saveCfg(); });
  }
  if (S.cfg.lastBackup && days(Date.now() - S.cfg.lastBackup) > 7) {
    setTimeout(function () { toast('It has been a while since your last backup'); }, 3000);
  }
}).catch(function (e) {
  document.body.innerHTML = '<div class="pad"><h1>Could not start</h1><p class="muted">' +
    esc(e.message || e) + '</p></div>';
});

if ('serviceWorker' in navigator) {
  window.addEventListener('load', function () {
    navigator.serviceWorker.register('sw.js').catch(function () {});
  });
}
