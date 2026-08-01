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
  out.sort(function (a, b) {
    var d = (a.priority + jitter(a.id)) - (b.priority + jitter(b.id));
    if (Math.abs(d) > 0.001) return d;
    return a.dueAt - b.dueAt;
  });
  return out;
}

/* When the due pile outgrows what you can clear, spread it out.
   Lowest priority gets pushed hardest, and drifts down a notch. */
function autoPostpone() {
  var limit = S.cfg.overload || 40;
  var due = queue();
  if (due.length <= limit) return 0;
  var excess = due.slice(limit);
  var dirty = [];
  excess.forEach(function (o, i) {
    o.dueAt = Date.now() + (1 + Math.floor(i / 12)) * DAY;
    o.priority = clampP(o.priority + 0.1);
    dirty.push(o);
  });
  dirty.forEach(function (o) { put(o.kind === 'source' ? 'sources' : 'items', o); });
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
    out.push({ t: /^h[1-6]$/.test(tag) ? 'h' : (tag === 'blockquote' ? 'q' : 'p'), x: t });
  });
  return out;
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
        var chain = Promise.resolve();
        order.forEach(function (p) {
          chain = chain.then(function () {
            var f = zip.file(p);
            if (!f) return;
            return f.async('string').then(function (h) {
              blocksFromHtml(h).forEach(function (b) { blocks.push(b); });
            }).catch(function () {});
          });
        });
        return chain.then(function () {
          if (!blocks.length) throw new Error('No readable text found in that EPUB');
          return { title: title, author: author, blocks: blocks };
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
      position: 0, priority: 3, interval: 0, dueAt: Date.now(), reps: 0,
      archived: false, addedAt: Date.now(), finishedAt: null
    };
    S.sources.push(s);
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

function makeItem(type, text, source, parentId, blockIdx) {
  var it = {
    id: uid(), kind: 'item', type: type, sourceId: source ? source.id : null,
    parentId: parentId || null, text: text, history: [],
    blockIdx: blockIdx == null ? null : blockIdx,
    priority: source ? source.priority : 3,
    interval: type === 'note' ? 2 : 0, reps: 0,
    dueAt: Date.now() + (type === 'note' ? 2 : 1) * DAY,
    timesShortened: 0, state: 'open', cardType: null, cloze: '', front: '', back: '',
    createdAt: Date.now()
  };
  S.items.push(it);
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
  var q = queue();
  var d = new Date();
  $('today').textContent = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][d.getDay()];
  $('duecount').textContent = q.length ? q.length + ' due' : 'all clear';

  var nc = $('nextcard');
  nc.innerHTML = '';
  if (!q.length) {
    nc.style.background = 'transparent';
    nc.style.padding = '0 0 8px';
    var e = el('p', 'empty', S.sources.length ? 'Nothing due. Pick a book below.' : 'Add a book to begin.');
    nc.appendChild(e);
  } else {
    nc.style.background = '';
    nc.style.padding = '';
    var top = q[0];
    var head = el('div', 'row');
    head.appendChild(el('span', 'eyebrow', 'Up next'));
    var kindLabel = top.kind === 'source' ? (isRevisit(top) ? 'revisit' : 'read') : top.type;
    head.appendChild(el('span', 'meta', kindLabel + ' · p' + Math.round(top.priority)));
    nc.appendChild(head);
    var body = el('p', 'body', top.kind === 'source' ? top.title : top.text.slice(0, 150) + (top.text.length > 150 ? '…' : ''));
    nc.appendChild(body);
    var b = el('button', '', 'Start queue');
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

function renderChunk(from) {
  var s = S.read.src;
  var end = Math.min(s.blocks.length, from + CHUNK);
  var page = $('page');
  for (var i = from; i < end; i++) {
    var b = s.blocks[i];
    var p = el('p', 'para' + (b.t === 'h' ? ' h' : b.t === 'q' ? ' q' : ''), b.x);
    p.dataset.i = i;
    p.onclick = onTapPara;
    page.appendChild(p);
  }
  S.read.end = end;
  if (end >= s.blocks.length) {
    var fin = el('p', 'empty', 'End of book.');
    fin.id = 'fin';
    if (!$('fin')) page.appendChild(fin);
  }
}

function onTapPara(e) {
  var p = e.currentTarget;
  var i = +p.dataset.i;
  var s = S.read.src;
  if (S.read.taken[i]) {
    removeItem(S.read.taken[i]);
    delete S.read.taken[i];
    p.classList.remove('taken');
  } else {
    S.read.taken[i] = makeItem('extract', s.blocks[i].x, s, null, i);
    p.classList.add('taken');
    if (navigator.vibrate) navigator.vibrate(8);
  }
  countHint();
}

function countHint() {
  var n = Object.keys(S.read.taken).length;
  var h = $('r-hint');
  h.textContent = n === 0 ? 'Tap a paragraph to extract it' : plural(n, 'extract') + ' saved';
  h.style.color = n === 0 ? '' : 'var(--green)';
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
      s.priority = clampP(s.priority + 1);
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
  var close = el('button', 'plain', 'Close');
  close.onclick = home;
  head.appendChild(close);
  root.appendChild(head);

  var body = el('p', 'serif');
  body.style.cssText = 'font-size:18px;line-height:1.7;margin:18px 0 10px';
  body.textContent = it.text;
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
    var t = selText(body);
    if (!t) return toast('Select some text first');
    it.history.push(it.text);
    it.text = t;
    it.timesShortened++;
    save(it);
    drawItem();
    toast('Shortened');
  };
  var split = el('button', '', 'Split off');
  split.onclick = function () {
    var t = selText(body);
    if (!t) return toast('Select some text first');
    var rest = it.text.replace(t, '').replace(/\s+/g, ' ').trim();
    if (!rest) return toast('That is the whole thing, use Keep selection');
    var child = makeItem('extract', t, src, it.id, it.blockIdx);
    child.priority = it.priority;
    save(child);
    it.history.push(it.text);
    it.text = rest;
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
    b.onclick = function () { it.priority = p; save(it); drawItem(); };
    chips.appendChild(b);
  });
  pr.appendChild(chips);
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

function settingsSheet() {
  sheet(function (w) {
    w.appendChild(el('h2', '', 'Settings'));
    var wrap = el('div', '');
    wrap.style.marginTop = '14px';

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

/* ---------- archive / list ---------- */
function archiveScreen() {
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
$('l-back').onclick = home;
$('r-done').onclick = endSession;

$('r-sel').onclick = function () {
  var t = selText($('page'));
  if (!t) return;
  var sel = window.getSelection();
  var node = sel.anchorNode;
  while (node && !(node.classList && node.classList.contains('para'))) node = node.parentElement;
  var idx = node ? +node.dataset.i : null;
  makeItem('extract', t, S.read.src, null, idx);
  sel.removeAllRanges();
  $('r-sel').classList.remove('on');
  S.read.taken['sel' + uid()] = true;
  countHint();
  if (navigator.vibrate) navigator.vibrate(8);
  toast('Selection extracted');
};

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
