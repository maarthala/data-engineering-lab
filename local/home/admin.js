// Shared by the managers' pages (Manage in the left menu — Members, Teams, Activity, Exams,
// Settings): the page title, sub-tabs, the Actions menu, the result card, and calls to /api/admin
// (home-api, behind the Keycloak login; managers only). Actions run in the background — run()
// polls the job until it finishes.
var admin = (function () {
  var main, result, busy = false;

  function el(tag, attrs, kids) {
    var e = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (k) {
      if (k === 'text') e.textContent = attrs[k];
      else if (k === 'html') e.innerHTML = attrs[k];
      else if (k === 'on') Object.keys(attrs.on).forEach(function (ev) { e.addEventListener(ev, attrs.on[ev]); });
      else e.setAttribute(k, attrs[k]);
    });
    (kids || []).forEach(function (c) { if (c) e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c); });
    return e;
  }

  function api(method, path, body) {
    return fetch(path, {method: method, credentials: 'same-origin',
        headers: body ? {'Content-Type': 'application/json', 'X-Lab-Admin': '1'} : {},
        body: body ? JSON.stringify(body) : undefined})
      .then(function (r) { return r.json().then(function (j) {
        if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status)); return j; }); });
  }

  // start an action, then poll its job until done
  function run(action, body) {
    return api('POST', '/api/admin/' + action, body).then(function (j) {
      return new Promise(function (resolve, reject) {
        (function poll() {
          api('GET', '/api/admin/jobs?id=' + j.job).then(function (s) {
            if (s.state === 'running') return setTimeout(poll, 1500);
            if (s.state === 'done') resolve(s.result); else reject(new Error(s.error));
          }).catch(reject);
        })();
      });
    });
  }

  // the result card at the top of the page
  function show(title, lines, password, err) {
    result.className = 'card result' + (err ? ' err' : '');
    result.innerHTML = '';
    result.appendChild(el('h3', {text: title}));
    lines.forEach(function (l) { result.appendChild(el('div', {class: 'sub', text: l})); });
    if (password) {
      var box = el('span', {class: 'secret', text: password});
      var copy = el('button', {class: 'small', text: 'Copy', on: {click: function () {
        navigator.clipboard.writeText(password).then(function () { copy.textContent = 'Copied ✓'; });
      }}});
      result.appendChild(el('div', {class: 'row', style: 'margin-top:10px'}, [box, copy]));
      result.appendChild(el('div', {class: 'sub', style: 'margin-top:8px',
        text: 'Shown once — pass it on to the member. They choose their own password at the next sign-in.'}));
    }
    result.hidden = false;
    window.scrollTo({top: 0, behavior: 'smooth'});
  }

  // run an action from a control; `describe(result)` → [lines, password]; then `after()`
  function act(ctl, action, body, title, describe, after) {
    if (busy) return;
    busy = true;
    var label = ctl.textContent, isButton = ctl.tagName === 'BUTTON';
    document.querySelectorAll('main button, main select, main input').forEach(function (x) { x.disabled = true; });
    if (isButton) ctl.textContent = 'Working…';
    run(action, body).then(function (r) {
      var d = describe(r); show(title, d[0], d[1]);
    }).catch(function (e) {
      show('Could not ' + action + ' ' + (body.user || body.team || ''), [e.message], null, true);
    }).then(function () {
      busy = false;
      document.querySelectorAll('main button, main select, main input').forEach(function (x) { x.disabled = false; });
      if (isButton) ctl.textContent = label;
      if (after) after();
    });
  }

  // page title (+ a line about it) + result card; `load()` fills the page and is re-run after each action
  var bar;
  function page(title, desc) {
    bar = el('div', {class: 'pagebar'}, [el('h1', {text: title}), desc ? el('div', {class: 'desc', text: desc}) : null]);
    document.body.insertBefore(bar, document.getElementById('main'));
    main = document.getElementById('main');
    result = el('div', {class: 'card result', hidden: ''});
  }

  // sub-tabs under the title: [[id, label], …]; the open one is #<id> in the address. → current id
  function tabs(list, onChange) {
    var cur = location.hash.slice(1).split('/')[0];
    if (!list.some(function (t) { return t[0] === cur; })) cur = list[0][0];
    var nav = el('nav', {class: 'tabs'});
    list.forEach(function (t) {
      var a = el('a', {href: '#' + t[0], class: t[0] === cur ? 'on' : '', text: t[1]});
      a.addEventListener('click', function (e) {
        e.preventDefault();
        cur = t[0];
        history.replaceState(null, '', '#' + t[0]);
        nav.querySelectorAll('a').forEach(function (x) { x.className = x === a ? 'on' : ''; });
        result.hidden = true;
        onChange(cur);
      });
      nav.appendChild(a);
    });
    var old = bar.querySelector('nav.tabs');
    if (old) old.remove();
    bar.appendChild(nav);
    return cur;
  }

  // an "Actions ▾" button with a small menu: [{label, danger, click}] (closes on any outside click)
  function menu(label, items) {
    var box = el('span', {style: 'position:relative; display:inline-block'});
    var b = el('button', {class: 'small', text: label + ' ▾'});
    var pop = el('div', {class: 'menu-pop', hidden: ''});
    items.forEach(function (it) {
      var x = el('button', {class: it.danger ? 'danger' : '', text: it.label});
      x.addEventListener('click', function (e) { e.stopPropagation(); pop.hidden = true; it.click(b); });
      pop.appendChild(x);
    });
    b.addEventListener('click', function (e) {
      e.stopPropagation();
      document.querySelectorAll('.menu-pop').forEach(function (p) { if (p !== pop) p.hidden = true; });
      pop.hidden = !pop.hidden;
    });
    box.appendChild(b); box.appendChild(pop);
    return box;
  }
  document.addEventListener('click', function () { document.querySelectorAll('.menu-pop').forEach(function (p) { p.hidden = true; }); });

  // replace the page's cards (the result card stays on top)
  function fill(cards) {
    main.innerHTML = '';
    main.appendChild(result);
    cards.forEach(function (c) { main.appendChild(c); });
  }

  function failed(e) {
    if (main.querySelector('.card:not(.result)')) return show('Could not refresh the page', [e.message], null, true);
    main.innerHTML = '';
    main.appendChild(el('div', {class: 'placeholder', text: e.message === 'managers only'
      ? 'This page is for lab managers (Keycloak group “managers”).' : 'Could not load: ' + e.message}));
  }

  return {el: el, api: api, run: run, act: act, show: show, page: page, tabs: tabs, menu: menu, fill: fill, failed: failed};
})();
