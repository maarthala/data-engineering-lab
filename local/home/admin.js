// Shared by the managers' pages — 👥 Members (manage.html), 🧩 Teams (teams.html) and
// ⚙️ Settings (settings.html): the page bar with its tabs, the result card, and calls to
// /api/admin (home-api, behind the Keycloak login; managers only). Actions run in the
// background — run() polls the job until it finishes.
var admin = (function () {
  var TABS = [['/manage.html', '👥 Members'], ['/teams.html', '🧩 Teams'], ['/settings.html', '⚙️ Settings']];
  var main, result, busy = false;

  function el(tag, attrs, kids) {
    var e = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (k) {
      if (k === 'text') e.textContent = attrs[k];
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

  // page bar + tabs + result card; `load()` fills the page and is re-run after each action
  function page(title) {
    var here = location.pathname;
    document.body.insertBefore(el('div', {class: 'pagebar'}, [
      el('h1', {}, [title + ' ', el('span', {class: 'dot', text: '·'}), ' managers only']),
      el('nav', {class: 'tabs'}, TABS.map(function (t) {
        return el('a', {href: t[0], class: here === t[0] ? 'on' : '', text: t[1]});
      }))]), document.getElementById('main'));
    main = document.getElementById('main');
    result = el('div', {class: 'card result', hidden: ''});
  }

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

  return {el: el, api: api, act: act, show: show, page: page, fill: fill, failed: failed};
})();
