// The portal's frame — the same on every page: the left menu (Workspace · Tools · Manage) and the top
// bar (where you are, who's signed in, Log out). A page puts <header id="lab-top"></header> first in
// <body>, loads /admin.css and this script. /api/me (home-api, behind the Keycloak login) also creates
// the learner's lakehouse on first call; pages that need the same data listen for the "lab:me" event.
// Tools open inside the portal (/tool.html#<tool>); window.LAB_TOOLS has their addresses.
(function () {
  var host = location.hostname, k8s = /(^|\.)de\.lan$/.test(host);
  function url(sub, localPort, path) {
    return (k8s ? 'http://' + sub + '.' + host : 'http://' + (sub === 'spark' ? '127.0.0.1' : 'localhost') + ':' + localPort) + (path || '');
  }
  var TOOLS = window.LAB_TOOLS = {
    jupyter:    {name: 'Notebooks', what: 'Jupyter', svc: 'jupyter', url: url('jupyter', 8008)},
    sqlpad:     {name: 'SQL editor', what: 'SQLPad', svc: 'sqlpad', url: url('sqlpad', 8003, '/auth/oidc')},
    airflow:    {name: 'Pipelines', what: 'Airflow', svc: 'airflow', url: url('airflow', 8001)},
    superset:   {name: 'Dashboards', what: 'Superset', svc: 'superset', url: url('superset', 8004)},
    spark:      {name: 'Spark UI', what: 'Spark', svc: 'spark', url: url('spark', 8002)},
    storage:    {name: 'Storage', what: 'RustFS console', url: url('storage', 9001, '/rustfs/admin/v3/oidc/authorize/default')},
    governance: {name: 'Governance', what: 'Polaris Console', svc: 'governance', url: url('polaris-console', 8189, '/?sso=1'), managers: true,
                 external: k8s}            // k8s: its ingress sends the console's own SAMEORIGIN → a new tab
  };
  // simple line icons (24×24)
  var I = {
    home: '<path d="M3 11l9-7 9 7"/><path d="M5 10v10h5v-6h4v6h5V10"/>',
    files: '<path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>',
    catalogs: '<ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6"/><path d="M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/>',
    secrets: '<circle cx="8" cy="15" r="4"/><path d="M11 12l9-9M17 6l3 3M15 8l2 2"/>',
    exams: '<rect x="5" y="4" width="14" height="17" rx="2"/><path d="M9 4V3h6v1M9 11l2 2 4-4M9 17h6"/>',
    jupyter: '<path d="M4 5a1 1 0 0 1 1-1h11l4 4v11a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1z"/><path d="M8 13l2 2-2 2M12 17h4"/>',
    sqlpad: '<path d="M8 9l-4 3 4 3M16 9l4 3-4 3M13.5 6l-3 12"/>',
    airflow: '<circle cx="6" cy="6" r="2.5"/><circle cx="18" cy="12" r="2.5"/><circle cx="6" cy="18" r="2.5"/><path d="M8.5 6H12a3 3 0 0 1 3 3v.5M8.5 18H12a3 3 0 0 0 3-3v-.5"/>',
    superset: '<path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/>',
    spark: '<path d="M13 2L4 14h7l-1 8 9-12h-7z"/>',
    storage: '<rect x="3" y="4" width="18" height="7" rx="1.5"/><rect x="3" y="13" width="18" height="7" rx="1.5"/><path d="M7 7.5h.01M7 16.5h.01"/>',
    governance: '<path d="M12 3l8 3v6c0 4.5-3.4 8.3-8 9-4.6-.7-8-4.5-8-9V6z"/><path d="M9 12l2 2 4-4"/>',
    members: '<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20c.8-3.5 3.4-5.5 6.5-5.5s5.7 2 6.5 5.5"/><path d="M16 4.5a3.5 3.5 0 0 1 0 7M18 14.5c1.9.6 3.1 2.4 3.5 5.5"/>',
    teams: '<rect x="3" y="3" width="7.5" height="7.5" rx="1.5"/><rect x="13.5" y="3" width="7.5" height="7.5" rx="1.5"/><rect x="3" y="13.5" width="7.5" height="7.5" rx="1.5"/><rect x="13.5" y="13.5" width="7.5" height="7.5" rx="1.5"/>',
    activity: '<path d="M3 12h4l3-8 4 16 3-8h4"/>',
    settings: '<path d="M4 6h10M18 6h2M4 12h4M12 12h8M4 18h12M20 18h0"/><circle cx="16" cy="6" r="2"/><circle cx="10" cy="12" r="2"/><circle cx="18" cy="18" r="2"/>',
    course: '<path d="M2 8l10-5 10 5-10 5z"/><path d="M6 10v5c0 1.7 2.7 3 6 3s6-1.3 6-3v-5"/>',
    menu: '<path d="M4 6h16M4 12h16M4 18h16"/>'
  };
  function svg(k) { return '<svg viewBox="0 0 24 24" aria-hidden="true">' + (I[k] || '') + '</svg>'; }

  var WORK = [['/', 'home', 'Home'], ['/files.html', 'files', 'My files'], ['/catalogs.html', 'catalogs', 'My catalogs'],
              ['/secrets.html', 'secrets', 'My secrets'], ['/exams.html', 'exams', 'Mock exams']];
  var MANAGE = [['/manage.html', 'members', 'Members'], ['/teams.html', 'teams', 'Teams'], ['/activity.html', 'activity', 'Activity'],
                ['/exams-admin.html', 'exams', 'Exams'], ['/settings.html', 'settings', 'Settings']];
  var path = location.pathname.replace(/\/+$/, '') || '/';
  if (path === '/index.html') path = '/';
  var tool = path === '/tool.html' ? location.hash.slice(1) : null;

  function item(href, icon, label, on, extra) {
    return '<a class="item' + (on ? ' on' : '') + '" href="' + href + '"' + (extra || '') + '>' + svg(icon) + '<span>' + label + '</span></a>';
  }
  function title() {
    if (tool && TOOLS[tool]) return ['Tools', TOOLS[tool].name];
    var w = WORK.filter(function (x) { return x[0] === path; })[0];
    if (w) return ['Workspace', w[2]];
    var m = MANAGE.filter(function (x) { return x[0] === path; })[0];
    return m ? ['Manage', m[2]] : ['', document.title.split(' — ')[0]];
  }

  document.body.classList.add('lab');
  var nav = document.createElement('nav');
  nav.id = 'lab-nav';
  nav.innerHTML =
    '<a class="brand" href="/"><img src="/img/epireum-logo.png" alt=""><b>Data Engineering Lab<span>Epireum</span></b></a>' +
    '<div class="grp"><h6>Workspace</h6>' + WORK.map(function (x) { return item(x[0], x[1], x[2], x[0] === path); }).join('') + '</div>' +
    '<div class="grp"><h6>Tools</h6>' + Object.keys(TOOLS).map(function (k) {
      var t = TOOLS[k];
      return item(t.external ? t.url : '/tool.html#' + k, k, t.name + (t.external ? ' <span class="ext">↗</span>' : ''), tool === k,
                  ' data-tool="' + k + '"' + (t.managers ? ' data-mgr hidden' : '') + (t.external ? ' target="_blank" rel="noopener"' : '') + ' title="' + t.what + '"');
    }).join('') + '</div>' +
    '<div class="grp" id="lab-nav-manage" hidden><h6>Manage</h6>' + MANAGE.map(function (x) { return item(x[0], x[1], x[2], x[0] === path); }).join('') + '</div>' +
    '<div class="foot"><a href="https://maarthala.github.io/data-engineering-lab/" target="_blank" rel="noopener">Course ↗</a> · ' +
    '<a href="https://github.com/maarthala/data-engineering-lab#readme" target="_blank" rel="noopener">Docs ↗</a>' +
    '<div id="lab-nav-version" style="margin-top:4px"></div></div>';
  document.body.insertBefore(nav, document.body.firstChild);

  var top = document.getElementById('lab-top');
  if (!top) return;
  var t = title();
  top.innerHTML =
    '<div class="where"><button class="menu" id="lab-menu" aria-label="Menu">☰</button>' +
    (t[0] ? '<span class="crumb">' + t[0] + '</span><span class="crumb">/</span>' : '') + '<span id="lab-title">' + t[1] + '</span></div>' +
    '<div class="me" id="me" hidden><span class="lake" id="me-lake" hidden></span>' +
    '<span class="avatar" id="me-av"></span><span class="who"><b id="me-name"></b></span>' +
    '<a class="out" id="me-out" href="#">Log out</a></div>';
  document.getElementById('lab-menu').addEventListener('click', function () { document.body.classList.toggle('nav-open'); });

  var auth = k8s ? 'http://auth.' + host : 'http://localhost:8180';
  // Log out = clear oauth2-proxy's cookie, then end the Keycloak SSO session.
  var kcOut = auth + '/realms/de-lab/protocol/openid-connect/logout?client_id=de-lab-home' +
              '&post_logout_redirect_uri=' + encodeURIComponent(location.origin + '/');
  document.getElementById('me-out').href = '/oauth2/sign_out?rd=' + encodeURIComponent(kcOut);

  fetch('/api/me', {credentials: 'same-origin'})
    .then(function (r) { return r.ok ? r.json() : null; })
    .then(function (me) {
      if (!me) return;
      document.getElementById('me-name').textContent = me.user;
      document.getElementById('me-av').textContent = (me.user || '?').slice(0, 2);
      if (me.status !== 'ready') {
        var lake = document.getElementById('me-lake');
        lake.textContent = 'lakehouse not ready — ' + (me.detail || 'try again shortly');
        lake.hidden = false;
      }
      if (!k8s && me.airflow_port) TOOLS.airflow.url = 'http://localhost:' + me.airflow_port;
      var mgr = (me.groups || []).indexOf('managers') >= 0;
      document.getElementById('lab-nav-manage').hidden = !mgr;
      nav.querySelectorAll('[data-mgr]').forEach(function (a) { a.hidden = !mgr; });
      document.getElementById('me').hidden = false;
      document.dispatchEvent(new CustomEvent('lab:me', {detail: me}));
    })
    .catch(function () {});
  // stopped tools (local: make status · k8s: Settings → Services) are greyed in the menu
  fetch('/api/services').then(function (r) { return r.ok ? r.json() : {}; }).then(function (up) {
    nav.querySelectorAll('a.item[data-tool]').forEach(function (a) {
      var s = TOOLS[a.dataset.tool].svc;
      var off = s && up[s] === false;
      a.classList.toggle('off', !!off);
      if (off) a.title = TOOLS[a.dataset.tool].what + ' — not running';
    });
  }).catch(function () {});
  fetch('/api/version').then(function (r) { return r.ok ? r.json() : {}; }).then(function (v) {
    if (v.current) document.getElementById('lab-nav-version').textContent = 'Release ' + v.current;
  }).catch(function () {});
})();
