// The lab's top bar — the same on every page (home, My files, My catalogs, My variables).
// A page puts <header id="lab-top"></header> where the bar goes and loads this script; it draws
// the brand, who's signed in + their lakehouse, the page links (current one highlighted; managers
// also get Manage members) and Log out. /api/me (home-api, behind the Keycloak login) also creates the learner's lakehouse
// on first call. Pages that need the same data listen for the "lab:me" event.
(function () {
  var css = [
    '#lab-top{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;',
    '  padding:14px 28px;border-bottom:1px solid #2a3550;background:rgba(11,16,32,.72);',
    '  backdrop-filter:blur(8px);position:sticky;top:0;z-index:5;color:#e8edf7;',
    '  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}',
    '#lab-top a{color:inherit;text-decoration:none}',
    '#lab-top .brand{display:flex;align-items:center;gap:14px;min-width:0}',
    '#lab-top .brand img{width:46px;height:46px}',
    '#lab-top .name{font-size:1.25rem;font-weight:800;letter-spacing:.2px;white-space:nowrap}',
    '#lab-top .tag{color:#9aa8c7;font-size:.86rem;margin-top:2px}',
    '#lab-top .me{display:flex;align-items:center;gap:10px;flex-wrap:wrap;color:#9aa8c7;font-size:.9rem}',
    '#lab-top .me b{color:#e8edf7}',
    '#lab-top .lake{border:1px solid #2a3550;background:#141b2e;border-radius:999px;padding:5px 12px}',
    '#lab-top .lake.ok{border-color:rgba(57,208,196,.5)}',
    '#lab-top .lake.err{border-color:#d9534f;color:#ffb3b0}',
    '#lab-top a.btn{border:1px solid #2a3550;border-radius:8px;padding:5px 11px;background:#141b2e}',
    '#lab-top a.btn:hover{border-color:#6d8bff;color:#e8edf7}',
    '#lab-top a.btn.on{border-color:#6d8bff;color:#e8edf7;background:rgba(109,139,255,.15)}',
    '@media (max-width:720px){#lab-top{padding:12px 16px}#lab-top .tag{display:none}}'
  ].join('\n');
  var st = document.createElement('style'); st.textContent = css; document.head.appendChild(st);

  var top = document.getElementById('lab-top');
  if (!top) return;
  var here = location.pathname.replace(/\/+$/, '') || '/';
  function link(href, text) {
    var on = (href === '/' ? (here === '/' || here === '/index.html') : here === href);
    return '<a class="btn' + (on ? ' on' : '') + '" href="' + href + '">' + text + '</a>';
  }
  top.innerHTML =
    '<a class="brand" href="/"><img src="/img/epireum-logo.png" alt="Epireum">' +
    '<div><div class="name">Epireum\'s Data Engineering Lab</div>' +
    '<div class="tag">Your gateway to a Data Engineering career</div></div></a>' +
    '<div class="me" id="me" hidden><span>Hi <b id="me-name"></b></span><span class="lake" id="me-lake"></span>' +
    link('/', '🏠 Home') + link('/files.html', '📁 My files') + link('/catalogs.html', '🗂️ My catalogs') + link('/variables.html', '🔑 My variables') +
    '<span id="me-admin" hidden>' + link('/manage.html', '👥 Manage members') + '</span>' +
    '<a class="btn" id="me-out" href="#">Log out</a></div>';

  var k8s = /(^|\.)de\.lan$/.test(location.hostname);
  var auth = k8s ? 'http://auth.de.lan' : 'http://localhost:8180';
  // Log out = clear oauth2-proxy's cookie, then end the Keycloak SSO session.
  var kcOut = auth + '/realms/de-lab/protocol/openid-connect/logout?client_id=de-lab-home' +
              '&post_logout_redirect_uri=' + encodeURIComponent(location.origin + '/');
  document.getElementById('me-out').href = '/oauth2/sign_out?rd=' + encodeURIComponent(kcOut);

  fetch('/api/me', {credentials: 'same-origin'})
    .then(function (r) { return r.ok ? r.json() : null; })
    .then(function (me) {
      if (!me) return;
      document.getElementById('me-name').textContent = me.user;
      var lake = document.getElementById('me-lake');
      if (me.status === 'ready') {
        lake.textContent = 'your lakehouse: ' + me.lakehouse + ' ✓ · bucket: ' + me.bucket;
        lake.className = 'lake ok';
      } else {
        lake.textContent = 'lakehouse not ready — ' + (me.detail || 'try again shortly');
        lake.className = 'lake err';
      }
      // managers (Keycloak group) also get the member admin page
      document.getElementById('me-admin').hidden = (me.groups || []).indexOf('managers') < 0;
      document.getElementById('me').hidden = false;
      document.dispatchEvent(new CustomEvent('lab:me', {detail: me}));
    })
    .catch(function () {});
})();
