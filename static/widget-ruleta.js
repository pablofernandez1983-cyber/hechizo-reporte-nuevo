(function () {
  'use strict';

  const COOKIE   = 'hb_ruleta_v1';
  const ENDPOINT = 'https://hechizo-reporte-nuevo-production.up.railway.app/ruleta/participar';
  const LAUNCH_AT = Date.parse('2026-06-16T03:00:00Z'); // 16/06 00:00 Argentina
  const PREVIEW_PARAM = 'ruleta_preview';
  const PREVIEW_CODE = 'P9YS4XOcHL_mFgrjoKuMx7jw';
  const PREVIEW_SESSION = 'hb_ruleta_preview';

  var host = window.location.hostname.toLowerCase();
  var isProduction = host === 'hechizo.com.ar' || host === 'www.hechizo.com.ar';
  var params = new URLSearchParams(window.location.search);
  var previewRequested = params.get(PREVIEW_PARAM) === PREVIEW_CODE;

  if (previewRequested) {
    sessionStorage.setItem(PREVIEW_SESSION, '1');
    params.delete(PREVIEW_PARAM);
    var cleanUrl = window.location.pathname + (params.toString() ? '?' + params.toString() : '') + window.location.hash;
    window.history.replaceState({}, document.title, cleanUrl);
  }

  var previewEnabled = sessionStorage.getItem(PREVIEW_SESSION) === '1';
  if (isProduction && Date.now() < LAUNCH_AT && !previewEnabled) return;

  if (document.getElementById('hb-overlay')) return;
  if (!previewEnabled && document.cookie.split(';').some(function (c) { return c.trim().startsWith(COOKIE + '='); })) return;

  // ── Google Fonts ──────────────────────────────────────────────────────────────
  var fontLink = document.createElement('link');
  fontLink.rel = 'stylesheet';
  fontLink.href = 'https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,600;1,400;1,600&family=Montserrat:wght@600;700;800&display=swap';
  document.head.appendChild(fontLink);

  // ── Estilos ───────────────────────────────────────────────────────────────────
  var style = document.createElement('style');
  style.textContent = [
    '#hb-overlay{position:fixed;inset:0;background:rgba(10,35,75,.75);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);z-index:99999;display:none;align-items:center;justify-content:center;padding:12px;animation:hbFade .35s ease}',
    '@keyframes hbFade{from{opacity:0}to{opacity:1}}',
    '#hb-modal{background:#f0f8ff;border-radius:20px;width:100%;max-width:420px;max-height:calc(100vh - 24px);overflow-y:auto;position:relative;box-shadow:0 30px 80px rgba(0,40,100,.35),0 0 0 1px #75AADB44;animation:hbUp .42s cubic-bezier(.22,1,.36,1)}',
    '@keyframes hbUp{from{transform:translateY(28px);opacity:0}to{transform:translateY(0);opacity:1}}',
    '.hb-header{background:repeating-linear-gradient(-55deg,#5A97CC 0px,#5A97CC 14px,#75AADB 14px,#75AADB 28px);padding:14px 24px 12px;text-align:center;border-radius:20px 20px 0 0;position:relative;overflow:hidden}',
    '.hb-stars{font-size:13px;letter-spacing:8px;color:#FFD700;text-shadow:0 1px 4px rgba(0,0,0,.4);margin-bottom:4px}',
    '.hb-logo{font-family:"Montserrat",sans-serif;font-size:17px;font-weight:800;letter-spacing:5px;text-transform:uppercase;color:#fff;text-shadow:0 2px 10px rgba(0,0,0,.3)}',
    '.hb-body{padding:14px 22px 16px}',
    '.hb-title{font-family:"Cormorant Garamond",serif;font-size:24px;font-weight:400;font-style:italic;color:#1A3A5C;text-align:center;line-height:1.2;margin-bottom:12px}',
    '.hb-wheel-wrap{position:relative;width:220px;height:220px;margin:0 auto 12px}',
    '#hb-canvas{display:block;border-radius:50%;box-shadow:0 0 0 3px #f0f8ff,0 0 0 5px #75AADB,0 8px 30px rgba(0,60,120,.22)}',
    '.hb-pointer{position:absolute;top:-10px;left:50%;transform:translateX(-50%);width:0;height:0;border-left:9px solid transparent;border-right:9px solid transparent;border-top:20px solid #1A3A5C;filter:drop-shadow(0 3px 6px rgba(0,40,100,.4));z-index:2}',
    '.hb-center{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);width:30px;height:30px;border-radius:50%;background:#f0f8ff;border:3px solid #75AADB;z-index:3;display:flex;align-items:center;justify-content:center;font-size:14px;box-shadow:0 2px 10px rgba(0,60,120,.15)}',
    '.hb-email-wrap{margin-bottom:8px}',
    '#hb-email{width:100%;box-sizing:border-box;border:1.5px solid #B8D8F0;border-radius:50px;padding:11px 18px;font-family:"Montserrat",sans-serif;font-size:12px;color:#1A3A5C;background:#fff;outline:none;transition:border-color .2s,box-shadow .2s;text-align:center}',
    '#hb-email::placeholder{color:#90B0D0}',
    '#hb-email:focus{border-color:#75AADB;box-shadow:0 0 0 3px rgba(117,170,219,.15)}',
    '#hb-email.hb-valid{border-color:#2A5A9C}',
    '#hb-email-error{display:none;color:#b42318;font-size:10px;text-align:center;margin:6px 0 8px;line-height:1.4}',
    '#hb-spin-btn{display:block;width:100%;background:linear-gradient(135deg,#1A3A5C,#2A5A9C);color:#fff;border:none;padding:13px 0;font-family:"Montserrat",sans-serif;font-size:13px;font-weight:700;letter-spacing:2px;text-transform:uppercase;cursor:pointer;border-radius:50px;margin-bottom:8px;transition:transform .15s,box-shadow .15s;box-shadow:0 4px 20px rgba(0,60,120,.3)}',
    '#hb-spin-btn:hover:not(:disabled){transform:translateY(-2px);box-shadow:0 8px 28px rgba(0,60,120,.42)}',
    '#hb-spin-btn:disabled{opacity:.35;cursor:default;transform:none;box-shadow:none}',
    '.hb-legal{font-size:9px;font-weight:300;letter-spacing:.6px;color:#6080A0;text-align:center;line-height:1.6;margin-bottom:0}',
    '.hb-minimum{background:#e8f4ff;border:1.5px solid #75AADB;border-radius:10px;padding:8px 12px;margin:0 0 7px;color:#1A3A5C;font-family:"Montserrat",sans-serif;font-size:12px;font-weight:700;letter-spacing:.3px;text-align:center;line-height:1.35}',
    '#hb-no-gracias{display:block;width:100%;background:none;border:none;padding:7px 0 0;font-size:9px;letter-spacing:.8px;color:#9AB0C8;text-align:center;cursor:pointer;text-decoration:underline;text-underline-offset:2px;font-family:"Montserrat",sans-serif}',
    '#hb-no-gracias:hover{color:#5A7A9A}',
    '#hb-close{position:absolute;top:12px;right:14px;background:#ffffff22;border:none;color:#fff;font-size:15px;cursor:pointer;border-radius:50%;width:26px;height:26px;display:flex;align-items:center;justify-content:center;z-index:10;transition:background .2s}',
    '#hb-close:hover{background:#ffffff44}',
    '#hb-result{display:none;text-align:center;animation:hbPop .45s cubic-bezier(.34,1.56,.64,1)}',
    '@keyframes hbPop{from{transform:scale(.75);opacity:0}to{transform:scale(1);opacity:1}}',
    '.res-emoji{font-size:34px;margin-bottom:6px}',
    '.res-title{font-family:"Cormorant Garamond",serif;font-size:24px;font-style:italic;color:#1A3A5C;margin-bottom:4px}',
    '.res-prize{font-family:"Montserrat",sans-serif;font-size:28px;font-weight:800;letter-spacing:2px;background:linear-gradient(135deg,#1A3A5C,#75AADB);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;margin-bottom:6px;line-height:1.2}',
    '.res-desc{font-size:10px;font-weight:300;letter-spacing:.8px;color:#4A6E8A;margin-bottom:14px;line-height:1.7}',
    '.hb-coupon{border:1.5px dashed #75AADB;border-radius:14px;padding:12px 18px;margin-bottom:12px;background:linear-gradient(135deg,#e8f4ff,#f0f8ff);cursor:pointer;transition:border-color .2s,box-shadow .2s}',
    '.hb-coupon:hover{border-color:#1A3A5C;box-shadow:0 4px 16px rgba(0,60,120,.12)}',
    '.hb-coupon-label{font-size:8px;letter-spacing:3px;text-transform:uppercase;color:#4A80B0;margin-bottom:5px;font-family:"Montserrat",sans-serif}',
    '.hb-coupon-code{font-family:"Montserrat",sans-serif;font-size:22px;font-weight:800;letter-spacing:4px;color:#1A3A5C}',
    '.hb-coupon-hint{font-size:8px;letter-spacing:1.5px;color:#6080A0;margin-top:4px;text-transform:uppercase;font-family:"Montserrat",sans-serif}',
    '#hb-copy-btn{display:block;width:100%;background:transparent;border:1.5px solid #75AADB;color:#1A3A5C;padding:11px 0;font-family:"Montserrat",sans-serif;font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;cursor:pointer;border-radius:50px;margin-bottom:10px;transition:all .2s}',
    '#hb-copy-btn:hover{background:linear-gradient(135deg,#1A3A5C,#2A5A9C);border-color:transparent;color:#fff}',
    '.hb-divider{display:flex;align-items:center;gap:10px;margin:0 0 10px}',
    '.hb-divider::before,.hb-divider::after{content:"";flex:1;height:1px;background:linear-gradient(to right,transparent,#75AADB,transparent)}',
    '.hb-divider span{font-size:12px;color:#75AADB}',
    '#hb-spin-section{transition:opacity .25s}',
  ].join('\n');
  document.head.appendChild(style);

  // ── HTML ──────────────────────────────────────────────────────────────────────
  var wrap = document.createElement('div');
  wrap.innerHTML = '<div id="hb-overlay">' +
    '<div id="hb-modal">' +
      '<div class="hb-header">' +
        '<button id="hb-close">✕</button>' +
        '<div class="hb-stars">⭐ ⭐ ⭐</div>' +
        '<div class="hb-logo">Hechizo Bijou</div>' +
      '</div>' +
      '<div class="hb-body">' +
        '<div id="hb-spin-section">' +
          '<h2 class="hb-title">Girá y descubrí tu regalo</h2>' +
          '<div class="hb-wheel-wrap">' +
            '<div class="hb-pointer"></div>' +
            '<canvas id="hb-canvas" width="220" height="220"></canvas>' +
            '<div class="hb-center">⭐</div>' +
          '</div>' +
          '<div class="hb-email-wrap">' +
            '<input id="hb-email" type="email" placeholder="Tu email para recibir el cupón" autocomplete="email">' +
            '<div id="hb-email-error"></div>' +
          '</div>' +
          '<button id="hb-spin-btn" disabled>⚽ ¡Girar la ruleta!</button>' +
          '<div class="hb-minimum">Aplica en compras mayores a $50.000</div>' +
          '<p class="hb-legal">Un solo uso · Válido en tu próxima compra · No acumulable</p>' +
          '<button id="hb-no-gracias">No quiero participar</button>' +
        '</div>' +
        '<div id="hb-result"></div>' +
      '</div>' +
    '</div>' +
  '</div>';
  document.body.appendChild(wrap);

  // ── Segmentos ─────────────────────────────────────────────────────────────────
  // fake:true → figura en la ruleta pero nunca puede salir como ganador
  var HB_SEG = [
    { label: 'Pulsera de\nArgentina', bg: '#C9A227', fg: '#ffffff', codigo: 'HECHIZOPULSERA', tipo: 'pulsera'   },
    { label: '10% OFF',               bg: '#75AADB', fg: '#ffffff', codigo: 'HECHIZO10',      tipo: 'descuento' },
    { label: '5% OFF',                bg: '#D6EAFA', fg: '#1A3A5C', codigo: null,             tipo: 'fake',      fake: true },
    { label: 'Envío\nGratis',         bg: '#5A97CC', fg: '#ffffff', codigo: 'HECHIZOENV',     tipo: 'envio'     },
    { label: '$4.000',                bg: '#1A3A5C', fg: '#ffffff', codigo: 'HECHIZO4000',    tipo: 'monto'     },
    { label: '15% OFF',               bg: '#2A5A9C', fg: '#ffffff', codigo: 'HECHIZO15',      tipo: 'descuento' },
  ];

  var N   = HB_SEG.length;
  var ARC = (2 * Math.PI) / N;
  var hbAng = 0, hbSpinning = false, hbEmail = '';

  // ── Canvas ────────────────────────────────────────────────────────────────────
  function hbDraw(ang) {
    var cv  = document.getElementById('hb-canvas');
    var ctx = cv.getContext('2d');
    var cx = 110, cy = 110, r = 106;
    ctx.clearRect(0, 0, 220, 220);

    HB_SEG.forEach(function (s, i) {
      var a0 = ang + i * ARC - Math.PI / 2;
      var a1 = a0 + ARC;
      ctx.beginPath(); ctx.moveTo(cx, cy);
      ctx.arc(cx, cy, r, a0, a1); ctx.closePath();
      ctx.fillStyle = s.bg; ctx.fill();
      ctx.strokeStyle = '#f0f8ff55'; ctx.lineWidth = 1.5; ctx.stroke();

      ctx.save(); ctx.translate(cx, cy); ctx.rotate(a0 + ARC / 2);
      ctx.textAlign = 'right'; ctx.fillStyle = s.fg;
      ctx.shadowColor = 'rgba(0,0,0,.3)'; ctx.shadowBlur = 3;
      var lines = s.label.split('\n');
      if (lines.length === 1) {
        ctx.font = "700 13px 'Montserrat', sans-serif";
        ctx.fillText(s.label, r - 10, 5);
      } else {
        ctx.font = "700 11px 'Montserrat', sans-serif";
        ctx.fillText(lines[0], r - 10, -3);
        ctx.fillText(lines[1], r - 10, 10);
      }
      ctx.restore();
    });

    ctx.beginPath(); ctx.arc(cx, cy, r, 0, 2 * Math.PI);
    ctx.strokeStyle = '#75AADB'; ctx.lineWidth = 3; ctx.stroke();

    HB_SEG.forEach(function (_, i) {
      var ta = ang + i * ARC - Math.PI / 2;
      ctx.beginPath();
      ctx.moveTo(cx + (r - 6) * Math.cos(ta), cy + (r - 6) * Math.sin(ta));
      ctx.lineTo(cx + (r + 1) * Math.cos(ta), cy + (r + 1) * Math.sin(ta));
      ctx.strokeStyle = '#B8D8F0'; ctx.lineWidth = 1.5; ctx.stroke();
    });
  }

  // ── Spin ──────────────────────────────────────────────────────────────────────
  function hbGirar() {
    if (hbSpinning) return;
    hbSpinning = true;
    var btn = document.getElementById('hb-spin-btn');
    btn.disabled = true;
    btn.textContent = 'Verificando...';
    document.getElementById('hb-email').disabled = true;
    hbSetError('');

    hbReservarParticipacion(hbEmail).then(function (premio) {
      var winner = HB_SEG.findIndex(function (seg) { return seg.codigo === premio; });
      if (winner < 0) throw new Error('Premio inválido');
      hbAnimarGiro(winner);
    }).catch(function (err) {
      hbSpinning = false;
      document.getElementById('hb-email').disabled = false;
      btn.disabled = false;
      btn.textContent = '⚽ ¡Girar la ruleta!';
      hbSetError(err.message || 'No pudimos validar tu participación. Intentá nuevamente.');
    });
  }

  function hbAnimarGiro(winner) {
    // extra DEBE ser entero: extra no-entero hace que el segmento ganador quede ~180° girado
    var extra    = 5 + Math.floor(Math.random() * 4); // 5, 6, 7 u 8 vueltas completas
    var offset   = (Math.random() - 0.5) * ARC * 0.4; // variación dentro del segmento
    var angFinal = hbAng + extra * 2 * Math.PI - (winner + 0.5) * ARC + offset;
    var dur = 4400, t0 = performance.now(), ang0 = hbAng;

    (function frame(t) {
      var p    = Math.min((t - t0) / dur, 1);
      var ease = 1 - Math.pow(1 - p, 3.5);
      hbAng = ang0 + (angFinal - ang0) * ease;
      hbDraw(hbAng);
      if (p < 1) {
        requestAnimationFrame(frame);
      } else {
        hbAng = angFinal;
        hbSpinning = false;
        var seg = HB_SEG[winner];
        hbShowResult(seg);
        hbSetCookie(COOKIE, '1', 365);
      }
    })(t0);
  }

  // ── Reserva de participación ──────────────────────────────────────────────────
  function hbReservarParticipacion(email) {
    var storeId = (window.LS && window.LS.store && window.LS.store.id) || null;
    return fetch(ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: email, store_id: storeId }),
    }).then(function (response) {
      return response.json().catch(function () { return {}; }).then(function (data) {
        if (!response.ok) {
          throw new Error(data.error || 'No pudimos validar tu participación. Intentá nuevamente.');
        }
        return data.premio;
      });
    });
  }

  function hbSetError(message) {
    var error = document.getElementById('hb-email-error');
    error.textContent = message;
    error.style.display = message ? 'block' : 'none';
  }

  // ── Result ────────────────────────────────────────────────────────────────────
  var LEGAL_RES = '<div class="hb-minimum">Aplica en compras mayores a $50.000</div>' +
    '<p class="hb-legal">Válido 7 días · Un solo uso · No acumulable</p>';

  function hbShowResult(seg) {
    var res  = document.getElementById('hb-result');
    var spin = document.getElementById('hb-spin-section');
    var html = '<div class="hb-divider" style="margin-top:4px"><span>⭐</span></div>';

    if (seg.tipo === 'pulsera') {
      html +=
        '<div class="res-emoji">🏆</div>' +
        '<h2 class="res-title">¡Ganaste!</h2>' +
        '<div class="res-prize">PULSERA DE<br>ARGENTINA</div>' +
        '<p class="res-desc">Aplicá este cupón al finalizar tu compra<br>y recibís la pulsera junto con tu pedido.</p>' +
        hbCuponHTML(seg.codigo) +
        LEGAL_RES;
    } else if (seg.tipo === 'envio') {
      html +=
        '<div class="res-emoji">🏆</div>' +
        '<h2 class="res-title">¡Ganaste!</h2>' +
        '<div class="res-prize">ENVÍO GRATIS</div>' +
        '<p class="res-desc">Aplicá este cupón al finalizar tu compra<br>y el envío corre por nuestra cuenta.</p>' +
        hbCuponHTML(seg.codigo) +
        LEGAL_RES;
    } else if (seg.tipo === 'monto') {
      html +=
        '<div class="res-emoji">🏆</div>' +
        '<h2 class="res-title">¡Tu regalo está listo!</h2>' +
        '<div class="res-prize">$4.000 OFF</div>' +
        '<p class="res-desc">Aplicá este cupón al finalizar tu compra<br>y el descuento se aplica automáticamente.</p>' +
        hbCuponHTML(seg.codigo) +
        LEGAL_RES;
    } else {
      html +=
        '<div class="res-emoji">🏆</div>' +
        '<h2 class="res-title">¡Tu regalo está listo!</h2>' +
        '<div class="res-prize">' + seg.label + '</div>' +
        '<p class="res-desc">Aplicá este cupón al finalizar tu compra<br>y el descuento se aplica automáticamente.</p>' +
        hbCuponHTML(seg.codigo) +
        LEGAL_RES;
    }

    res.innerHTML = html;
    spin.style.opacity = '0';
    setTimeout(function () { spin.style.display = 'none'; res.style.display = 'block'; }, 250);
  }

  function hbCuponHTML(c) {
    return (
      '<div class="hb-coupon" onclick="hbCopiar(\'' + c + '\')">' +
        '<div class="hb-coupon-label">Tu cupón exclusivo</div>' +
        '<div class="hb-coupon-code">' + c + '</div>' +
        '<div class="hb-coupon-hint">Tocá para copiar</div>' +
      '</div>' +
      '<button id="hb-copy-btn" onclick="hbCopiar(\'' + c + '\')">Copiar cupón</button>'
    );
  }

  // ── Utils ─────────────────────────────────────────────────────────────────────
  function hbCopiar(c) {
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(c).then(hbCopyFeedback);
    } else {
      var ta = document.createElement('textarea');
      ta.value = c; ta.style.cssText = 'position:fixed;opacity:0;top:0;left:0';
      document.body.appendChild(ta); ta.focus(); ta.select();
      try { document.execCommand('copy'); } catch (_) {}
      document.body.removeChild(ta);
      hbCopyFeedback();
    }
  }

  function hbCopyFeedback() {
    var b = document.getElementById('hb-copy-btn');
    if (b) { b.textContent = '✓ ¡Copiado!'; setTimeout(function () { b.textContent = 'Copiar cupón'; }, 2000); }
  }

  function hbSetCookie(n, v, d) {
    var e = new Date(); e.setTime(e.getTime() + d * 864e5);
    document.cookie = n + '=' + v + ';expires=' + e.toUTCString() + ';path=/';
  }

  // ── Event listeners ───────────────────────────────────────────────────────────
  document.getElementById('hb-close').addEventListener('click', function () {
    document.getElementById('hb-overlay').style.display = 'none';
    // sin cookie → vuelve a aparecer la próxima visita
  });

  document.getElementById('hb-no-gracias').addEventListener('click', function () {
    document.getElementById('hb-overlay').style.display = 'none';
    hbSetCookie(COOKIE, '1', 365);
    // con cookie → no vuelve a aparecer
  });

  document.getElementById('hb-email').addEventListener('input', function () {
    hbEmail = this.value.trim();
    var valid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(hbEmail);
    this.classList.toggle('hb-valid', valid);
    hbSetError('');
    document.getElementById('hb-spin-btn').disabled = !valid;
  });

  document.getElementById('hb-spin-btn').addEventListener('click', hbGirar);

  window.hbCopiar = hbCopiar;

  // ── Init ──────────────────────────────────────────────────────────────────────
  hbDraw(0);
  setTimeout(function () {
    document.getElementById('hb-overlay').style.display = 'flex';
  }, 800);

})();
