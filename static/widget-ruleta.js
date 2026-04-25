(function () {
  'use strict';

  const COOKIE = 'hb_ruleta_v1';

  if (document.getElementById('hb-overlay')) return;
  if (document.cookie.split(';').some(function (c) { return c.trim().startsWith(COOKIE + '='); })) return;

  // ── Google Fonts ──────────────────────────────────────────────────────────────
  var fontLink = document.createElement('link');
  fontLink.rel = 'stylesheet';
  fontLink.href = 'https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;0,600;1,300;1,400;1,600&family=Jost:wght@200;300;400&display=swap';
  document.head.appendChild(fontLink);

  // ── Estilos ───────────────────────────────────────────────────────────────────
  var style = document.createElement('style');
  style.textContent = [
    '#hb-overlay{position:fixed;inset:0;background:rgba(40,20,55,.70);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);z-index:99999;display:none;align-items:center;justify-content:center;padding:16px;animation:hbFade .35s ease}',
    '@keyframes hbFade{from{opacity:0}to{opacity:1}}',
    '#hb-modal{background:#fdf8ff;border-radius:20px;width:100%;max-width:440px;max-height:96vh;overflow-y:auto;position:relative;box-shadow:0 30px 80px rgba(80,30,110,.3),0 0 0 1px #e2c8f022;animation:hbUp .42s cubic-bezier(.22,1,.36,1)}',
    '@keyframes hbUp{from{transform:translateY(28px);opacity:0}to{transform:translateY(0);opacity:1}}',
    '.hb-header{background:linear-gradient(135deg,#6b3fa0,#a05080,#c47aa0);padding:22px 24px 20px;text-align:center;border-radius:20px 20px 0 0;position:relative;overflow:hidden}',
    '.hb-header::before{content:"✦ ✦ ✦ ✦ ✦ ✦ ✦ ✦ ✦ ✦";position:absolute;top:6px;left:0;right:0;font-size:8px;letter-spacing:14px;color:#ffffff22;text-align:center}',
    '.hb-header::after{content:"✦ ✦ ✦ ✦ ✦ ✦ ✦ ✦ ✦ ✦";position:absolute;bottom:6px;left:0;right:0;font-size:8px;letter-spacing:14px;color:#ffffff22;text-align:center}',
    '.hb-logo{font-family:"Cormorant Garamond",serif;font-size:24px;font-weight:300;letter-spacing:7px;text-transform:uppercase;color:#fff;text-shadow:0 2px 12px rgba(0,0,0,.2)}',
    '.hb-tagline{font-size:10px;font-weight:200;letter-spacing:2.5px;text-transform:uppercase;color:#f0d8ff99;margin-top:3px}',
    '.hb-body{padding:22px 26px 28px}',
    '.hb-divider{display:flex;align-items:center;gap:10px;margin:0 0 16px}',
    '.hb-divider::before,.hb-divider::after{content:"";flex:1;height:1px;background:linear-gradient(to right,transparent,#c8a0d8,transparent)}',
    '.hb-divider span{font-size:13px;color:#b078c8}',
    '.hb-title{font-family:"Cormorant Garamond",serif;font-size:30px;font-weight:400;font-style:italic;color:#3d1f5a;text-align:center;line-height:1.2;margin-bottom:7px}',
    '.hb-sub{font-size:11px;font-weight:300;letter-spacing:1px;line-height:1.7;color:#9070a8;text-align:center;margin-bottom:20px}',
    '.hb-wheel-wrap{position:relative;width:248px;height:248px;margin:0 auto 20px}',
    '#hb-canvas{display:block;border-radius:50%;box-shadow:0 0 0 4px #fdf8ff,0 0 0 6px #c8a0d8,0 12px 40px rgba(100,50,140,.25)}',
    '.hb-pointer{position:absolute;top:-12px;left:50%;transform:translateX(-50%);width:0;height:0;border-left:10px solid transparent;border-right:10px solid transparent;border-top:22px solid #6b3fa0;filter:drop-shadow(0 3px 6px rgba(80,30,110,.4));z-index:2}',
    '.hb-center{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);width:36px;height:36px;border-radius:50%;background:#fdf8ff;border:3px solid #c8a0d8;z-index:3;display:flex;align-items:center;justify-content:center;font-size:14px;box-shadow:0 2px 10px rgba(100,50,140,.2)}',
    '#hb-spin-btn{display:block;width:100%;background:linear-gradient(135deg,#7b3faf,#b05888);color:#fff;border:none;padding:15px 0;font-family:"Cormorant Garamond",serif;font-size:18px;font-style:italic;letter-spacing:2px;cursor:pointer;border-radius:50px;margin-bottom:14px;transition:transform .15s,box-shadow .15s;box-shadow:0 4px 20px rgba(100,50,140,.3)}',
    '#hb-spin-btn:hover:not(:disabled){transform:translateY(-2px);box-shadow:0 8px 28px rgba(100,50,140,.4)}',
    '#hb-spin-btn:disabled{opacity:.45;cursor:default;transform:none;box-shadow:none}',
    '.hb-legal{font-size:10px;font-weight:300;letter-spacing:.8px;color:#b090c0;text-align:center;line-height:1.7}',
    '#hb-close{position:absolute;top:14px;right:16px;background:#ffffff22;border:none;color:#fff;font-size:16px;cursor:pointer;border-radius:50%;width:28px;height:28px;display:flex;align-items:center;justify-content:center;z-index:10;transition:background .2s}',
    '#hb-close:hover{background:#ffffff44}',
    '#hb-result{display:none;text-align:center;animation:hbPop .45s cubic-bezier(.34,1.56,.64,1)}',
    '@keyframes hbPop{from{transform:scale(.75);opacity:0}to{transform:scale(1);opacity:1}}',
    '.res-emoji{font-size:40px;margin-bottom:10px}',
    '.res-title{font-family:"Cormorant Garamond",serif;font-size:28px;font-style:italic;color:#3d1f5a;margin-bottom:6px}',
    '.res-prize{font-family:"Cormorant Garamond",serif;font-size:40px;font-weight:600;background:linear-gradient(135deg,#7b3faf,#c05888);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;margin-bottom:8px}',
    '.res-desc{font-size:11px;font-weight:300;letter-spacing:1px;color:#9070a8;margin-bottom:20px;line-height:1.8}',
    '.hb-coupon{border:1.5px dashed #c8a0d8;border-radius:14px;padding:15px 20px;margin-bottom:16px;background:linear-gradient(135deg,#fdf4ff,#faf0ff);cursor:pointer;transition:border-color .2s,box-shadow .2s}',
    '.hb-coupon:hover{border-color:#9060c0;box-shadow:0 4px 16px rgba(100,50,140,.12)}',
    '.hb-coupon-label{font-size:9px;letter-spacing:3px;text-transform:uppercase;color:#b088c8;margin-bottom:6px}',
    '.hb-coupon-code{font-family:"Cormorant Garamond",serif;font-size:28px;font-weight:600;letter-spacing:5px;color:#3d1f5a}',
    '.hb-coupon-hint{font-size:9px;letter-spacing:1.5px;color:#b090c0;margin-top:5px;text-transform:uppercase}',
    '#hb-copy-btn{display:block;width:100%;background:transparent;border:1.5px solid #9060c0;color:#7040a0;padding:13px 0;font-family:"Cormorant Garamond",serif;font-size:16px;font-style:italic;letter-spacing:2px;cursor:pointer;border-radius:50px;margin-bottom:14px;transition:all .2s}',
    '#hb-copy-btn:hover{background:linear-gradient(135deg,#7b3faf,#b05888);border-color:transparent;color:#fff}',
    '.hb-consolation{background:linear-gradient(135deg,#f8f0ff,#fff0f6);border-left:3px solid #c8a0d8;border-radius:0 12px 12px 0;padding:14px 16px;margin-bottom:16px;text-align:left}',
    '.hb-consolation p{font-size:12px;font-weight:300;color:#7050a0;line-height:1.8}',
    '.hb-consolation strong{color:#9040c0;font-weight:500}',
    '#hb-spin-section{transition:opacity .25s}',
  ].join('\n');
  document.head.appendChild(style);

  // ── HTML ──────────────────────────────────────────────────────────────────────
  var wrap = document.createElement('div');
  wrap.innerHTML = '<div id="hb-overlay">' +
    '<div id="hb-modal">' +
      '<div class="hb-header">' +
        '<button id="hb-close">✕</button>' +
        '<div class="hb-logo">Hechizo Bijou</div>' +
        '<div class="hb-tagline">Protegé tu energía ✨</div>' +
      '</div>' +
      '<div class="hb-body">' +
        '<div id="hb-spin-section">' +
          '<div class="hb-divider"><span>✦</span></div>' +
          '<h2 class="hb-title">Girá y descubrí<br>tu regalo</h2>' +
          '<p class="hb-sub">Un gesto mágico para tu primera visita.<br>Girá la ruleta y lleváte un descuento exclusivo.</p>' +
          '<div class="hb-wheel-wrap">' +
            '<div class="hb-pointer"></div>' +
            '<canvas id="hb-canvas" width="248" height="248"></canvas>' +
            '<div class="hb-center">✦</div>' +
          '</div>' +
          '<button id="hb-spin-btn">✨ ¡Girar la ruleta!</button>' +
          '<p class="hb-legal">Un solo uso · Válido en tu próxima compra · No acumulable</p>' +
        '</div>' +
        '<div id="hb-result"></div>' +
      '</div>' +
    '</div>' +
  '</div>';
  document.body.appendChild(wrap);

  // ── Segmentos ─────────────────────────────────────────────────────────────────
  var HB_SEG = [
    { label: '10% OFF',          bg: '#7b3fa0', fg: '#f8e8ff', codigo: 'HECHIZO10',  tipo: 'descuento' },
    { label: '¡Casi!\nSeguí', bg: '#e8d8f4', fg: '#8060a8', codigo: null,          tipo: 'nada'      },
    { label: '15% OFF',          bg: '#a04070', fg: '#ffe8f4', codigo: 'HECHIZO15',  tipo: 'descuento' },
    { label: 'Envío\ngratis', bg: '#5a3888', fg: '#e8d8ff', codigo: 'HECHIZOENV', tipo: 'envio'     },
    { label: '10% OFF',          bg: '#7b3fa0', fg: '#f8e8ff', codigo: 'HECHIZO10',  tipo: 'descuento' },
    { label: '20% OFF',          bg: '#3d1f5a', fg: '#f0d8ff', codigo: 'HECHIZO20',  tipo: 'descuento' },
    { label: '¡Casi!\nSeguí', bg: '#e8d8f4', fg: '#8060a8', codigo: null,          tipo: 'nada'      },
    { label: '5% OFF',           bg: '#c070a0', fg: '#fff0f8', codigo: 'HECHIZO05',  tipo: 'descuento' },
  ];

  var N   = HB_SEG.length;
  var ARC = (2 * Math.PI) / N;
  var hbAng = 0, hbSpinning = false;

  // ── Canvas ────────────────────────────────────────────────────────────────────
  function hbDraw(ang) {
    var cv  = document.getElementById('hb-canvas');
    var ctx = cv.getContext('2d');
    var cx = 124, cy = 124, r = 120;
    ctx.clearRect(0, 0, 248, 248);

    HB_SEG.forEach(function (s, i) {
      var a0 = ang + i * ARC - Math.PI / 2;
      var a1 = a0 + ARC;
      ctx.beginPath(); ctx.moveTo(cx, cy);
      ctx.arc(cx, cy, r, a0, a1); ctx.closePath();
      ctx.fillStyle = s.bg; ctx.fill();
      ctx.strokeStyle = '#fdf8ff33'; ctx.lineWidth = 1.5; ctx.stroke();

      ctx.save(); ctx.translate(cx, cy); ctx.rotate(a0 + ARC / 2);
      ctx.textAlign = 'right'; ctx.fillStyle = s.fg;
      ctx.shadowColor = 'rgba(0,0,0,.35)'; ctx.shadowBlur = 3;
      var lines = s.label.split('\n');
      if (lines.length === 1) {
        ctx.font = "600 13px 'Cormorant Garamond', Georgia, serif";
        ctx.fillText(s.label, r - 10, 5);
      } else {
        ctx.font = "300 11px 'Jost', sans-serif";
        ctx.fillText(lines[0], r - 10, -3);
        ctx.fillText(lines[1], r - 10, 10);
      }
      ctx.restore();
    });

    ctx.beginPath(); ctx.arc(cx, cy, r, 0, 2 * Math.PI);
    ctx.strokeStyle = '#c8a0d8'; ctx.lineWidth = 3; ctx.stroke();

    HB_SEG.forEach(function (_, i) {
      var ta = ang + i * ARC - Math.PI / 2;
      ctx.beginPath();
      ctx.moveTo(cx + (r - 8) * Math.cos(ta), cy + (r - 8) * Math.sin(ta));
      ctx.lineTo(cx + (r + 1) * Math.cos(ta), cy + (r + 1) * Math.sin(ta));
      ctx.strokeStyle = '#e0b8f0'; ctx.lineWidth = 1.5; ctx.stroke();
    });
  }

  // ── Spin ──────────────────────────────────────────────────────────────────────
  function hbGirar() {
    if (hbSpinning) return;
    hbSpinning = true;
    document.getElementById('hb-spin-btn').disabled = true;

    var winner    = Math.floor(Math.random() * N);
    var extra     = 5 + Math.random() * 3;
    var angWinner = (2 * Math.PI / N) * winner;
    var angFinal  = hbAng + extra * 2 * Math.PI - angWinner;
    var dur = 4400, t0 = performance.now(), ang0 = hbAng;

    (function frame(t) {
      var p    = Math.min((t - t0) / dur, 1);
      var ease = 1 - Math.pow(1 - p, 3.5);
      hbAng = ang0 + (angFinal - ang0) * ease;
      hbDraw(hbAng);
      if (p < 1) {
        requestAnimationFrame(frame);
      } else {
        hbAng = angFinal % (2 * Math.PI);
        hbSpinning = false;
        hbShowResult(HB_SEG[winner]);
        hbSetCookie(COOKIE, '1', 365);
      }
    })(t0);
  }

  // ── Result ────────────────────────────────────────────────────────────────────
  function hbShowResult(seg) {
    var res  = document.getElementById('hb-result');
    var spin = document.getElementById('hb-spin-section');
    var html = '';

    if (seg.tipo === 'nada') {
      html =
        '<div class="hb-divider" style="margin-top:8px"><span>✦</span></div>' +
        '<div class="res-emoji">🌙</div>' +
        '<h2 class="res-title">¡Casi!</h2>' +
        '<div class="hb-consolation">' +
          '<p>Esta vez la suerte no acompañó, pero hay magia esperándote en la colección.<br><br>' +
          '<strong>Suscribíte al newsletter</strong> y recibí descuentos exclusivos antes que nadie 💌</p>' +
        '</div>' +
        '<p class="hb-legal">Gracias por participar ✦ hechizo.com.ar</p>';
    } else if (seg.tipo === 'envio') {
      html =
        '<div class="hb-divider" style="margin-top:8px"><span>✦</span></div>' +
        '<div class="res-emoji">✨</div>' +
        '<h2 class="res-title">¡Ganaste!</h2>' +
        '<div class="res-prize">ENVÍO GRATIS</div>' +
        '<p class="res-desc">En tu próxima compra, el envío corre<br>completamente por nuestra cuenta.</p>' +
        hbCuponHTML(seg.codigo) +
        '<p class="hb-legal">Válido 7 días · Un solo uso · No acumulable</p>';
    } else {
      html =
        '<div class="hb-divider" style="margin-top:8px"><span>✦</span></div>' +
        '<div class="res-emoji">🎉</div>' +
        '<h2 class="res-title">¡Tu regalo está listo!</h2>' +
        '<div class="res-prize">' + seg.label + '</div>' +
        '<p class="res-desc">Usá este cupón al finalizar tu compra<br>y el descuento se aplica automáticamente.</p>' +
        hbCuponHTML(seg.codigo) +
        '<p class="hb-legal">Válido 7 días · Un solo uso · No acumulable</p>';
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
    navigator.clipboard.writeText(c).then(function () {
      var b = document.getElementById('hb-copy-btn');
      if (b) { b.textContent = '✓ ¡Copiado!'; setTimeout(function () { b.textContent = 'Copiar cupón'; }, 2000); }
    });
  }

  function hbSetCookie(n, v, d) {
    var e = new Date(); e.setTime(e.getTime() + d * 864e5);
    document.cookie = n + '=' + v + ';expires=' + e.toUTCString() + ';path=/';
  }

  // ── Event listeners ───────────────────────────────────────────────────────────
  document.getElementById('hb-close').addEventListener('click', function () {
    document.getElementById('hb-overlay').style.display = 'none';
  });
  document.getElementById('hb-spin-btn').addEventListener('click', hbGirar);

  // Expose for inline onclicks in dynamically generated result HTML
  window.hbCopiar = hbCopiar;

  // ── Init ──────────────────────────────────────────────────────────────────────
  hbDraw(0);
  setTimeout(function () {
    document.getElementById('hb-overlay').style.display = 'flex';
  }, 2500);

})();
