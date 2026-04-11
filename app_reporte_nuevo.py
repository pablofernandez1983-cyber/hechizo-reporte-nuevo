"""
app_reporte_nuevo.py — Flask wrapper para Railway
Expone reporte_nuevo.py como endpoint HTTP.
"""

import os, sys, threading, io, requests
from collections import deque
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
from reporte_nuevo import main as ejecutar_reporte

DATABASE_URL = os.environ.get("DATABASE_URL", "")
TN_STORE_ID  = os.environ.get("TIENDANUBE_STORE_ID", "")
TN_TOKEN     = os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")
TN_HEADERS   = {
    "Authentication": f"bearer {TN_TOKEN}",
    "User-Agent": "HechizoBijou-Stock/1.0 (hechizobijou@gmail.com)"
}

app = Flask(__name__)
CORS(app)

TZ_AR = timezone(timedelta(hours=-3))

_estado = {
    "corriendo": False,
    "ultimo_inicio": None,
    "ultimo_fin": None,
    "resultado": None,
    "error": None,
}

_log_buffer = deque(maxlen=200)  # últimas 200 líneas


class _LogCapture(io.TextIOBase):
    """Stream que escribe en stdout Y en el buffer de logs."""
    def __init__(self, original):
        self._original = original

    def write(self, text):
        self._original.write(text)
        self._original.flush()
        line = text.rstrip("\n")
        if line:
            _log_buffer.append(line)
        return len(text)

    def flush(self):
        self._original.flush()


def _run():
    _estado["corriendo"] = True
    _estado["ultimo_inicio"] = datetime.now(TZ_AR).isoformat()
    _estado["error"] = None
    _log_buffer.clear()

    original_stdout = sys.stdout
    sys.stdout = _LogCapture(original_stdout)
    try:
        ejecutar_reporte()
        _estado["resultado"] = "OK"
    except Exception as e:
        _estado["resultado"] = "ERROR"
        _estado["error"] = str(e)
    finally:
        sys.stdout = original_stdout
        _estado["corriendo"] = False
        _estado["ultimo_fin"] = datetime.now(TZ_AR).isoformat()


@app.route("/ejecutar", methods=["POST"])
def ejecutar():
    if _estado["corriendo"]:
        return jsonify({"ok": False, "msg": "Ya está corriendo un reporte"}), 409
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "msg": "Reporte iniciado"})


@app.route("/estado")
def ver_estado():
    return jsonify({
        "ok": True,
        "corriendo": _estado["corriendo"],
        "ultimo_inicio": _estado["ultimo_inicio"],
        "ultimo_fin": _estado["ultimo_fin"],
        "resultado": _estado["resultado"],
        "error": _estado["error"],
    })


@app.route("/logs")
def ver_logs():
    desde = request.args.get("desde", 0, type=int)
    lineas = list(_log_buffer)
    nuevas = lineas[desde:]
    return jsonify({
        "ok": True,
        "total": len(lineas),
        "lineas": nuevas,
    })


@app.route("/historico")
def historico():
    """Devuelve ventas agrupadas por día para los últimos 45 días desde Supabase."""
    if not DATABASE_URL:
        return jsonify({"ok": False, "error": "DATABASE_URL no configurada"}), 500
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("""
            SELECT fecha,
                   ROUND(SUM(total)::numeric, 0) AS total,
                   COUNT(orden_id) AS cantidad
            FROM ventas
            WHERE fecha >= (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date - INTERVAL '45 days'
              AND estado_pago IN ('paid', 'authorized')
            GROUP BY fecha
            ORDER BY fecha
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        dias = [
            {"fecha": str(r[0]), "total": int(r[1]), "cantidad": int(r[2])}
            for r in rows
        ]
        return jsonify({"ok": True, "dias": dias})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/historico-mensual")
def historico_mensual():
    """Devuelve cantidad de ventas agrupada por mes (anio, mes, cantidad)."""
    if not DATABASE_URL:
        return jsonify({"ok": False, "error": "DATABASE_URL no configurada"}), 500
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("""
            SELECT EXTRACT(YEAR FROM fecha)::int  AS anio,
                   EXTRACT(MONTH FROM fecha)::int AS mes,
                   COUNT(orden_id)                AS cantidad
            FROM ventas
            WHERE estado_pago IN ('paid', 'authorized')
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        meses = [
            {"anio": r[0], "mes": r[1], "cantidad": int(r[2])}
            for r in rows
        ]
        return jsonify({"ok": True, "meses": meses})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/stock")
def stock():
    errores = []

    # ── 1. Ventas últimos 90 días desde Supabase ──────────────────────────
    ventas_90 = {}  # variante_id -> unidades
    if DATABASE_URL:
        try:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            cur = conn.cursor()
            cur.execute("""
                SELECT vd.variante_id, SUM(vd.cantidad) AS unidades
                FROM ventas_detalle vd
                JOIN ventas v ON v.orden_id = vd.orden_id
                WHERE v.fecha >= (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date - INTERVAL '90 days'
                GROUP BY vd.variante_id
            """)
            for vid, uni in cur.fetchall():
                ventas_90[int(vid)] = int(uni)
            cur.close()
            conn.close()
        except Exception as e:
            errores.append(f"DB: {e}")

    # ── 2. Stock actual desde TiendaNube ─────────────────────────────────
    variantes = []  # lista de dicts con info de cada variante
    if TN_STORE_ID and TN_TOKEN:
        base = f"https://api.tiendanube.com/v1/{TN_STORE_ID}"
        page = 1
        while True:
            try:
                r = requests.get(f"{base}/products", headers=TN_HEADERS,
                                 params={"page": page, "per_page": 200}, timeout=20)
                r.raise_for_status()
                batch = r.json()
            except Exception as e:
                errores.append(f"TN pág {page}: {e}")
                break
            if not batch: break
            for prod in batch:
                nombre_prod = ""
                for lang in ("es", "en", "pt"):
                    nombre_prod = (prod.get("name") or {}).get(lang, "") or nombre_prod
                for v in (prod.get("variants") or []):
                    vid = v.get("id")
                    if not vid: continue
                    stock_val = v.get("stock")
                    if stock_val is None: continue  # sin control de stock
                    vals = v.get("values") or []
                    variante_str = " / ".join(
                        str(val.get("es") or val.get("en") or val.get("pt") or "")
                        for val in vals if isinstance(val, dict)
                    )
                    variantes.append({
                        "variante_id": int(vid),
                        "nombre": nombre_prod,
                        "variante": variante_str,
                        "sku": v.get("sku") or "",
                        "stock": int(stock_val),
                    })
            if len(batch) < 200: break
            page += 1
    else:
        errores.append("Credenciales TiendaNube no configuradas")

    # ── 3. Cruzar y calcular métricas ─────────────────────────────────────
    for v in variantes:
        vid = v["variante_id"]
        unidades_90 = ventas_90.get(vid, 0)
        vel = unidades_90 / 90
        v["unidades_90"] = unidades_90
        v["velocidad"]   = round(vel, 2)
        v["cobertura"]   = round(v["stock"] / vel, 1) if vel > 0 else None

    # Ordenar por cobertura asc (None al final), luego por unidades desc
    def sort_key(v):
        cob = v["cobertura"]
        return (0 if cob is not None else 1, cob if cob is not None else 0, -v["unidades_90"])

    variantes.sort(key=sort_key)

    alertas  = [v for v in variantes if v["cobertura"] is not None and v["cobertura"] < 21]
    top20    = sorted(variantes, key=lambda v: -v["unidades_90"])[:20]

    def color_cobertura(cob):
        if cob is None: return ""
        if cob < 7:  return "red"
        if cob < 14: return "orange"
        if cob < 21: return "yellow"
        return "ok"

    def fmt_cob(cob):
        if cob is None: return "—"
        return f"{cob:.0f}d"

    def badge(cob):
        c = color_cobertura(cob)
        if not c or c == "ok": return fmt_cob(cob)
        colors = {"red": "#f87171", "orange": "#fb923c", "yellow": "#fbbf24"}
        return f'<span style="color:{colors[c]};font-weight:600">{fmt_cob(cob)}</span>'

    def rows_html(items, idx_start=0):
        out = []
        for i, v in enumerate(items):
            c = color_cobertura(v["cobertura"])
            bg = {"red": "rgba(248,113,113,0.08)", "orange": "rgba(251,146,60,0.06)",
                  "yellow": "rgba(251,191,36,0.05)"}.get(c, "")
            bg_style = f'style="background:{bg}"' if bg else ""
            out.append(f"""
              <tr {bg_style} data-search="{v['nombre'].lower()} {v['variante'].lower()} {v['sku'].lower()}">
                <td class="num">{idx_start + i + 1}</td>
                <td>{v['nombre']}{f" <span class='var'>{v['variante']}</span>" if v['variante'] else ""}</td>
                <td class="mono">{v['sku']}</td>
                <td class="num">{v['stock']}</td>
                <td class="num">{v['unidades_90']}</td>
                <td class="num">{badge(v['cobertura'])}</td>
              </tr>""")
        return "".join(out)

    alertas_html = rows_html(alertas)
    top20_html   = rows_html(top20)
    tabla_html   = rows_html(variantes)
    errores_html = "".join(f'<div class="error-msg">⚠ {e}</div>' for e in errores)
    total_vars   = len(variantes)
    total_alerta = len(alertas)
    generado_en  = datetime.now(TZ_AR).strftime("%d/%m/%Y %H:%M")

    TABLE_HEADERS = """
      <thead><tr>
        <th class="num">#</th>
        <th onclick="sortTable(this,1)" class="sortable">Producto ▲▼</th>
        <th onclick="sortTable(this,2)" class="sortable">SKU ▲▼</th>
        <th onclick="sortTable(this,3)" class="sortable num">Stock ▲▼</th>
        <th onclick="sortTable(this,4)" class="sortable num">Vendidos 90d ▲▼</th>
        <th onclick="sortTable(this,5)" class="sortable num">Cobertura ▲▼</th>
      </tr></thead>"""

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Stock — Hechizo</title>
<style>
  :root {{
    --bg:#0f1117; --card:#1a1d27; --border:#2a2d3a;
    --text:#e2e8f0; --muted:#8b92a5; --success:#34d399;
  }}
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ background:var(--bg); color:var(--text); font-family:system-ui,-apple-system,sans-serif;
          font-size:14px; padding:20px; }}
  h1 {{ font-size:20px; font-weight:700; margin-bottom:4px; }}
  .meta {{ color:var(--muted); font-size:12px; margin-bottom:20px; }}
  a {{ color:var(--success); text-decoration:none; }}
  .section {{ margin-bottom:32px; }}
  .section-title {{ font-size:15px; font-weight:600; margin-bottom:12px;
                    padding-bottom:8px; border-bottom:1px solid var(--border); }}
  .error-msg {{ background:rgba(248,113,113,0.12); border:1px solid #f87171;
                color:#f87171; padding:8px 12px; border-radius:6px; margin-bottom:8px; font-size:12px; }}
  table {{ width:100%; border-collapse:collapse; }}
  th, td {{ padding:8px 10px; text-align:left; border-bottom:1px solid var(--border); }}
  th {{ color:var(--muted); font-size:12px; font-weight:500; background:var(--card);
        position:sticky; top:0; z-index:1; }}
  tr:hover {{ background:rgba(255,255,255,0.03); }}
  td.num, th.num {{ text-align:right; }}
  .var {{ color:var(--muted); font-size:12px; }}
  .mono {{ font-family:monospace; font-size:12px; color:var(--muted); }}
  .sortable {{ cursor:pointer; user-select:none; }}
  .sortable:hover {{ color:var(--text); }}
  .search-wrap {{ margin-bottom:12px; }}
  .search-wrap input {{
    width:100%; max-width:400px; background:var(--card); border:1px solid var(--border);
    color:var(--text); padding:8px 12px; border-radius:6px; font-size:14px; outline:none;
  }}
  .search-wrap input:focus {{ border-color:var(--success); }}
  .stat-chips {{ display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap; }}
  .chip {{ background:var(--card); border:1px solid var(--border); border-radius:8px;
           padding:8px 14px; font-size:13px; }}
  .chip span {{ font-weight:700; font-size:16px; display:block; }}
  .chip .red   {{ color:#f87171; }}
  .chip .orange{{ color:#fb923c; }}
  .chip .yellow{{ color:#fbbf24; }}
  .chip .ok    {{ color:var(--success); }}
</style>
</head>
<body>
<h1>📦 Reporte de Stock</h1>
<div class="meta">Generado {generado_en} · <a href="/">← Volver al reporte</a></div>
{errores_html}

<div class="stat-chips">
  <div class="chip"><span>{total_vars}</span>variantes activas</div>
  <div class="chip"><span class="red">{len([v for v in alertas if color_cobertura(v['cobertura'])=='red'])}</span>cobertura &lt;7d 🚨</div>
  <div class="chip"><span class="orange">{len([v for v in alertas if color_cobertura(v['cobertura'])=='orange'])}</span>cobertura 7-14d</div>
  <div class="chip"><span class="yellow">{len([v for v in alertas if color_cobertura(v['cobertura'])=='yellow'])}</span>cobertura 14-21d</div>
</div>

{"" if not alertas else f'''
<div class="section">
  <div class="section-title">🚨 Alertas — cobertura &lt; 21 días ({total_alerta} variantes)</div>
  <table>{TABLE_HEADERS}<tbody>{alertas_html}</tbody></table>
</div>
'''}

<div class="section">
  <div class="section-title">📈 Top 20 más vendidos — últimos 90 días</div>
  <table>{TABLE_HEADERS}<tbody>{top20_html}</tbody></table>
</div>

<div class="section">
  <div class="section-title">📦 Todas las variantes ({total_vars})</div>
  <div class="search-wrap">
    <input type="text" id="search-input" placeholder="Buscar por nombre, variante o SKU..."
           oninput="filtrar(this.value)">
  </div>
  <table id="tabla-completa">{TABLE_HEADERS}<tbody id="tabla-body">{tabla_html}</tbody></table>
</div>

<script>
function filtrar(q) {{
  q = q.toLowerCase().trim();
  document.querySelectorAll('#tabla-body tr').forEach(tr => {{
    tr.style.display = (!q || tr.dataset.search.includes(q)) ? '' : 'none';
  }});
}}

function sortTable(th, col) {{
  const tbody = document.getElementById('tabla-body');
  const rows  = Array.from(tbody.querySelectorAll('tr'));
  const asc   = th.dataset.asc !== '1';
  th.dataset.asc = asc ? '1' : '0';
  rows.sort((a, b) => {{
    const av = a.cells[col].textContent.replace('d','').trim();
    const bv = b.cells[col].textContent.replace('d','').trim();
    const an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  }});
  rows.forEach(r => tbody.appendChild(r));
}}
</script>
</body>
</html>"""
    return html


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "msg": "hechizo-reporte-nuevo running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
