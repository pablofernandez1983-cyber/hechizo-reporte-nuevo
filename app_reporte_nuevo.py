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


@app.route("/stock-debug")
def stock_debug():
    """Diagnóstico: chequea estado de ventas_detalle y ventas."""
    import traceback as _tb
    out = {"ok": True}
    if not DATABASE_URL:
        return jsonify({"ok": False, "error": "DATABASE_URL no configurada"}), 500
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cur = conn.cursor()

        # 1. ¿Existe la tabla?
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'ventas_detalle'
            )
        """)
        out["tabla_existe"] = cur.fetchone()[0]

        # 2. ¿Cuántas filas?
        if out["tabla_existe"]:
            cur.execute("SELECT COUNT(*) FROM ventas_detalle")
            out["total_filas"] = cur.fetchone()[0]

            # 3. Muestra de hasta 5 filas
            cur.execute("SELECT * FROM ventas_detalle ORDER BY id DESC LIMIT 5")
            cols = [d[0] for d in cur.description]
            out["muestra"] = [dict(zip(cols, r)) for r in cur.fetchall()]

            # 4. Rango de fechas en ventas_detalle (via join)
            cur.execute("""
                SELECT MIN(v.fecha), MAX(v.fecha), COUNT(DISTINCT vd.orden_id)
                FROM ventas_detalle vd
                JOIN ventas v ON v.orden_id = vd.orden_id
            """)
            row = cur.fetchone()
            out["fecha_min"] = str(row[0]) if row[0] else None
            out["fecha_max"] = str(row[1]) if row[1] else None
            out["ordenes_distintas"] = row[2]
        else:
            out["total_filas"] = 0
            out["muestra"] = []

        # 5. Cuántas ventas hay en tabla ventas (últimos 90 días)
        cur.execute("""
            SELECT COUNT(*) FROM ventas
            WHERE fecha >= (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date - INTERVAL '90 days'
              AND estado_pago IN ('paid', 'authorized')
        """)
        out["ventas_90d"] = cur.fetchone()[0]

        # 6. Columnas de ventas_detalle
        if out["tabla_existe"]:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'ventas_detalle'
                ORDER BY ordinal_position
            """)
            out["columnas"] = [{"col": r[0], "tipo": r[1]} for r in cur.fetchall()]

        cur.close(); conn.close()
    except Exception as e:
        out["error"] = str(e)
        out["trace"] = _tb.format_exc()
    return jsonify(out)


@app.route("/stock-data")
def stock_data():
    """Devuelve los datos de stock como JSON para el dashboard."""
    import traceback as _tb
    try:
        result = _stock_compute()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trace": _tb.format_exc()}), 500


@app.route("/stock")
def stock():
    import traceback as _tb
    try:
        return _stock_render()
    except Exception as e:
        return "<pre style='color:#f87171;padding:20px'>Error:\n" + _tb.format_exc() + "</pre>", 500


def _cob_color(cob):
    if cob is None: return ""
    if cob < 7:  return "red"
    if cob < 14: return "orange"
    if cob < 21: return "yellow"
    return "ok"


def _cob_badge(cob):
    if cob is None: return "&mdash;"
    txt = str(int(round(cob))) + "d"
    colors = {"red": "#f87171", "orange": "#fb923c", "yellow": "#fbbf24"}
    c = _cob_color(cob)
    if c in colors:
        return '<span style="color:' + colors[c] + ';font-weight:600">' + txt + '</span>'
    return txt


def _stock_rows(items, idx_start=0):
    bgs = {"red": "rgba(248,113,113,0.08)", "orange": "rgba(251,146,60,0.06)",
           "yellow": "rgba(251,191,36,0.05)"}
    out = []
    for i, v in enumerate(items):
        c    = _cob_color(v["cobertura"])
        bg   = (' style="background:' + bgs[c] + '"') if c in bgs else ""
        srch = (v["nombre"] + " " + v["variante"] + " " + v["sku"]).lower()
        var_span = (' <span class="var">' + v["variante"] + "</span>") if v["variante"] else ""
        sku_span = ('<span class="sku-sub">' + v["sku"] + "</span>") if v["sku"] else ""
        row = (
            "<tr" + bg
            + ' data-search="' + srch + '"'
            + ' data-cob-color="' + c + '">'
            + '<td class="num">' + str(idx_start + i + 1) + "</td>"
            + "<td>" + v["nombre"] + var_span + sku_span + "</td>"
            + '<td class="num">' + str(v["stock"]) + "</td>"
            + '<td class="num">' + str(v["unidades_90"]) + "</td>"
            + '<td class="num">' + _cob_badge(v["cobertura"]) + "</td>"
            + "</tr>"
        )
        out.append(row)
    return "".join(out)


_STOCK_TH = (
    "<thead><tr>"
    '<th class="num" style="width:36px">#</th>'
    '<th class="sortable" onclick="sortTable(this,1)">Producto</th>'
    '<th class="sortable num" onclick="sortTable(this,2)" style="width:72px">Stock</th>'
    '<th class="sortable num" onclick="sortTable(this,3)" style="width:106px">'
    '<span style="font-size:10px;line-height:1.4">Ventas<br>ult. 90d</span></th>'
    '<th class="sortable num" onclick="sortTable(this,4)" style="width:88px">Cobertura</th>'
    "</tr></thead>"
)


def _stock_compute():
    """Calcula datos de stock y los devuelve como dict (reutilizado por JSON y HTML)."""
    errores = []

    # ── 1. Ventas últimos 90 días desde Supabase ──────────────────────────
    ventas_90 = {}
    if DATABASE_URL:
        try:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            cur  = conn.cursor()
            cur.execute("""
                SELECT vd.variante_id, SUM(vd.cantidad) AS unidades
                FROM ventas_detalle vd
                JOIN ventas v ON v.orden_id = vd.orden_id
                WHERE v.fecha >= (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                      - INTERVAL '90 days'
                GROUP BY vd.variante_id
            """)
            for vid, uni in cur.fetchall():
                ventas_90[int(vid)] = int(uni)
            cur.close(); conn.close()
        except Exception as e:
            errores.append("DB: " + str(e))

    # ── 2. Stock actual desde TiendaNube ─────────────────────────────────
    variantes = []
    if TN_STORE_ID and TN_TOKEN:
        base = "https://api.tiendanube.com/v1/" + TN_STORE_ID
        page = 1
        while True:
            try:
                r = requests.get(base + "/products", headers=TN_HEADERS,
                                 params={"page": page, "per_page": 200}, timeout=20)
                r.raise_for_status()
                batch = r.json()
            except Exception as e:
                errores.append("TN pag " + str(page) + ": " + str(e))
                break
            if not batch: break
            for prod in batch:
                nombre_prod = ""
                for lang in ("es", "en", "pt"):
                    nombre_prod = ((prod.get("name") or {}).get(lang) or "") or nombre_prod
                for var in (prod.get("variants") or []):
                    vid = var.get("id")
                    if not vid: continue
                    stock_val = var.get("stock")
                    if stock_val is None: continue
                    vals = var.get("values") or []
                    var_str = " / ".join(
                        str(val.get("es") or val.get("en") or val.get("pt") or "")
                        for val in vals if isinstance(val, dict)
                    )
                    precio_venta = float(var.get("promotional_price") or var.get("price") or 0)
                    variantes.append({
                        "variante_id":  int(vid),
                        "nombre":       nombre_prod,
                        "variante":     var_str,
                        "sku":          var.get("sku") or "",
                        "stock":        int(stock_val),
                        "precio_venta": precio_venta,
                    })
            if len(batch) < 200: break
            page += 1
    else:
        errores.append("Credenciales TiendaNube no configuradas")

    # ── 3. Cruzar y calcular métricas ─────────────────────────────────────
    for v in variantes:
        u90 = ventas_90.get(v["variante_id"], 0)
        vel = u90 / 90
        v["unidades_90"] = u90
        v["cobertura"]   = round(v["stock"] / vel, 1) if vel > 0 else None

    variantes.sort(key=lambda v: (
        0 if v["cobertura"] is not None else 1,
        v["cobertura"] if v["cobertura"] is not None else 0,
        -v["unidades_90"]
    ))

    alertas = [v for v in variantes if v["cobertura"] is not None and v["cobertura"] < 21]
    top20   = sorted(variantes, key=lambda v: -v["unidades_90"])[:20]

    valuacion_total = sum(v["stock"] * v["precio_venta"] for v in variantes)

    return {
        "ok":             True,
        "generado_en":    datetime.now(TZ_AR).strftime("%d/%m/%Y %H:%M"),
        "total":          len(variantes),
        "errores":        errores,
        "variantes":      variantes,
        "alertas":        alertas,
        "top20":          top20,
        "valuacion_total": valuacion_total,
    }


def _stock_render():
    d         = _stock_compute()
    errores   = d["errores"]
    variantes = d["variantes"]
    alertas   = d["alertas"]
    top20     = d["top20"]
    total_v   = d["total"]
    n_alerta  = len(alertas)
    n_red     = sum(1 for v in alertas if _cob_color(v["cobertura"]) == "red")
    n_orange  = sum(1 for v in alertas if _cob_color(v["cobertura"]) == "orange")
    n_yellow  = sum(1 for v in alertas if _cob_color(v["cobertura"]) == "yellow")
    gen_en    = d["generado_en"]
    val_total = d.get("valuacion_total", 0)

    # Formatear valuación estilo AR: $ 1.234.567
    val_str = "$ " + "{:,.0f}".format(val_total).replace(",", ".")

    err_html = "".join(
        '<div class="error-msg">&#9888; ' + e + '</div>' for e in errores
    )

    val_card = (
        '<div class="val-card">'
        '<div class="val-label">Valuaci&#243;n del stock</div>'
        '<div class="val-amount">' + val_str + '</div>'
        '<div class="val-sub">precio de venta &times; stock actual</div>'
        '</div>'
    )

    chips = (
        '<div class="stat-chips">'
        '<div class="chip chip-filter active" data-filter="" onclick="setChip(this)">'
        '<span>' + str(total_v) + '</span>todas las variantes</div>'
        '<div class="chip chip-filter" data-filter="red" onclick="setChip(this)">'
        '<span class="red">' + str(n_red) + '</span>cobertura &lt;7d</div>'
        '<div class="chip chip-filter" data-filter="orange" onclick="setChip(this)">'
        '<span class="orange">' + str(n_orange) + '</span>cobertura 7-14d</div>'
        '<div class="chip chip-filter" data-filter="yellow" onclick="setChip(this)">'
        '<span class="yellow">' + str(n_yellow) + '</span>cobertura 14-21d</div>'
        '</div>'
    )

    alerta_sec = ""
    if alertas:
        alerta_sec = (
            '<div class="section">'
            '<div class="section-title">&#128680; Alertas &mdash; cobertura &lt; 21 dias ('
            + str(n_alerta) + ' variantes)</div>'
            + '<table>' + _STOCK_TH + '<tbody>' + _stock_rows(alertas) + '</tbody></table>'
            + '</div>'
        )

    top20_sec = (
        '<div class="section">'
        '<div class="section-title">&#128200; Top 20 mas vendidos &mdash; ultimos 90 dias</div>'
        + '<table>' + _STOCK_TH + '<tbody>' + _stock_rows(top20) + '</tbody></table>'
        + '</div>'
    )

    tabla_sec = (
        '<div class="section">'
        '<div class="section-title">&#128230; Todas las variantes (' + str(total_v) + ')</div>'
        '<div class="search-wrap"><input type="text" id="srch" placeholder="Buscar nombre, variante o SKU..." oninput="aplicarFiltros()"></div>'
        + '<table id="tc">' + _STOCK_TH + '<tbody id="tb">' + _stock_rows(variantes) + '</tbody></table>'
        + '</div>'
    )

    css = """
:root{--bg:#0f1117;--card:#1a1d27;--border:#2a2d3a;--text:#e2e8f0;--muted:#8b92a5;--success:#34d399}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,sans-serif;font-size:14px;padding:20px}
h1{font-size:20px;font-weight:700;margin-bottom:4px}
.meta{color:var(--muted);font-size:12px;margin-bottom:16px}
a{color:var(--success);text-decoration:none}
.section{margin-bottom:32px}
.section-title{font-size:15px;font-weight:600;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.error-msg{background:rgba(248,113,113,.12);border:1px solid #f87171;color:#f87171;padding:8px 12px;border-radius:6px;margin-bottom:8px;font-size:12px}
table{width:100%;border-collapse:collapse}
th,td{padding:8px 10px;text-align:left;border-bottom:1px solid var(--border)}
th{color:var(--muted);font-size:12px;font-weight:500;background:var(--card);position:sticky;top:0;z-index:1}
tr:hover{background:rgba(255,255,255,.03)}
td.num,th.num{text-align:right}
.var{color:var(--muted);font-size:12px}
.sku-sub{display:block;font-size:11px;color:var(--muted);font-family:monospace;margin-top:2px}
.sortable{cursor:pointer;user-select:none}
.sortable:hover{color:var(--text)}
.search-wrap{margin-bottom:12px}
.search-wrap input{width:100%;max-width:400px;background:var(--card);border:1px solid var(--border);color:var(--text);padding:8px 12px;border-radius:6px;font-size:14px;outline:none}
.search-wrap input:focus{border-color:var(--success)}
.stat-chips{display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap}
.chip{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:8px 14px;font-size:13px}
.chip span{font-weight:700;font-size:16px;display:block}
.chip .red{color:#f87171}.chip .orange{color:#fb923c}.chip .yellow{color:#fbbf24}
.chip-filter{cursor:pointer;transition:border-color .15s}
.chip-filter:hover{border-color:#4b5563}
.chip-filter.active{border-color:var(--success);box-shadow:0 0 0 1px var(--success)}
.val-card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px 20px;margin-bottom:20px;display:inline-block;min-width:210px}
.val-label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px}
.val-amount{font-size:26px;font-weight:700;color:var(--success)}
.val-sub{font-size:11px;color:var(--muted);margin-top:3px}
"""

    js = """
var _chipFilter='';
function setChip(el){
  document.querySelectorAll('.chip-filter').forEach(function(c){c.classList.remove('active');});
  el.classList.add('active');
  _chipFilter=el.dataset.filter;
  aplicarFiltros();
}
function aplicarFiltros(){
  var q=(document.getElementById('srch').value||'').toLowerCase().trim();
  document.querySelectorAll('#tb tr').forEach(function(tr){
    var matchText=!q||tr.dataset.search.includes(q);
    var matchChip=!_chipFilter||tr.dataset.cobColor===_chipFilter;
    tr.style.display=(matchText&&matchChip)?'':'none';
  });
}
function sortTable(th,col){
  var tbody=document.getElementById('tb');
  var rows=Array.from(tbody.querySelectorAll('tr'));
  var asc=th.dataset.asc!=='1';
  th.dataset.asc=asc?'1':'0';
  rows.sort(function(a,b){
    var av=a.cells[col].textContent.replace('d','').trim();
    var bv=b.cells[col].textContent.replace('d','').trim();
    var an=parseFloat(av),bn=parseFloat(bv);
    if(!isNaN(an)&&!isNaN(bn))return asc?an-bn:bn-an;
    return asc?av.localeCompare(bv):bv.localeCompare(av);
  });
  rows.forEach(function(r){tbody.appendChild(r);});
}
"""

    return (
        '<!DOCTYPE html><html lang="es"><head>'
        '<meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<title>Stock - Hechizo</title>'
        "<style>" + css + "</style>"
        "</head><body>"
        "<h1>Stock &mdash; Hechizo Bijou</h1>"
        '<div class="meta">Generado ' + gen_en + ' &nbsp;&middot;&nbsp; <a href="/">&#8592; Volver al reporte</a></div>'
        + err_html
        + val_card
        + chips
        + alerta_sec
        + top20_sec
        + tabla_sec
        + "<script>" + js + "</script>"
        "</body></html>"
    )


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "msg": "hechizo-reporte-nuevo running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
