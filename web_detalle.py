"""
web_detalle.py — Endpoints de solo lectura para la versión web del reporte (hechizo-app/web.html).

Todo lo que está acá exige el header X-Web-Key == WEB_PASSWORD (variable de entorno).
Cada rubro del P&L se abre con la MISMA lógica con la que lo calcula reporte_nuevo.py,
así el detalle suma lo mismo que el total; si no cierra, el frontend muestra la diferencia.
"""

import os, re, hmac, time
from datetime import date, datetime, timedelta
from decimal import Decimal
from flask import Blueprint, jsonify, request

import reporte_nuevo as rn

web_bp = Blueprint("web", __name__, url_prefix="/web")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
WEB_PASSWORD = os.environ.get("WEB_PASSWORD", "")

PAGADAS = "estado_pago IN ('paid', 'authorized')"

# Agrupa los nombres de medio de pago que TN fue cambiando con los años (Getnet = antecesor de PagoNube).
# Lleva %% porque todas las queries se ejecutan con parámetros.
FORMA_SQL = """CASE
    WHEN {c} ILIKE '%%mercado pago%%' THEN 'Mercado Pago'
    WHEN {c} ILIKE '%%pago nube%%' OR {c} ILIKE '%%getnet%%' THEN 'Pago Nube / Getnet'
    WHEN {c} ILIKE '%%transfer%%' OR {c} ILIKE '%%dep_sito%%' THEN 'Transferencia'
    ELSE 'Otros' END"""

def _forma(col="medio_pago"):
    return FORMA_SQL.format(c=col)


# ═══════════════════════════════════════════════════════════════
# AUTH + HELPERS
# ═══════════════════════════════════════════════════════════════

@web_bp.before_request
def _auth():
    if request.method == "OPTIONS":
        return None
    if not WEB_PASSWORD:
        return jsonify({"ok": False, "error": "WEB_PASSWORD no configurada"}), 503
    key = request.headers.get("X-Web-Key", "")
    if not hmac.compare_digest(key.encode(), WEB_PASSWORD.encode()):
        time.sleep(0.5)
        return jsonify({"ok": False, "error": "Clave incorrecta"}), 401
    return None


def _json_val(v):
    if isinstance(v, Decimal): return float(v)
    if isinstance(v, (date, datetime)): return v.isoformat()
    return v


def _q(sql, params=None):
    import psycopg2, psycopg2.extras
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or {})
            return [{k: _json_val(v) for k, v in r.items()} for r in cur.fetchall()]
    finally:
        conn.close()


_cache = {}
CACHE_SEG = 300

def _cacheado(clave, fn):
    """Cachea lecturas de Sheets/S3 5 min (?fresh=1 lo saltea)."""
    ahora = time.time()
    if request.args.get("fresh") != "1" and clave in _cache and ahora - _cache[clave][0] < CACHE_SEG:
        return _cache[clave][1]
    val = fn()
    _cache[clave] = (ahora, val)
    return val


def _periodo():
    """desde/hasta como YYYY-MM (inclusive). Devuelve (k_desde, k_hasta) como tuplas."""
    def parse(s, default):
        m = re.match(r"^(\d{4})-(\d{1,2})$", s or "")
        return (int(m.group(1)), int(m.group(2))) if m else default
    hoy = rn.ahora_ar()
    d = parse(request.args.get("desde"), (hoy.year, 1))
    h = parse(request.args.get("hasta"), (hoy.year, hoy.month))
    return d, h


def _en(k, d, h):
    return d <= tuple(k) <= h


def _km(k):
    return f"{k[0]}-{k[1]:02d}"


def _fecha_txt(f, k):
    """Fecha legible (ISO cuando se puede) para filas de Sheets, que vienen en formatos mezclados."""
    if isinstance(f, (int, float)) or re.match(r"^\d{5}(\.\d+)?$", str(f).strip()):
        serial = float(f)
        if 30000 < serial < 70000:
            return (date(1899, 12, 30) + timedelta(days=int(serial))).isoformat()
    s = str(f).strip()
    m = re.match(r"^(\d{1,2})[-/ ]([a-záéíóú]+)\.?$", s, re.IGNORECASE)
    if m and k:
        try: return date(k[0], k[1], int(m.group(1))).isoformat()
        except ValueError: pass
    for ln, fmt in ((10, "%Y-%m-%d"), (10, "%d/%m/%Y"), (10, "%d-%m-%Y"), (8, "%d/%m/%y")):
        try: return datetime.strptime(s[:ln], fmt).date().isoformat()
        except ValueError: pass
    return s


def _sql_periodo(alias=""):
    p = f"{alias}." if alias else ""
    return f"({p}anio * 100 + {p}mes) BETWEEN %(d)s AND %(h)s"


def _params_periodo(d, h):
    return {"d": d[0] * 100 + d[1], "h": h[0] * 100 + h[1]}


# ═══════════════════════════════════════════════════════════════
# P&L
# ═══════════════════════════════════════════════════════════════

@web_bp.route("/check")
def check():
    return jsonify({"ok": True})


@web_bp.route("/pnl")
def pnl():
    try:
        rows = _q("SELECT anio, mes, rubro, monto FROM detalle_pnl ORDER BY anio, mes")
        act = _q("SELECT MAX(updated_at) AS t FROM pnl_mensual")
        valores = {}
        for r in rows:
            valores.setdefault(r["rubro"], {})[_km((r["anio"], r["mes"]))] = r["monto"]
        meses = sorted({_km((r["anio"], r["mes"])) for r in rows})
        return jsonify({
            "ok": True,
            "filas": [{"rubro": r, "desc": d, "cat": c} for r, d, c in rn.PNL_FILAS],
            "categorias_egreso": rn.CATEGORIAS_EGRESO,
            "meses": meses,
            "valores": valores,
            "actualizado": act[0]["t"] if act else None,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Columnas reutilizables ─────────────────────────────────────
C_VENTA = [
    {"key": "fecha", "label": "Fecha", "tipo": "fecha"},
    {"key": "orden_id", "label": "Orden", "tipo": "texto"},
    {"key": "cliente", "label": "Cliente", "tipo": "texto"},
    {"key": "tipo", "label": "Tipo", "tipo": "texto", "filtro": True},
    {"key": "forma", "label": "Forma de pago", "tipo": "texto", "filtro": True},
    {"key": "carrier", "label": "Envío", "tipo": "texto", "filtro": True},
    {"key": "subtotal", "label": "Subtotal", "tipo": "money"},
    {"key": "descuento", "label": "Descuento", "tipo": "money"},
    {"key": "envio_cobrado", "label": "Envío cobrado", "tipo": "money"},
    {"key": "total", "label": "Total", "tipo": "money"},
]
C_VALOR = {"key": "valor", "label": "Impacto P&L", "tipo": "money"}
C_SHEET = [
    {"key": "fecha", "label": "Fecha", "tipo": "fecha"},
    {"key": "detalle", "label": "Detalle", "tipo": "texto"},
    C_VALOR,
]


def _ventas_rubro(rubro, d, h):
    """Rubros que salen de las órdenes de Tiendanube (misma clasificación que fetch_tiendanube)."""
    exprs = {
        "ventas_min": ("subtotal", "tipo = 'minorista'"),
        "ventas_may": ("subtotal", "tipo = 'mayorista'"),
        "dto_min": ("-descuento", "tipo = 'minorista' AND descuento <> 0"),
        "dto_may": ("-descuento", "tipo = 'mayorista' AND descuento <> 0"),
        "envio_min": ("envio_cobrado", "tipo = 'minorista' AND envio_cobrado <> 0"),
        "envio_may": ("envio_cobrado", "tipo = 'mayorista' AND envio_cobrado <> 0"),
        "envio_andreani": ("-envio_cobrado", "envio_cobrado <> 0 AND tracking LIKE '%%36000%%'"),
        "envio_correo": ("-envio_cobrado",
                         "envio_cobrado <> 0 AND tracking NOT LIKE '%%36000%%' AND tracking LIKE '%%1978%%'"
                         " AND (anio * 100 + mes) >= 202408"),
        "envio_moto": ("-envio_cobrado",
                       "envio_cobrado <> 0 AND tracking NOT LIKE '%%36000%%' AND tracking NOT LIKE '%%1978%%'"
                       " AND carrier = 'moto'"),
        "envio_otro": ("-envio_cobrado",
                       "envio_cobrado <> 0 AND tracking NOT LIKE '%%36000%%' AND tracking NOT LIKE '%%1978%%'"
                       " AND carrier <> 'moto' AND COALESCE(tracking, '') <> ''"),
    }
    valor, cond = exprs[rubro]
    filas = _q(f"""
        SELECT orden_id::text AS orden_id, fecha, cliente, email, tipo, medio_pago, {_forma()} AS forma, carrier,
               estado_envio, tracking, subtotal, descuento, envio_cobrado, total,
               ({valor}) AS valor
        FROM ventas
        WHERE {PAGADAS} AND {_sql_periodo()} AND {cond}
        ORDER BY fecha, orden_id
    """, _params_periodo(d, h))
    cols = C_VENTA + ([{"key": "tracking", "label": "Tracking", "tipo": "texto"}]
                      if rubro.startswith("envio_") and rubro not in ("envio_min", "envio_may") else []) + [C_VALOR]
    notas = []
    if rubro == "envio_correo":
        hist = [x for x in _s3_filas("correo_historico.json", "importe") if _en(x["k"], d, h)]
        for x in hist:
            filas.append({"fecha": _fecha_txt(x["fecha"], x["k"]), "orden_id": "", "cliente": "",
                          "tipo": "", "medio_pago": "", "carrier": "correo (histórico)",
                          "detalle": "Histórico Correo Argentino", "valor": -x["importe"]})
        if hist: notas.append("Hasta jul-2024 el correo sale del histórico de facturas de Correo Argentino.")
    return {"columnas": cols, "filas": filas, "link": "orden", "notas": notas}


def _s3_filas(key, campo):
    def leer():
        out = []
        for row in rn.s3_leer(key) or []:
            f = row.get("fecha", "")
            v = rn.safe_float(row.get(campo, 0))
            k = rn.mes_key(f)
            if f and v and k:
                out.append({"k": k, "fecha": f, "importe": v, "row": row})
        return out
    return _cacheado(f"s3:{key}:{campo}", leer)


def _mp_rubro(rubro, d, h):
    filas = []
    notas = []
    if rubro == "com_mp":
        filas = _q(f"""
            SELECT fecha, source_id, transaction_type, payment_method, transaction_amount,
                   financing_fee_amount, mkp_fee_amount, fee_amount, taxes_amount, settlement_net_amount,
                   (COALESCE(financing_fee_amount, 0) + COALESCE(mkp_fee_amount, 0)) AS valor
            FROM mp_settlement
            WHERE {_sql_periodo()} AND (COALESCE(financing_fee_amount, 0) + COALESCE(mkp_fee_amount, 0)) <> 0
            ORDER BY fecha
        """, _params_periodo(d, h))
        cols = [
            {"key": "fecha", "label": "Fecha", "tipo": "fecha"},
            {"key": "source_id", "label": "ID MP", "tipo": "texto"},
            {"key": "transaction_type", "label": "Tipo", "tipo": "texto", "filtro": True},
            {"key": "payment_method", "label": "Medio", "tipo": "texto", "filtro": True},
            {"key": "transaction_amount", "label": "Monto", "tipo": "money"},
            {"key": "financing_fee_amount", "label": "Costo cuotas", "tipo": "money"},
            {"key": "mkp_fee_amount", "label": "Comisión MP", "tipo": "money"},
            {"key": "settlement_net_amount", "label": "Neto", "tipo": "money"},
            C_VALOR,
        ]
        notas.append("Comisión = costo de cuotas + comisión de mercado (fecha de liquidación).")
        return {"columnas": cols, "filas": filas, "notas": notas}

    # ret_iibb: MercadoPago + PagoNube (desde feb-2024) + histórico Getnet
    for r in _q(f"""
        SELECT fecha, source_id AS referencia, payment_method AS medio, transaction_amount AS monto,
               iibb_jurisdiccion AS jurisdiccion, taxes_amount AS valor
        FROM mp_settlement WHERE {_sql_periodo()} AND COALESCE(taxes_amount, 0) <> 0
    """, _params_periodo(d, h)):
        r["origen"] = "MercadoPago"; filas.append(r)
    for r in _q(f"""
        SELECT fecha, numero_venta AS referencia, cliente, medio_pago AS medio, monto_venta AS monto,
               iibb AS valor
        FROM pagonube WHERE {_sql_periodo()} AND (anio * 100 + mes) >= 202402 AND COALESCE(iibb, 0) <> 0
    """, _params_periodo(d, h)):
        r["origen"] = "PagoNube"; filas.append(r)
    for x in _s3_filas("mp_getnet_historico.json", "iibb"):
        if _en(x["k"], d, h):
            filas.append({"fecha": _fecha_txt(x["fecha"], x["k"]), "origen": "Getnet (histórico)",
                          "referencia": "", "valor": -abs(x["importe"])})
    filas.sort(key=lambda r: str(r.get("fecha", "")))
    cols = [
        {"key": "fecha", "label": "Fecha", "tipo": "fecha"},
        {"key": "origen", "label": "Origen", "tipo": "texto", "filtro": True},
        {"key": "referencia", "label": "Referencia", "tipo": "texto"},
        {"key": "cliente", "label": "Cliente", "tipo": "texto"},
        {"key": "medio", "label": "Medio", "tipo": "texto", "filtro": True},
        {"key": "jurisdiccion", "label": "Jurisdicción", "tipo": "texto", "filtro": True},
        {"key": "monto", "label": "Monto operación", "tipo": "money"},
        C_VALOR,
    ]
    return {"columnas": cols, "filas": filas, "notas": notas}


def _pagonube_rubro(d, h):
    filas = _q(f"""
        SELECT fecha, numero_venta, cliente, medio_pago, monto_venta, tasa, cuota_simple,
               cuotas_pagonube, iibb, comision_total AS valor
        FROM pagonube
        WHERE {_sql_periodo()} AND (anio * 100 + mes) >= 202402 AND COALESCE(comision_total, 0) <> 0
        ORDER BY fecha
    """, _params_periodo(d, h))
    for r in filas: r["origen"] = "PagoNube"
    for x in _s3_filas("mp_getnet_historico.json", "comision"):
        if _en(x["k"], d, h):
            filas.append({"fecha": _fecha_txt(x["fecha"], x["k"]), "origen": "Getnet (histórico)",
                          "numero_venta": "", "valor": -x["importe"]})
    cols = [
        {"key": "fecha", "label": "Fecha", "tipo": "fecha"},
        {"key": "origen", "label": "Origen", "tipo": "texto", "filtro": True},
        {"key": "numero_venta", "label": "Venta #", "tipo": "texto"},
        {"key": "cliente", "label": "Cliente", "tipo": "texto"},
        {"key": "medio_pago", "label": "Medio", "tipo": "texto", "filtro": True},
        {"key": "monto_venta", "label": "Monto venta", "tipo": "money"},
        {"key": "tasa", "label": "Tasa", "tipo": "money"},
        {"key": "cuota_simple", "label": "Cuota Simple", "tipo": "money"},
        {"key": "cuotas_pagonube", "label": "Cuotas PN", "tipo": "money"},
        C_VALOR,
    ]
    notas = ["El mes en curso puede incluir ventas que todavía no llegaron a la base (se toman del último export de PagoNube)."]
    return {"columnas": cols, "filas": filas, "notas": notas}


def _meta_rubro(d, h):
    filas = _q(f"""
        SELECT fecha, anio, mes, gasto, impresiones, clicks FROM meta_ads
        WHERE {_sql_periodo()} AND COALESCE(gasto, 0) <> 0 ORDER BY fecha
    """, _params_periodo(d, h))
    for r in filas:
        mult = rn.META_MULTIPLICADOR.get((r["anio"], r["mes"]), rn.META_MULTIPLICADOR_DEFAULT)
        r["mult"] = mult
        r["valor"] = -r["gasto"] * mult
        r["cpc"] = (r["gasto"] / r["clicks"]) if r.get("clicks") else None
    cols = [
        {"key": "fecha", "label": "Fecha", "tipo": "fecha"},
        {"key": "gasto", "label": "Gasto Meta", "tipo": "money"},
        {"key": "mult", "label": "Ajuste (IVA/perc.)", "tipo": "num"},
        {"key": "impresiones", "label": "Impresiones", "tipo": "int"},
        {"key": "clicks", "label": "Clicks", "tipo": "int"},
        {"key": "cpc", "label": "Costo x click", "tipo": "money"},
        C_VALOR,
    ]
    return {"columnas": cols, "filas": filas,
            "notas": ["Impacto = gasto informado por Meta × multiplicador de impuestos del mes."]}


def _gads_rubro(d, h):
    todas, _ = _cacheado("gads", lambda: rn._leer_gads_filas(verbose=False))
    filas = [{"fecha": _fecha_txt(x["fecha"], x["k"]), "descripcion": x["descripcion"],
              "credito": x["credito"], "mult": x["mult"], "valor": x["valor"]}
             for x in todas if _en(x["k"], d, h)]
    filas.sort(key=lambda r: r["fecha"])
    cols = [
        {"key": "fecha", "label": "Fecha", "tipo": "fecha"},
        {"key": "descripcion", "label": "Descripción", "tipo": "texto"},
        {"key": "credito", "label": "Pago", "tipo": "money"},
        {"key": "mult", "label": "Ajuste", "tipo": "num"},
        C_VALOR,
    ]
    return {"columnas": cols, "filas": filas, "notas": ["Pagos a Google Ads cargados en el Sheet de Google Ads."]}


SOLAPAS = {
    "compras": (["Compra Materia prima - Producto", "Compras", "compras"], 3, False),
    "sueldos": (["Sueldos", "sueldos"], 3, False),
    "pub_agencia": (["Publicidad", "publicidad"], 3, False),
    "ventas_manual": (["Ventas", "ventas"], 2, True),
}


def _sheet_rubro(rubro, d, h):
    nombres, col, es_ing = SOLAPAS[rubro]
    todas, solapa = _cacheado(f"sheet:{rubro}", lambda: rn._leer_solapa_filas(nombres, col, es_ing, rubro, verbose=False))
    filas = [{"fecha": _fecha_txt(x["fecha"], x["k"]), "detalle": x["detalle"], "valor": x["valor"]}
             for x in todas if _en(x["k"], d, h)]
    return {"columnas": C_SHEET, "filas": filas,
            "notas": [f"Cargado a mano (bot de Telegram) en la solapa «{solapa}» del Sheet de gastos."] if solapa else []}


def _tn_abono_rubro(d, h):
    todas = _cacheado("sheet:tn_abono", lambda: rn._leer_tn_abono_filas(verbose=False))
    filas = [{"fecha": _fecha_txt(x["fecha"], x["k"]), "detalle": "Abono Tiendanube", "valor": -x["importe"]}
             for x in todas if _en(x["k"], d, h) and x["k"][0] >= rn.ANO_DESDE]
    return {"columnas": C_SHEET, "filas": filas, "notas": ["Solapa «Tiendanube_abono» del Sheet de gastos."]}


def _monotributo_rubro(d, h):
    filas = [{"fecha": _fecha_txt(x["fecha"], x["k"]), "detalle": "Monotributo", "valor": -x["importe"]}
             for x in _s3_filas("monotributo.json", "importe") if _en(x["k"], d, h)]
    return {"columnas": C_SHEET, "filas": filas, "notas": []}


@web_bp.route("/rubro")
def rubro():
    rub = request.args.get("rubro", "")
    d, h = _periodo()
    try:
        if rub in ("ventas_min", "ventas_may", "dto_min", "dto_may", "envio_min", "envio_may",
                   "envio_andreani", "envio_correo", "envio_moto", "envio_otro"):
            res = _ventas_rubro(rub, d, h)
        elif rub in ("com_mp", "ret_iibb"):
            res = _mp_rubro(rub, d, h)
        elif rub == "com_pagonube":
            res = _pagonube_rubro(d, h)
        elif rub == "pub_meta":
            res = _meta_rubro(d, h)
        elif rub == "pub_gads":
            res = _gads_rubro(d, h)
        elif rub in SOLAPAS:
            res = _sheet_rubro(rub, d, h)
        elif rub == "com_tn":
            res = _tn_abono_rubro(d, h)
        elif rub == "monotributo":
            res = _monotributo_rubro(d, h)
        else:
            return jsonify({"ok": False, "error": f"rubro desconocido: {rub}"}), 400

        tot = _q(f"SELECT COALESCE(SUM(monto), 0) AS t FROM detalle_pnl WHERE rubro = %(r)s AND {_sql_periodo()}",
                 {"r": rub, **_params_periodo(d, h)})
        res.update({
            "ok": True, "rubro": rub, "desde": _km(d), "hasta": _km(h),
            "total_detalle": round(sum(float(r.get("valor") or 0) for r in res["filas"]), 2),
            "total_pnl": round(tot[0]["t"], 2),
        })
        return jsonify(res)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════
# EXPLORADORES: ventas, orden, productos, clientes
# ═══════════════════════════════════════════════════════════════

def _rango_fechas():
    desde = request.args.get("desde", "")
    hasta = request.args.get("hasta", "")
    for s in (desde, hasta):
        if s: datetime.strptime(s, "%Y-%m-%d")
    return desde or "2000-01-01", hasta or "2100-01-01"


@web_bp.route("/ventas")
def ventas():
    try:
        desde, hasta = _rango_fechas()
        cond, params = ["v." + PAGADAS, "v.fecha BETWEEN %(desde)s AND %(hasta)s"], {"desde": desde, "hasta": hasta}
        if request.args.get("email"):
            cond.append("LOWER(v.email) = LOWER(%(email)s)"); params["email"] = request.args["email"]
        if request.args.get("producto_id"):
            cond.append("EXISTS (SELECT 1 FROM ventas_detalle x WHERE x.orden_id = v.orden_id AND x.producto_id::text = %(pid)s)")
            params["pid"] = request.args["producto_id"]
        filas = _q(f"""
            SELECT v.orden_id::text AS orden_id, v.fecha, v.cliente, v.email, v.tipo, v.medio_pago,
                   {_forma("v.medio_pago")} AS forma, v.carrier,
                   v.estado_envio, v.subtotal, v.descuento, v.envio_cobrado, v.total,
                   COALESCE((SELECT SUM(cantidad) FROM ventas_detalle x WHERE x.orden_id = v.orden_id), 0) AS unidades
            FROM ventas v WHERE {' AND '.join(cond)}
            ORDER BY v.fecha DESC, v.orden_id DESC
        """, params)
        return jsonify({"ok": True, "filas": filas})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@web_bp.route("/orden/<orden_id>")
def orden(orden_id):
    try:
        o = _q(f"SELECT *, orden_id::text AS orden_id, {_forma()} AS forma FROM ventas WHERE orden_id::text = %(id)s",
               {"id": orden_id})
        if not o:
            return jsonify({"ok": False, "error": "Orden no encontrada"}), 404
        items = _q("""
            SELECT producto_id::text AS producto_id, producto_nombre, variante_nombre, sku, cantidad,
                   precio_unitario, cantidad * precio_unitario AS importe
            FROM ventas_detalle WHERE orden_id::text = %(id)s ORDER BY id
        """, {"id": orden_id})
        o = o[0]; o.pop("id", None)
        otras = []
        if o.get("email"):
            otras = _q(f"SELECT COUNT(*) AS n, COALESCE(SUM(total), 0) AS t FROM ventas WHERE {PAGADAS} AND LOWER(email) = LOWER(%(e)s)",
                       {"e": o["email"]})
        return jsonify({"ok": True, "orden": o, "items": items,
                        "cliente_hist": otras[0] if otras else None})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@web_bp.route("/productos")
def productos():
    try:
        desde, hasta = _rango_fechas()
        filas = _q(f"""
            SELECT d.producto_id::text AS producto_id, MAX(d.producto_nombre) AS producto,
                   SUM(d.cantidad) AS unidades, SUM(d.cantidad * d.precio_unitario) AS facturado,
                   COUNT(DISTINCT d.orden_id) AS ordenes,
                   SUM(d.cantidad * d.precio_unitario) / NULLIF(SUM(d.cantidad), 0) AS precio_prom,
                   MAX(v.fecha) AS ultima_venta
            FROM ventas_detalle d JOIN ventas v ON v.orden_id = d.orden_id
            WHERE v.{PAGADAS} AND v.fecha BETWEEN %(desde)s AND %(hasta)s
            GROUP BY d.producto_id
            ORDER BY facturado DESC
        """, {"desde": desde, "hasta": hasta})
        return jsonify({"ok": True, "filas": filas})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@web_bp.route("/producto/<producto_id>")
def producto(producto_id):
    try:
        desde, hasta = _rango_fechas()
        p = {"pid": producto_id, "desde": desde, "hasta": hasta}
        variantes = _q(f"""
            SELECT COALESCE(NULLIF(d.variante_nombre, ''), '(sin variante)') AS variante, MAX(d.sku) AS sku,
                   SUM(d.cantidad) AS unidades, SUM(d.cantidad * d.precio_unitario) AS facturado
            FROM ventas_detalle d JOIN ventas v ON v.orden_id = d.orden_id
            WHERE v.{PAGADAS} AND v.fecha BETWEEN %(desde)s AND %(hasta)s AND d.producto_id::text = %(pid)s
            GROUP BY 1 ORDER BY unidades DESC
        """, p)
        mensual = _q(f"""
            SELECT TO_CHAR(v.fecha, 'YYYY-MM') AS mes, SUM(d.cantidad) AS unidades,
                   SUM(d.cantidad * d.precio_unitario) AS facturado
            FROM ventas_detalle d JOIN ventas v ON v.orden_id = d.orden_id
            WHERE v.{PAGADAS} AND d.producto_id::text = %(pid)s
            GROUP BY 1 ORDER BY 1
        """, p)
        nombre = _q("SELECT MAX(producto_nombre) AS n FROM ventas_detalle WHERE producto_id::text = %(pid)s", p)
        return jsonify({"ok": True, "producto": nombre[0]["n"] if nombre else "", "variantes": variantes, "mensual": mensual})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@web_bp.route("/clientes")
def clientes():
    try:
        desde, hasta = _rango_fechas()
        filas = _q(f"""
            SELECT LOWER(COALESCE(NULLIF(email, ''), cliente)) AS clave, MAX(cliente) AS cliente,
                   MAX(email) AS email, COUNT(*) AS ordenes, SUM(total) AS total,
                   SUM(total) / COUNT(*) AS ticket_prom, MIN(fecha) AS primera, MAX(fecha) AS ultima,
                   BOOL_OR(tipo = 'mayorista') AS mayorista
            FROM ventas
            WHERE {PAGADAS} AND fecha BETWEEN %(desde)s AND %(hasta)s
            GROUP BY 1 ORDER BY total DESC
        """, {"desde": desde, "hasta": hasta})
        return jsonify({"ok": True, "filas": filas})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════
# COBRANZAS por forma de pago
# ═══════════════════════════════════════════════════════════════

@web_bp.route("/cobranzas")
def cobranzas():
    """Cobrado por mes y forma de pago (órdenes TN + ventas manuales del P&L) y comisiones de cobro."""
    d, h = _periodo()
    p = _params_periodo(d, h)
    try:
        filas = _q(f"""
            SELECT anio, mes, {_forma()} AS forma, COUNT(*) AS ordenes, SUM(total) AS cobrado
            FROM ventas WHERE {PAGADAS} AND {_sql_periodo()}
            GROUP BY 1, 2, 3
        """, p)
        out = [{"mes": _km((r["anio"], r["mes"])), "forma": r["forma"],
                "ordenes": r["ordenes"], "cobrado": r["cobrado"]} for r in filas]
        comisiones = {}
        for r in _q(f"""
            SELECT anio, mes, rubro, monto FROM detalle_pnl
            WHERE rubro IN ('ventas_manual', 'com_mp', 'com_pagonube') AND {_sql_periodo()}
        """, p):
            km = _km((r["anio"], r["mes"]))
            if r["rubro"] == "ventas_manual":
                out.append({"mes": km, "forma": "Ventas manuales", "ordenes": None, "cobrado": r["monto"]})
            else:
                comisiones.setdefault(km, {})[r["rubro"]] = r["monto"]
        return jsonify({"ok": True, "filas": out, "comisiones": comisiones})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════
# GRÁFICOS: series mensuales, día/hora, recompra, productos que suben/bajan
# ═══════════════════════════════════════════════════════════════

CLI_SQL = "LOWER(COALESCE(NULLIF(email, ''), cliente))"
HOY_AR = "(NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date"


@web_bp.route("/analisis")
def analisis():
    try:
        mensual = _q(f"""
            WITH v AS (SELECT *, {CLI_SQL} AS cli FROM ventas WHERE {PAGADAS}),
            primera AS (SELECT cli, MIN(fecha) AS f1 FROM v GROUP BY cli),
            u AS (SELECT orden_id, SUM(cantidad) AS unidades FROM ventas_detalle GROUP BY orden_id)
            SELECT TO_CHAR(v.fecha, 'YYYY-MM') AS mes,
                   COUNT(*) AS ordenes, SUM(v.total) AS total, SUM(v.subtotal) AS subtotal,
                   SUM(COALESCE(u.unidades, 0)) AS unidades,
                   COUNT(DISTINCT v.cli) AS clientes,
                   COUNT(DISTINCT v.cli) FILTER (WHERE DATE_TRUNC('month', p.f1) = DATE_TRUNC('month', v.fecha)) AS clientes_nuevos,
                   COALESCE(SUM(v.total) FILTER (WHERE DATE_TRUNC('month', p.f1) = DATE_TRUNC('month', v.fecha)), 0) AS total_nuevos,
                   COUNT(*) FILTER (WHERE COALESCE(v.envio_cobrado, 0) = 0) AS envio_gratis,
                   COUNT(*) FILTER (WHERE v.carrier = 'correo') AS env_correo,
                   COUNT(*) FILTER (WHERE v.carrier = 'andreani') AS env_andreani,
                   COUNT(*) FILTER (WHERE v.carrier = 'moto') AS env_moto,
                   COUNT(*) FILTER (WHERE v.carrier NOT IN ('correo', 'andreani', 'moto')) AS env_otro,
                   COALESCE(SUM(v.envio_cobrado), 0) AS envio_cobrado
            FROM v JOIN primera p USING (cli) LEFT JOIN u USING (orden_id)
            GROUP BY 1 ORDER BY 1
        """)
        rec = _q(f"""
            WITH o AS (
                SELECT {CLI_SQL} AS cli, fecha, ROW_NUMBER() OVER (PARTITION BY {CLI_SQL} ORDER BY fecha, orden_id) AS rn
                FROM ventas WHERE {PAGADAS}
            ), c AS (SELECT cli, MAX(rn) AS n FROM o GROUP BY cli)
            SELECT (SELECT COUNT(*) FROM c) AS clientes,
                   (SELECT COUNT(*) FROM c WHERE n > 1) AS volvieron,
                   (SELECT AVG(n) FROM c) AS compras_prom,
                   (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY b.fecha - a.fecha)
                      FROM o a JOIN o b ON a.cli = b.cli AND a.rn = 1 AND b.rn = 2) AS dias_a_segunda
        """)
        return jsonify({"ok": True, "mensual": mensual, "recompra": rec[0] if rec else None})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@web_bp.route("/dia-hora")
def dia_hora():
    """Órdenes por día de la semana (1=lunes) y hora, hora de Argentina."""
    try:
        desde, hasta = _rango_fechas()
        filas = _q(f"""
            SELECT EXTRACT(ISODOW FROM creada AT TIME ZONE 'America/Argentina/Buenos_Aires')::int AS dow,
                   EXTRACT(HOUR FROM creada AT TIME ZONE 'America/Argentina/Buenos_Aires')::int AS hora,
                   COUNT(*) AS ordenes, SUM(total) AS total
            FROM ventas
            WHERE {PAGADAS} AND creada IS NOT NULL AND fecha BETWEEN %(desde)s AND %(hasta)s
            GROUP BY 1, 2
        """, {"desde": desde, "hasta": hasta})
        sin_hora = _q(f"""SELECT COUNT(*) AS n FROM ventas
                          WHERE {PAGADAS} AND creada IS NULL AND fecha BETWEEN %(desde)s AND %(hasta)s""",
                      {"desde": desde, "hasta": hasta})
        return jsonify({"ok": True, "filas": filas, "sin_hora": sin_hora[0]["n"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@web_bp.route("/productos-cambio")
def productos_cambio():
    """Unidades y facturación de cada producto en los últimos N días vs los N días anteriores."""
    try:
        n = max(7, min(365, int(request.args.get("dias", 90))))
        filas = _q(f"""
            SELECT d.producto_id::text AS producto_id, MAX(d.producto_nombre) AS producto,
                   COALESCE(SUM(d.cantidad) FILTER (WHERE v.fecha > {HOY_AR} - %(n)s), 0) AS u_act,
                   COALESCE(SUM(d.cantidad) FILTER (WHERE v.fecha <= {HOY_AR} - %(n)s), 0) AS u_ant,
                   COALESCE(SUM(d.cantidad * d.precio_unitario) FILTER (WHERE v.fecha > {HOY_AR} - %(n)s), 0) AS f_act,
                   COALESCE(SUM(d.cantidad * d.precio_unitario) FILTER (WHERE v.fecha <= {HOY_AR} - %(n)s), 0) AS f_ant
            FROM ventas_detalle d JOIN ventas v ON v.orden_id = d.orden_id
            WHERE v.{PAGADAS} AND v.fecha > {HOY_AR} - 2 * %(n)s
            GROUP BY d.producto_id
        """, {"n": n})
        return jsonify({"ok": True, "dias": n, "filas": filas})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
