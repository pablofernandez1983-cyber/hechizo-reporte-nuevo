"""
app_reporte_nuevo.py — Flask wrapper para Railway
Expone reporte_nuevo.py como endpoint HTTP.
"""

import os, sys, threading, io, requests, json, time
from collections import deque
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from reporte_nuevo import main as ejecutar_reporte, sync_ventas_recientes

DATABASE_URL = os.environ.get("DATABASE_URL", "")
TN_STORE_ID  = os.environ.get("TIENDANUBE_STORE_ID", "")
TN_TOKEN     = os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")
TN_HEADERS   = {
    "Authentication": f"bearer {TN_TOKEN}",
    "User-Agent": "HechizoBijou-Stock/1.0 (hechizobijou@gmail.com)"
}

app = Flask(__name__)
CORS(app)

from stock_notifications import notify_bp
app.register_blueprint(notify_bp)

from tiendanube_app import tn_bp
app.register_blueprint(tn_bp)

from tiendanube_ruleta import ruleta_bp
app.register_blueprint(ruleta_bp)

from web_detalle import web_bp
app.register_blueprint(web_bp)

from recurrentes import rec_bp
app.register_blueprint(rec_bp)

from forecast import forecast_bp
app.register_blueprint(forecast_bp)

from tiktok_app import tiktok_bp
app.register_blueprint(tiktok_bp)

from juli_horarios import juli_bp
app.register_blueprint(juli_bp)

from envios_seguimiento import envios_bp
app.register_blueprint(envios_bp)

from auth_clave import chequear_clave

# Rutas que usa la app del celu (index.html): exigen la misma clave que /web/*.
# /ping queda abierta (despierta el servidor); los blueprints tienen su propio chequeo.
_ENDPOINTS_CON_CLAVE = {
    "ejecutar", "ver_estado", "ver_tokens", "ver_logs", "historico", "historico_mensual",
    "historico_rango", "stock_debug", "stock_data", "sheet_values",
}


@app.before_request
def _auth_app():
    if request.method == "OPTIONS" or request.endpoint not in _ENDPOINTS_CON_CLAVE:
        return None
    return chequear_clave()


# Proxy de lectura del Sheet RESUMEN para la app del celu (así el Sheet no tiene que
# estar compartido por link). Solo las solapas del P&L y Trigger, nunca Ventas_diarias.
import re as _re
_SOLAPAS_APP = _re.compile(r"^(20\d\d|20\d\d detalle|Trigger)$")


@app.route("/sheet-values")
def sheet_values():
    from reporte_nuevo import get_svc, SHEET_ID_RESUMEN
    rango = request.args.get("range", "")
    solapa = rango.split("!", 1)[0].strip("'")
    if not _SOLAPAS_APP.match(solapa):
        return jsonify({"ok": False, "error": f"rango no permitido: {rango}"}), 403
    kwargs = dict(spreadsheetId=SHEET_ID_RESUMEN, range=rango)
    if request.args.get("valueRenderOption") == "UNFORMATTED_VALUE":
        kwargs["valueRenderOption"] = "UNFORMATTED_VALUE"
    try:
        res = get_svc().spreadsheets().values().get(**kwargs).execute()
    except Exception as e:
        # 404 = la solapa no existe (ej. el año nuevo antes de la primera corrida);
        # 502 = no se pudo leer. La app los trata distinto.
        no_existe = "Unable to parse range" in str(e)
        return jsonify({"ok": False, "error": str(e)[:300]}), 404 if no_existe else 502
    return jsonify({"ok": True, "range": res.get("range"), "values": res.get("values", [])})


@app.after_request
def no_cache_widget(response):
    if request.path == "/static/widget-ruleta.js":
        response.headers["Cache-Control"] = "no-store"
    return response

S3_BUCKET   = os.environ.get("AWS_S3_BUCKET_NAME", "")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "")
S3_REGION   = os.environ.get("AWS_DEFAULT_REGION", "auto")
S3_KEY_ID   = os.environ.get("AWS_ACCESS_KEY_ID", "")
S3_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

def _s3_leer(key):
    if not S3_BUCKET: return None
    try:
        import boto3
        from botocore.config import Config
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT,
                          aws_access_key_id=S3_KEY_ID,
                          aws_secret_access_key=S3_SECRET,
                          region_name=S3_REGION,
                          config=Config(signature_version="s3v4"))
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None

TZ_AR = timezone(timedelta(hours=-3))

_estado = {
    "corriendo": False,
    "ultimo_inicio": None,
    "ultimo_fin": None,
    "resultado": None,
    "error": None,
    "warnings": [],
}

_log_buffer = deque(maxlen=200)  # últimas 200 líneas

_ultimo_sync_ventas_rapido = 0
# El servidor atiende varios pedidos a la vez (gunicorn --threads): la corrida del reporte y
# el sync liviano de /historico comparten la conexión a la base, así que no se pisan.
_db_lock = threading.Lock()
_inicio_lock = threading.Lock()
SYNC_VENTAS_THROTTLE_SEG   = 300  # no repetir el sync liviano de TN más de 1 vez cada 5 min


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


_FUENTES_WARN = [
    ("Meta Ads",    ["meta"]),
    ("Google Ads",  ["google ads", "gads"]),
    ("Tiendanube",  ["tiendanube"]),
    ("MercadoPago", ["mercadopago", "mp "]),
    ("Pagonube",    ["pagonube"]),
    ("Supabase",    ["supabase", "db exec", "db connect"]),
    ("Google Sheets", ["leer_hoja", "sheets"]),
]

def _detectar_warnings():
    warns = []
    logs = " ".join(l.lower() for l in _log_buffer if "[warn]" in l.lower() or "[error]" in l.lower())
    for nombre, claves in _FUENTES_WARN:
        if any(c in logs for c in claves):
            warns.append(nombre)
    return warns


def _run():
    _estado["ultimo_inicio"] = datetime.now(TZ_AR).isoformat()
    _estado["error"] = None
    _log_buffer.clear()

    original_stdout = sys.stdout
    sys.stdout = _LogCapture(original_stdout)
    try:
        with _db_lock:   # espera si justo hay un sync liviano en curso
            ejecutar_reporte()
        _estado["resultado"] = "OK"
    except Exception as e:
        _estado["resultado"] = "ERROR"
        _estado["error"] = str(e)
    finally:
        sys.stdout = original_stdout
        _estado["corriendo"] = False
        _estado["ultimo_fin"] = datetime.now(TZ_AR).isoformat()
        _estado["warnings"] = _detectar_warnings()



@app.route("/ejecutar", methods=["POST"])
def ejecutar():
    with _inicio_lock:   # dos toques seguidos no largan dos corridas
        if _estado["corriendo"]:
            return jsonify({"ok": False, "msg": "Ya está corriendo un reporte"}), 409
        _estado["corriendo"] = True
    import reporte_nuevo
    reporte_nuevo.DB_COMPLETO = request.args.get("completo") == "1"
    reporte_nuevo.FORZAR = request.args.get("forzar") == "1"
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
        "warnings": _estado["warnings"],
    })


@app.route("/tokens")
def ver_tokens():
    tokens = []
    hoy = datetime.now(timezone.utc)

    # ── Meta Ads token ─────────────────────────────────────────────
    meta_token  = os.environ.get("META_ACCESS_TOKEN", "")
    meta_app_id = os.environ.get("META_APP_ID", "")
    meta_secret = os.environ.get("META_APP_SECRET", "")
    if meta_token and meta_app_id and meta_secret:
        try:
            r = requests.get(
                "https://graph.facebook.com/debug_token",
                params={"input_token": meta_token, "access_token": f"{meta_app_id}|{meta_secret}"},
                timeout=8
            )
            d = r.json().get("data", {})
            expires_ts = d.get("expires_at")
            is_valid   = d.get("is_valid", False)
            if expires_ts:
                expires_dt = datetime.fromtimestamp(expires_ts, tz=timezone.utc)
                dias = (expires_dt - hoy).days
                tokens.append({
                    "nombre": "Meta Ads",
                    "valido": is_valid,
                    "expira": expires_dt.strftime("%d/%m/%Y"),
                    "dias_restantes": dias,
                })
            elif expires_ts == 0 and is_valid:
                tokens.append({"nombre": "Meta Ads", "valido": True, "expira": "permanente", "dias_restantes": None})
            else:
                tokens.append({"nombre": "Meta Ads", "valido": False, "expira": None, "dias_restantes": 0})
        except Exception as e:
            tokens.append({"nombre": "Meta Ads", "valido": None, "expira": None, "dias_restantes": None, "error": str(e)})

    # ── Tiendanube / Pagonube — último run exitoso desde S3 ────────
    try:
        last_run_data = _s3_leer("pagonube_last_run.json")
        if last_run_data and isinstance(last_run_data, dict):
            ultimo = last_run_data.get("ultimo_exito")
        elif last_run_data and isinstance(last_run_data, list) and last_run_data:
            ultimo = last_run_data[0].get("ultimo_exito") if isinstance(last_run_data[0], dict) else None
        else:
            ultimo = None
        if ultimo:
            from datetime import date as _date
            ultimo_dt = datetime.fromisoformat(ultimo).replace(tzinfo=timezone.utc)
            dias_desde = (hoy - ultimo_dt).days
            # Solo marcar dias_restantes si ya está atrasado o borderline (>=2 días).
            # Si corrió hoy o ayer, no hay nada que alertar → None evita el banner.
            dr = (2 - dias_desde) if dias_desde >= 2 else None
            tokens.append({
                "nombre": "Pagonube (sesión TN)",
                "valido": dias_desde <= 2,
                "expira": f"último run: {ultimo_dt.strftime('%d/%m/%Y')}",
                "dias_restantes": dr,
            })
        else:
            tokens.append({
                "nombre": "Pagonube (sesión TN)",
                "valido": None,
                "expira": None,
                "dias_restantes": None,
                "error": "sin datos de último run",
            })
    except Exception as e:
        tokens.append({"nombre": "Pagonube (sesión TN)", "valido": None, "expira": None, "dias_restantes": None, "error": str(e)})

    return jsonify({"ok": True, "tokens": tokens})


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

    # Sync liviano de TN → Supabase antes de leer, para que "ayer" no quede pisado
    # por la última corrida completa del reporte (que puede ser de horas antes).
    # Se salta si ya hay un reporte completo corriendo (comparten la misma conexión
    # a DB) y se throttlea para no golpear la API de TN en cada request.
    global _ultimo_sync_ventas_rapido
    ahora_ts = time.time()
    if not _estado["corriendo"] and (ahora_ts - _ultimo_sync_ventas_rapido) >= SYNC_VENTAS_THROTTLE_SEG:
        _ultimo_sync_ventas_rapido = ahora_ts
        if _db_lock.acquire(blocking=False):
            try:
                sync_ventas_recientes(dias=3)
            except Exception as e:
                print(f"[WARN] sync_ventas_recientes: {e}")
            finally:
                _db_lock.release()

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


@app.route("/historico-rango")
def historico_rango():
    """Ventas agrupadas por día/semana/mes en un rango de fechas arbitrario.
    Para el gráfico de evolución y las comparaciones de la solapa Estadísticas.
    Query params: desde, hasta (YYYY-MM-DD), granularidad (dia|semana|mes)."""
    if not DATABASE_URL:
        return jsonify({"ok": False, "error": "DATABASE_URL no configurada"}), 500

    desde = request.args.get("desde", "")
    hasta = request.args.get("hasta", "")
    granularidad = request.args.get("granularidad", "dia")
    if granularidad not in ("dia", "semana", "mes"):
        granularidad = "dia"
    try:
        datetime.strptime(desde, "%Y-%m-%d")
        datetime.strptime(hasta, "%Y-%m-%d")
    except ValueError:
        return jsonify({"ok": False, "error": "desde/hasta deben ser YYYY-MM-DD"}), 400

    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("""
            SELECT
              CASE
                WHEN %(gran)s = 'mes'    THEN TO_CHAR(fecha, 'YYYY-MM')
                WHEN %(gran)s = 'semana' THEN TO_CHAR(DATE_TRUNC('week', fecha), 'YYYY-MM-DD')
                ELSE TO_CHAR(fecha, 'YYYY-MM-DD')
              END AS periodo,
              ROUND(SUM(total)::numeric, 0) AS total,
              COUNT(orden_id)               AS cantidad
            FROM ventas
            WHERE fecha >= %(desde)s::date AND fecha <= %(hasta)s::date
              AND estado_pago IN ('paid', 'authorized')
            GROUP BY periodo
            ORDER BY periodo
        """, {"gran": granularidad, "desde": desde, "hasta": hasta})
        rows = cur.fetchall()
        cur.close()
        conn.close()
        puntos = [
            {"periodo": r[0], "total": int(r[1]), "cantidad": int(r[2])}
            for r in rows
        ]
        return jsonify({"ok": True, "puntos": puntos})
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
    """Devuelve los datos de stock como JSON para el dashboard.
    Calcularlo tarda ~25 s, así que se guarda en S3 (stock_data.json): sin ?refresh=1
    se devuelve lo guardado (lo actualiza el horario de las 19 h desde ventas-service
    y el botón "Actualizar stock"); con ?refresh=1 se recalcula y se guarda."""
    import traceback as _tb
    from reporte_nuevo import s3_guardar
    try:
        if request.args.get("refresh") != "1":
            guardado = _s3_leer("stock_data.json")
            if guardado and guardado.get("ok"):
                return jsonify(guardado)
        result = _stock_compute()
        if result.get("ok") and result.get("variantes"):  # no pisar lo guardado si TN falló
            s3_guardar("stock_data.json", result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trace": _tb.format_exc()}), 500


def _stock_compute():
    """Calcula datos de stock y los devuelve como dict."""
    errores = []

    # ── 1. Ventas últimos 180 días desde Supabase (3 buckets) ────────────
    ventas_30 = {}; ventas_90 = {}; ventas_180 = {}
    if DATABASE_URL:
        try:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            cur  = conn.cursor()
            cur.execute("""
                SELECT vd.variante_id,
                       SUM(CASE WHEN v.fecha >= (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date - INTERVAL '30 days'  THEN vd.cantidad ELSE 0 END) AS u30,
                       SUM(CASE WHEN v.fecha >= (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date - INTERVAL '90 days'  THEN vd.cantidad ELSE 0 END) AS u90,
                       SUM(vd.cantidad) AS u180
                FROM ventas_detalle vd
                JOIN ventas v ON v.orden_id = vd.orden_id
                WHERE v.fecha >= (NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires')::date
                      - INTERVAL '180 days'
                  AND v.estado_pago IN ('paid','authorized')
                GROUP BY vd.variante_id
            """)
            for vid, u30, u90, u180 in cur.fetchall():
                ventas_30[int(vid)]  = int(u30)
                ventas_90[int(vid)]  = int(u90)
                ventas_180[int(vid)] = int(u180)
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
        u30  = ventas_30.get(v["variante_id"], 0)
        u90  = ventas_90.get(v["variante_id"], 0)
        u180 = ventas_180.get(v["variante_id"], 0)
        v["unidades_30"]  = u30
        v["unidades_90"]  = u90
        v["unidades_180"] = u180
        vel90 = u90 / 90
        v["cobertura"] = round(v["stock"] / vel90, 1) if vel90 > 0 else None

    variantes.sort(key=lambda v: (
        0 if v["cobertura"] is not None else 1,
        v["cobertura"] if v["cobertura"] is not None else 0,
        -v["unidades_90"]
    ))

    alertas             = [v for v in variantes if v["cobertura"] is not None and v["cobertura"] < 21]
    top20               = sorted(variantes, key=lambda v: -v["unidades_90"])[:20]
    candidatos_liquidar = [v for v in variantes if v["stock"] >= 1 and v["unidades_180"] == 0]

    valuacion_total      = sum(v["stock"] * v["precio_venta"] for v in variantes
                              if "GIFT CARD" not in v["nombre"].upper())
    cantidad_valuacion   = sum(v["stock"] for v in variantes
                              if "GIFT CARD" not in v["nombre"].upper())

    # ── 4. Guardar valuación diaria y obtener mes anterior ────────────────
    valuacion_mes_anterior = None
    valuacion_historia     = []
    if DATABASE_URL:
        try:
            import psycopg2
            conn2 = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            cur2  = conn2.cursor()
            cur2.execute("""
                CREATE TABLE IF NOT EXISTS stock_valuacion (
                    fecha           DATE PRIMARY KEY,
                    valuacion_total NUMERIC(14,2)
                )
            """)
            cur2.execute("ALTER TABLE stock_valuacion ADD COLUMN IF NOT EXISTS cantidad INTEGER")
            today_ar        = datetime.now(TZ_AR).date()
            first_of_month  = today_ar.replace(day=1)
            cur2.execute("""
                INSERT INTO stock_valuacion (fecha, valuacion_total, cantidad)
                VALUES (%s, %s, %s)
                ON CONFLICT (fecha) DO UPDATE SET valuacion_total = EXCLUDED.valuacion_total,
                                                  cantidad = EXCLUDED.cantidad
            """, (today_ar, round(valuacion_total, 2), cantidad_valuacion))
            cur2.execute("""
                SELECT valuacion_total FROM stock_valuacion
                WHERE fecha < %s
                ORDER BY fecha DESC
                LIMIT 1
            """, (first_of_month,))
            row = cur2.fetchone()
            if row:
                valuacion_mes_anterior = float(row[0])
            cur2.execute("""
                SELECT TO_CHAR(mes_date, 'YYYY-MM') AS mes, valuacion_total AS val, cantidad
                FROM (
                    SELECT DISTINCT ON (DATE_TRUNC('month', fecha))
                           DATE_TRUNC('month', fecha) AS mes_date,
                           valuacion_total,
                           cantidad
                    FROM stock_valuacion
                    WHERE fecha >= NOW()::date - INTERVAL '13 months'
                    ORDER BY DATE_TRUNC('month', fecha), fecha DESC
                ) t
                ORDER BY mes_date
            """)
            valuacion_historia = [{"mes": r[0], "val": float(r[1]), "cant": r[2]} for r in cur2.fetchall()]
            conn2.commit()
            cur2.close(); conn2.close()
        except Exception as e:
            errores.append("Val hist: " + str(e))

    return {
        "ok":                    True,
        "generado_en":           datetime.now(TZ_AR).strftime("%d/%m/%Y %H:%M"),
        "total":                 len(variantes),
        "errores":               errores,
        "variantes":             variantes,
        "alertas":               alertas,
        "top20":                 top20,
        "candidatos_liquidar":   candidatos_liquidar,
        "valuacion_total":        valuacion_total,
        "cantidad_valuacion":     cantidad_valuacion,
        "valuacion_mes_anterior":  valuacion_mes_anterior,
        "valuacion_historia":      valuacion_historia,
    }


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "msg": "hechizo-reporte-nuevo running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
