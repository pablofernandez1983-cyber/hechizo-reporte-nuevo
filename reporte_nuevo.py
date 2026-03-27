"""
reporte_nuevo.py — Hechizo Bijou P&L mensual
"""

import os, json, re, time, traceback, requests
from datetime import datetime, date, timedelta, timezone
from collections import defaultdict
from google.oauth2 import service_account
from googleapiclient.discovery import build

TZ_AR = timezone(timedelta(hours=-3))
def ahora_ar(): return datetime.now(TZ_AR)

ANO = int(os.environ.get("ANO_REPORTE", ahora_ar().year))
SHEET_ID_RESUMEN    = os.environ.get("SHEET_ID_RESUMEN",    "1nUWfj9u0y7M7n2fNG6v55WnIxAedPpBxQZFUXk28nlI")
SHEET_ID_GASTOS     = os.environ.get("SHEET_ID_GASTOS",     "1Eswje16JVngNEPTpq8f2-_2tjO8XAbsBBJnCIaqNkRY")
SHEET_ID_GOOGLE_ADS = os.environ.get("SHEET_ID_GOOGLE_ADS", "1dNJReQ2zxMxRcs0tFTdhdPPAGaXzxqOJIRWDcY9KMPI")
TN_STORE_ID  = os.environ.get("TIENDANUBE_STORE_ID", "")
TN_TOKEN     = os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")
MP_TOKEN     = os.environ.get("MP_ACCESS_TOKEN", "")
MP_USER_ID   = os.environ.get("MP_USER_ID", "")
META_TOKEN   = os.environ.get("META_ACCESS_TOKEN", "")
META_ACCOUNT = os.environ.get("META_AD_ACCOUNT_ID", "")
SA_JSON      = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
S3_BUCKET    = os.environ.get("AWS_S3_BUCKET_NAME", "")
S3_KEY_ID    = os.environ.get("AWS_ACCESS_KEY_ID", "")
S3_SECRET    = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT  = os.environ.get("AWS_ENDPOINT_URL", "")
S3_REGION    = os.environ.get("AWS_DEFAULT_REGION", "auto")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

META_MULTIPLICADOR = {
    (2023,1):1.545,(2023,2):1.545,(2023,3):1.545,(2023,4):1.545,
    (2023,5):1.545,(2023,6):1.545,(2023,7):1.545,(2023,8):1.545,
    (2023,9):1.545,(2023,10):1.545,(2023,11):1.545,(2023,12):1.545,
    (2024,1):1.58,(2024,2):1.58,(2024,3):1.43,(2024,4):1.43,
    (2024,5):1.58,(2024,6):1.68,(2024,7):1.68,(2024,8):1.68,
    (2024,9):1.48,(2024,10):1.38,(2024,11):1.30,(2024,12):1.30,
    (2025,1):1.10,(2025,2):1.15,(2025,3):1.15,(2025,4):1.15,
    (2025,5):1.15,(2025,6):1.15,(2025,7):1.30,(2025,8):1.05,
    (2025,9):1.05,(2025,10):1.05,(2025,11):1.05,(2025,12):1.05,
    (2026,1):1.05,(2026,2):1.05,(2026,3):1.05,(2026,4):1.05,
    (2026,5):1.05,(2026,6):1.05,(2026,7):1.05,(2026,8):1.05,
    (2026,9):1.05,(2026,10):1.05,(2026,11):1.05,(2026,12):1.05,
}
META_MULTIPLICADOR_DEFAULT = 1.15

PNL_FILAS = [
    ("ventas_min",     "Ventas",                    "Ingresos"),
    ("envio_min",      "Cobro Envio Minorista",      "Ingresos"),
    ("dto_min",        "Descuento Minorista",        "Ingresos"),
    ("ventas_may",     "Ventas Mayoristas",          "Ingresos"),
    ("envio_may",      "Cobro Envio Mayorista",      "Ingresos"),
    ("dto_may",        "Descuento Mayorista",        "Ingresos"),
    ("ventas_manual",  "Ventas Manuales",            "Ingresos"),
    ("compras",        "Compras",                   "Costo de Mercaderia"),
    ("com_mp",         "Comisiones Mercadolibre",   "Gastos por Ventas"),
    ("com_pagonube",   "Comision Getnet/Pagonube",  "Gastos por Ventas"),
    ("com_tn",         "Comision Tiendanube",       "Gastos por Ventas"),
    ("envio_correo",   "Envio Correo Argentino",    "Gastos de Comercializacion"),
    ("envio_andreani", "Envio Andreani",             "Gastos de Comercializacion"),
    ("envio_moto",     "Envio Moto",                "Gastos de Comercializacion"),
    ("envio_otro",     "Envio Otro",                "Gastos de Comercializacion"),
    ("pub_meta",       "Publicidad Meta",           "Publicidad"),
    ("pub_gads",       "Publicidad Google Ads",     "Publicidad"),
    ("pub_agencia",    "Agencia Publicidad",        "Publicidad"),
    ("ret_iibb",       "Retenciones IIBB",          "Impuestos"),
    ("monotributo",    "Monotributo",               "Impuestos"),
    ("sueldos",        "Sueldos",                   "Gastos de Administracion"),
]
CATEGORIAS_EGRESO = [
    "Costo de Mercaderia","Gastos por Ventas","Gastos de Comercializacion",
    "Publicidad","Impuestos","Gastos de Administracion",
]

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def log(msg):
    print(f"[{ahora_ar().strftime('%H:%M:%S')}] {msg}", flush=True)

_MESES_ES = {
    'ene':1,'feb':2,'mar':3,'abr':4,'may':5,'jun':6,
    'jul':7,'ago':8,'sep':9,'sept':9,'oct':10,'nov':11,'dic':12
}

def mes_key(fecha, ano_ctx=None):
    if isinstance(fecha, (datetime, date)):
        return (fecha.year, fecha.month)
    try:
        serial = float(str(fecha).strip())
        if 30000 < serial < 70000:
            d = date(1899, 12, 30) + timedelta(days=int(serial))
            return (d.year, d.month)
    except (ValueError, TypeError):
        pass
    s = str(fecha).strip()
    m = re.match(r"(\d{4})\s+M(\d{2})", s)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    m = re.match(r"^([a-záéíóú]+)-(\d{2,4})$", s, re.IGNORECASE)
    if m:
        mes_num = _MESES_ES.get(m.group(1).lower()) or _MESES_ES.get(m.group(1).lower()[:3])
        if mes_num:
            raw = m.group(2)
            return (int("20"+raw) if len(raw)==2 else int(raw), mes_num)
    m = re.match(r"^(\d{1,2})-([a-záéíóú]+)(?:-(\d{2,4}))?$", s, re.IGNORECASE)
    if m:
        mes_num = _MESES_ES.get(m.group(2).lower()) or _MESES_ES.get(m.group(2).lower()[:3])
        if mes_num:
            if m.group(3):
                raw = m.group(3)
                return (int("20"+raw) if len(raw)==2 else int(raw), mes_num)
            else:
                if ano_ctx is None:
                    return None
                return (ano_ctx, mes_num)
    for ln in [10, 8]:
        sub = s[:ln]
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                dt = datetime.strptime(sub, fmt)
                return (dt.year, dt.month)
            except ValueError:
                pass
    return None

def acumular(dic, fecha, valor):
    k = mes_key(fecha)
    if k:
        dic[k] += valor

def safe_float(v):
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    is_neg = s.startswith('-')
    s = re.sub(r'[A-Za-z\$\s]', '', s)
    if is_neg and not s.startswith('-'):
        s = '-' + s.lstrip('-')
    if not s or s in ('-', '+', '--'):
        return 0.0
    lc, ld = s.rfind(','), s.rfind('.')
    if lc > 0 and ld > 0:
        s = s.replace(',','') if ld > lc else s.replace('.','').replace(',','.')
    elif lc > 0:
        s = s.replace(',','.') if len(s[lc+1:]) <= 2 else s.replace(',','')
    try:
        return float(s)
    except:
        return 0.0

def _col_idx(header_list, *keywords):
    h = [str(x).strip().lower() for x in header_list]
    for kw in keywords:
        for i, c in enumerate(h):
            if kw.lower() in c:
                return i
    return -1

# ═══════════════════════════════════════════════════════════════
# SUPABASE
# ═══════════════════════════════════════════════════════════════

_db_conn = None

def get_db():
    global _db_conn
    if not DATABASE_URL:
        return None
    try:
        if _db_conn is None or _db_conn.closed:
            import psycopg2
            _db_conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            _db_conn.autocommit = False
        return _db_conn
    except Exception as e:
        log(f"  [WARN] DB connect: {e}")
        return None

def db_exec(sql, params=None):
    conn = get_db()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception as e:
        log(f"  [WARN] DB exec: {e}")
        try: conn.rollback()
        except: pass

def db_exec_many(sql, rows):
    if not rows: return
    conn = get_db()
    if not conn: return
    try:
        import psycopg2.extras
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
        conn.commit()
        return True
    except Exception as e:
        log(f"  [WARN] DB exec_many: {e}")
        try: conn.rollback()
        except: pass
        return False

def guardar_ventas_db(orders):
    if not DATABASE_URL: return
    log("  DB: guardando ventas...")
    rows = []
    for o in orders:
        if o.get("status") == "cancelled": continue
        try:
            dt = datetime.fromisoformat(
                o.get("created_at","").replace("Z","+00:00")
            ).astimezone(TZ_AR)
        except: continue
        shipping_cust = safe_float(o.get("shipping_cost_customer", 0))
        tracking  = str(o.get("shipping_tracking_number","") or "").lower()
        medio_env = str(o.get("shipping_option","") or "").lower()
        if "36000" in tracking:       carrier = "andreani"
        elif medio_env.startswith("mot"): carrier = "moto"
        elif "1978" in tracking:      carrier = "correo"
        else:                         carrier = "otro"
        payment_details = o.get("payment_details") or []
        gateways = [p.get("payment_method_id","") or "" for p in payment_details if isinstance(p, dict)]
        is_may = any("transfer" in g.lower() or "deposito" in g.lower() for g in gateways)
        customer = o.get("customer") or {}
        rows.append((
            o.get("id"), dt.date(), dt.year, dt.month,
            customer.get("name",""), customer.get("email",""),
            safe_float(o.get("subtotal",0)), safe_float(o.get("discount",0)),
            shipping_cust, safe_float(o.get("total",0)),
            "mayorista" if is_may else "minorista", carrier,
            o.get("payment_status",""), o.get("shipping_status",""),
            str(o.get("gateway_name","") or o.get("gateway","")),
            str(o.get("shipping_tracking_number","") or ""),
        ))
    sql = """
        INSERT INTO ventas
            (orden_id, fecha, anio, mes, cliente, email,
             subtotal, descuento, envio_cobrado, total,
             tipo, carrier, estado_pago, estado_envio, medio_pago, tracking)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (orden_id) DO UPDATE SET
            subtotal=EXCLUDED.subtotal, descuento=EXCLUDED.descuento,
            envio_cobrado=EXCLUDED.envio_cobrado, total=EXCLUDED.total,
            estado_pago=EXCLUDED.estado_pago, estado_envio=EXCLUDED.estado_envio,
            tracking=EXCLUDED.tracking
    """
    ok = db_exec_many(sql, rows)
    if ok: log(f"  DB: {len(rows)} ventas guardadas")

def guardar_mp_db(lines, header, sep, i_date, i_fee, i_taxes, i_net, i_type):
    if not DATABASE_URL: return
    log("  DB: guardando MP settlement...")
    rows = []
    for line in lines[1:]:
        if not line.strip(): continue
        cols = line.split(sep)
        if len(cols) < 3: continue
        fecha_str = cols[i_date].strip().strip('"') if i_date < len(cols) else ""
        k = mes_key(fecha_str)
        if not k: continue
        try: fecha = datetime.strptime(fecha_str[:10], "%Y-%m-%d").date()
        except: continue
        source_id = cols[1].strip().strip('"') if len(cols) > 1 else ""
        tx_type   = cols[i_type].strip().strip('"') if i_type >= 0 and len(cols) > i_type else ""
        tx_amount = safe_float(cols[7]) if len(cols) > 7 else 0
        fee    = safe_float(cols[i_fee])   if i_fee   >= 0 and len(cols) > i_fee   else 0
        taxes  = safe_float(cols[i_taxes]) if i_taxes >= 0 and len(cols) > i_taxes else 0
        net    = safe_float(cols[i_net])   if i_net   >= 0 and len(cols) > i_net   else 0
        payment = cols[5].strip().strip('"') if len(cols) > 5 else ""
        rows.append((source_id, fecha, k[0], k[1], tx_type, tx_amount, fee, taxes, net, payment))
    sql = """
        INSERT INTO mp_settlement
            (source_id, fecha, anio, mes, transaction_type,
             transaction_amount, fee_amount, taxes_amount,
             settlement_net_amount, payment_method)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (source_id) DO UPDATE SET
            fee_amount=EXCLUDED.fee_amount, taxes_amount=EXCLUDED.taxes_amount,
            settlement_net_amount=EXCLUDED.settlement_net_amount
    """
    ok = db_exec_many(sql, rows)
    if ok: log(f"  DB: {len(rows)} filas MP guardadas")

def guardar_pagonube_db(datos):
    if not DATABASE_URL or not datos: return
    log("  DB: guardando PagoNube...")
    rows = []
    for row in datos:
        desc = str(row.get("Descripción", row.get("Descripcion","venta"))).lower()
        if any(x in desc for x in ["devolucion","devolución","refund","chargeback"]): continue
        fecha_str = (row.get("Fecha de creación") or row.get("Fecha de creacion",""))
        k = mes_key(fecha_str)
        if not k: continue
        try: fecha = datetime.strptime(str(fecha_str)[:10], "%d-%m-%Y").date()
        except:
            try: fecha = datetime.strptime(str(fecha_str)[:10], "%Y-%m-%d").date()
            except: continue
        tasa    = safe_float(row.get("Tasa Pago Nube", 0))
        cuota_s = safe_float(row.get("Costo de Cuota Simple", 0))
        cuota_p = safe_float(row.get("Costo de cuotas Pago Nube", 0))
        iibb    = safe_float(row.get("Impuestos - IIBB", 0))
        com     = tasa + cuota_s + cuota_p
        rows.append((
            str(row.get("Número de venta", row.get("Numero de venta",""))),
            fecha, k[0], k[1],
            str(row.get("Cliente","")), str(row.get("Medio de pago","")),
            safe_float(row.get("Monto de la venta",0)),
            tasa, cuota_s, cuota_p, iibb, com,
        ))
    sql = """
        INSERT INTO pagonube
            (numero_venta, fecha, anio, mes, cliente, medio_pago,
             monto_venta, tasa, cuota_simple, cuotas_pagonube, iibb, comision_total)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (numero_venta) DO UPDATE SET
            tasa=EXCLUDED.tasa, cuota_simple=EXCLUDED.cuota_simple,
            cuotas_pagonube=EXCLUDED.cuotas_pagonube, iibb=EXCLUDED.iibb,
            comision_total=EXCLUDED.comision_total
    """
    ok = db_exec_many(sql, rows)
    if ok: log(f"  DB: {len(rows)} movimientos PagoNube guardados")

def guardar_meta_db(datos_dict):
    if not DATABASE_URL or not datos_dict: return
    log("  DB: guardando Meta Ads...")
    rows = []
    for fecha_str, reg in datos_dict.items():
        try: fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()
        except: continue
        rows.append((
            fecha, fecha.year, fecha.month,
            safe_float(reg.get("spend",0)),
            int(reg.get("impressions",0) or 0),
            int(reg.get("clicks",0) or 0),
        ))
    sql = """
        INSERT INTO meta_ads (fecha, anio, mes, gasto, impresiones, clicks)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (fecha) DO UPDATE SET
            gasto=EXCLUDED.gasto, impresiones=EXCLUDED.impresiones, clicks=EXCLUDED.clicks
    """
    ok = db_exec_many(sql, rows)
    if ok: log(f"  DB: {len(rows)} dias Meta guardados")

def guardar_gastos_manuales_db(manuales):
    if not DATABASE_URL: return
    log("  DB: guardando gastos manuales...")
    rows = []
    mapeo = {
        "compras":       "compras",
        "sueldos":       "sueldos",
        "pub_agencia":   "agencia_publicidad",
        "ventas_manual": "ventas_manuales",
    }
    for campo, tipo in mapeo.items():
        for (anio, mes), monto in manuales.get(campo, {}).items():
            try: fecha = date(anio, mes, 1)
            except: continue
            rows.append((fecha, anio, mes, tipo, "", abs(monto)))

    # FIX: verdadero upsert con constraint única (tipo, anio, mes)
    # Requiere: ALTER TABLE gastos_manuales ADD CONSTRAINT gastos_manuales_tipo_anio_mes_key UNIQUE (tipo, anio, mes)
    sql = """
        INSERT INTO gastos_manuales (fecha, anio, mes, tipo, detalle, monto)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (tipo, anio, mes) DO UPDATE SET
            monto=EXCLUDED.monto,
            fecha=EXCLUDED.fecha
    """
    ok = db_exec_many(sql, rows)
    if ok: log(f"  DB: {len(rows)} gastos manuales guardados")

def guardar_pnl_db(periodos, tabla):
    if not DATABASE_URL: return
    log("  DB: guardando P&L mensual...")
    rows = []
    for p in periodos:
        def cat(c):
            return round(sum(tabla.get(r,{}).get(p,0) for r,_,cat in PNL_FILAS if cat==c), 2)
        ingresos = cat("Ingresos")
        egresos  = sum(cat(c) for c in CATEGORIAS_EGRESO)
        rows.append((
            p[0], p[1], ingresos,
            cat("Costo de Mercaderia"), cat("Gastos por Ventas"),
            cat("Gastos de Comercializacion"), cat("Gastos de Administracion"),
            cat("Publicidad"), cat("Impuestos"),
            round(ingresos + egresos, 2), ahora_ar(),
        ))
    sql = """
        INSERT INTO pnl_mensual
            (anio, mes, ingresos, costo_mercaderia, gastos_ventas,
             gastos_comercializacion, gastos_admin, publicidad,
             impuestos, resultado, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (anio, mes) DO UPDATE SET
            ingresos=EXCLUDED.ingresos, costo_mercaderia=EXCLUDED.costo_mercaderia,
            gastos_ventas=EXCLUDED.gastos_ventas,
            gastos_comercializacion=EXCLUDED.gastos_comercializacion,
            gastos_admin=EXCLUDED.gastos_admin, publicidad=EXCLUDED.publicidad,
            impuestos=EXCLUDED.impuestos, resultado=EXCLUDED.resultado,
            updated_at=EXCLUDED.updated_at
    """
    ok = db_exec_many(sql, rows)
    if ok: log(f"  DB: P&L {len(rows)} meses guardados")

# ═══════════════════════════════════════════════════════════════
# S3 CACHE
# ═══════════════════════════════════════════════════════════════

_s3 = None

def get_s3():
    global _s3
    if _s3 is None:
        if not S3_BUCKET: return None
        try:
            import boto3
            from botocore.config import Config
            _s3 = boto3.client(
                "s3", endpoint_url=S3_ENDPOINT,
                aws_access_key_id=S3_KEY_ID, aws_secret_access_key=S3_SECRET,
                region_name=S3_REGION, config=Config(signature_version="s3v4")
            )
        except Exception as e:
            log(f"  [WARN] S3 init: {e}")
    return _s3

def s3_leer(key):
    s3 = get_s3()
    if not s3: return None
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        if "NoSuchKey" in str(e) or "404" in str(e): return None
        log(f"  [WARN] S3 leer {key}: {e}")
        return None

def s3_guardar(key, data):
    s3 = get_s3()
    if not s3: return False
    try:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
        return True
    except Exception as e:
        log(f"  [WARN] S3 guardar {key}: {e}")
        return False

# ═══════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════

_svc = None

def get_svc():
    global _svc
    if _svc is None:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(SA_JSON),
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        _svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _svc

def leer_hoja(sheet_id, hoja, rango="A:Z", unformatted=False):
    try:
        kwargs = dict(spreadsheetId=sheet_id, range=f"'{hoja}'!{rango}")
        if unformatted: kwargs["valueRenderOption"] = "UNFORMATTED_VALUE"
        res = get_svc().spreadsheets().values().get(**kwargs).execute()
        return res.get("values", [])
    except Exception as e:
        log(f"  [WARN] leer_hoja '{hoja}': {e}")
        return []

def escribir_hoja(sheet_id, rango, valores):
    get_svc().spreadsheets().values().clear(
        spreadsheetId=sheet_id, range=rango).execute()
    get_svc().spreadsheets().values().update(
        spreadsheetId=sheet_id, range=rango,
        valueInputOption="RAW", body={"values": valores}).execute()

# ═══════════════════════════════════════════════════════════════
# FUENTE 1: TIENDANUBE
# ═══════════════════════════════════════════════════════════════

def fetch_tiendanube():
    log("Tiendanube: cargando cache S3...")
    if not TN_STORE_ID or not TN_TOKEN:
        log("  [SKIP] sin credenciales TN")
        return {}, []

    cache = s3_leer("tn_ordenes.json") or {}
    log(f"  TN cache: {len(cache)} ordenes")

    headers = {
        "Authentication": f"bearer {TN_TOKEN}",
        "User-Agent": "HechizoBijou-Reporte/1.0 (hechizobijou@gmail.com)"
    }
    base = f"https://api.tiendanube.com/v1/{TN_STORE_ID}"
    fecha_30d = (ahora_ar() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S-03:00")
    page = 1; actualizadas = 0
    while True:
        try:
            r = requests.get(f"{base}/orders", headers=headers,
                             params={"page": page, "per_page": 200, "updated_at_min": fecha_30d},
                             timeout=30)
            r.raise_for_status()
            batch = r.json()
        except Exception as e:
            log(f"  [ERROR] TN pag {page}: {e}"); break
        if not batch: break
        for o in batch:
            cache[str(o["id"])] = o
            actualizadas += 1
        log(f"  TN pag {page}: {len(batch)} ordenes (total cache {len(cache)})")
        if len(batch) < 200: break
        page += 1; time.sleep(0.5)

    log(f"  TN: {actualizadas} ordenes actualizadas (últimos 30 días)")
    if actualizadas > 0:
        if s3_guardar("tn_ordenes.json", cache):
            log(f"  TN cache guardado S3 ({len(cache)} ordenes)")

    orders = list(cache.values())
    log(f"  TN procesando {len(orders)} ordenes totales")

    acum = {k: defaultdict(float) for k in [
        "ventas_min","envio_min","dto_min",
        "ventas_may","envio_may","dto_may",
        "envio_andreani","envio_moto","envio_correo_tn","envio_otro"]}

    for o in orders:
        if o.get("status") == "cancelled": continue
        try:
            dt = datetime.fromisoformat(
                o.get("created_at","").replace("Z","+00:00")).astimezone(TZ_AR)
        except: continue
        k = (dt.year, dt.month)

        subtotal      = safe_float(o.get("subtotal", 0))
        shipping_cust = safe_float(o.get("shipping_cost_customer", 0))
        discount      = safe_float(o.get("discount", 0))
        tracking      = str(o.get("shipping_tracking_number","") or "").lower()
        medio_env     = str(o.get("shipping_option","") or "").lower()

        payment_details = o.get("payment_details") or []
        gateways = [p.get("payment_method_id","") or "" for p in payment_details if isinstance(p, dict)]
        is_may = any("transfer" in g.lower() or "deposito" in g.lower() for g in gateways)

        prefix = "may" if is_may else "min"
        acum[f"ventas_{prefix}"][k] += subtotal
        acum[f"envio_{prefix}"][k]  += shipping_cust
        acum[f"dto_{prefix}"][k]    -= discount

        if shipping_cust:
            if "36000" in tracking:           acum["envio_andreani"][k]  -= shipping_cust
            elif medio_env.startswith("mot"): acum["envio_moto"][k]      -= shipping_cust
            elif "1978" in tracking:          acum["envio_correo_tn"][k] -= shipping_cust
            else:                             acum["envio_otro"][k]       -= shipping_cust

    return {k: dict(v) for k, v in acum.items()}, orders

# ═══════════════════════════════════════════════════════════════
# FUENTE 2: MERCADOPAGO
# ═══════════════════════════════════════════════════════════════

def fetch_mercadopago():
    log("MercadoPago: descargando settlement...")
    if not MP_TOKEN or not MP_USER_ID:
        log("  [SKIP] sin credenciales MP"); return {}

    # ── Cache de 48hs — igual que el script original de KNIME ──
    cache_mp = s3_leer("mp_settlement_cache.json")
    if cache_mp:
        ts_str = cache_mp.get("timestamp","")
        try:
            ts = datetime.fromisoformat(ts_str)
            edad_hs = (ahora_ar() - ts).total_seconds() / 3600
            if edad_hs < 48:
                log(f"  MP cache válido ({edad_hs:.1f}hs < 48hs) — saltando descarga")
                # Claves guardadas como "2026,1" → convertir a tuplas (2026, 1)
                def str_keys_to_tuple(d):
                    result = {}
                    for k, v in d.items():
                        try:
                            partes = k.split(",")
                            result[(int(partes[0]), int(partes[1]))] = v
                        except Exception:
                            pass
                    return result
                return {
                    "com_mp":   str_keys_to_tuple(cache_mp.get("com_mp", {})),
                    "ret_iibb": str_keys_to_tuple(cache_mp.get("ret_iibb", {})),
                }
            else:
                log(f"  MP cache expirado ({edad_hs:.1f}hs) — descargando de nuevo")
        except Exception:
            log("  MP cache: timestamp inválido — descargando de nuevo")

    headers = {"Authorization": f"Bearer {MP_TOKEN}"}
    base    = "https://api.mercadopago.com"
    inicio  = f"{ANO}-01-01T00:00:00Z"
    fin     = f"{min(ahora_ar().date(), date(ANO,12,31)).strftime('%Y-%m-%d')}T23:59:59Z"

    report_id = None
    try:
        r = requests.post(f"{base}/v1/account/settlement_report",
                          headers=headers, json={"begin_date": inicio, "end_date": fin})
        log(f"  MP create: {r.status_code}")
        report_id = r.json().get("id")
        log(f"  MP reporte ID: {report_id}")
    except Exception as e:
        log(f"  [ERROR] MP create: {e}"); return {}

    filename = None
    for intento in range(1, 21):
        time.sleep(30)
        try:
            reportes = requests.get(f"{base}/v1/account/settlement_report/list",
                                    headers=headers, timeout=30).json()
            nuestro = next((rep for rep in reportes if rep.get("id") == report_id), None)
            if nuestro:
                status    = nuestro.get("status","")
                file_name = nuestro.get("file_name","")
                log(f"  MP intento {intento}/20: {status}")
                if file_name and status == "processed":
                    filename = file_name; break
                elif status == "error":
                    log("  [ERROR] MP reporte falló"); return {}
            else:
                log(f"  MP intento {intento}/20: esperando...")
        except Exception as e:
            log(f"  [WARN] MP polling: {e}")

    if not filename:
        log("  [WARN] MP settlement no disponible después de 10 minutos"); return {}

    try:
        content = requests.get(f"{base}/v1/account/settlement_report/{filename}",
                               headers=headers, timeout=60).text
    except Exception as e:
        log(f"  [ERROR] MP download: {e}"); return {}

    lines = content.splitlines()
    if not lines:
        log("  [WARN] MP CSV vacío"); return {}

    sep = ";"
    if lines[0].count(",") > lines[0].count(";"):
        sep = ","; log("  [WARN] MP CSV: separador detectado como coma")

    log(f"  MP CSV: {len(lines)} lineas, sep='{sep}'")
    log(f"  MP CSV header: {lines[0][:200]}")
    if len(lines) > 1: log(f"  MP CSV fila1: {lines[1][:200]}")

    header  = [c.strip().strip('"').upper() for c in lines[0].split(sep)]
    idx_map = {col: i for i, col in enumerate(header)}

    i_date  = idx_map.get("SETTLEMENT_DATE") or idx_map.get("TRANSACTION_DATE") or 0
    i_fee   = idx_map.get("FEE_AMOUNT", -1)
    i_taxes = idx_map.get("TAXES_AMOUNT", -1)
    i_net   = idx_map.get("SETTLEMENT_NET_AMOUNT", -1)
    i_type  = idx_map.get("TRANSACTION_TYPE", -1)

    com_mp = defaultdict(float); ret_iibb = defaultdict(float)
    parsed = 0
    for line in lines[1:]:
        if not line.strip(): continue
        cols = line.split(sep)
        if len(cols) < 3: continue
        fecha = cols[i_date].strip().strip('"') if i_date < len(cols) else ""
        fee   = safe_float(cols[i_fee])   if i_fee   >= 0 and i_fee   < len(cols) else 0.0
        taxes = safe_float(cols[i_taxes]) if i_taxes >= 0 and i_taxes < len(cols) else 0.0
        if not fecha: continue
        if fee:   acumular(com_mp,   fecha, fee)
        if taxes: acumular(ret_iibb, fecha, taxes)
        parsed += 1

    log(f"  MP settlement OK — {parsed} filas, com_mp={len(com_mp)} meses")

    # Guardar cache con timestamp para skip de 48hs en próximas corridas
    def dict_to_str_keys(d):
        return {f"{k[0]},{k[1]}": v for k, v in d.items()}
    s3_guardar("mp_settlement_cache.json", {
        "timestamp": ahora_ar().isoformat(),
        "com_mp":   dict_to_str_keys(com_mp),
        "ret_iibb": dict_to_str_keys(ret_iibb),
    })
    log("  MP cache guardado en S3")

    return {
        "com_mp": dict(com_mp), "ret_iibb": dict(ret_iibb),
        "_raw": {"lines": lines, "header": header, "sep": sep,
                 "i_date": i_date, "i_fee": i_fee, "i_taxes": i_taxes,
                 "i_net": i_net, "i_type": i_type},
    }

# ═══════════════════════════════════════════════════════════════
# FUENTE 3: META ADS
# ═══════════════════════════════════════════════════════════════

def _meta_descargar_periodo(fecha_desde, fecha_hasta):
    account_id = META_ACCOUNT if META_ACCOUNT.startswith("act_") else f"act_{META_ACCOUNT}"
    url = f"https://graph.facebook.com/v19.0/{account_id}/insights"
    params = {
        "access_token": META_TOKEN, "level": "account",
        "fields": "spend,impressions,clicks,date_start,account_name,account_id",
        "time_increment": 1,
        "time_range": json.dumps({"since": fecha_desde, "until": fecha_hasta}),
        "limit": 500,
    }
    resultados = []
    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        resultados.extend(data.get("data", []))
        cursor = data.get("paging",{}).get("cursors",{}).get("after")
        if not cursor: break
        params["after"] = cursor
    return resultados

def fetch_meta():
    log("Meta Ads: cargando cache S3...")
    if not META_TOKEN or not META_ACCOUNT:
        log("  [SKIP] sin credenciales Meta"); return {}

    cache_list = s3_leer("meta_gastos.json") or []
    datos_dict = {d["date_start"]: d for d in cache_list}
    log(f"  Meta cache: {len(datos_dict)} dias")

    fecha_fin = ahora_ar().date()
    fecha_ini = date(ANO, 1, 1) if not datos_dict else max(
        date(ANO, 1, 1), fecha_fin - timedelta(days=60))

    fecha_actual = fecha_ini
    while fecha_actual <= fecha_fin:
        fecha_lote_fin = min(fecha_actual + timedelta(days=89), fecha_fin)
        try:
            lote = _meta_descargar_periodo(
                fecha_actual.strftime("%Y-%m-%d"), fecha_lote_fin.strftime("%Y-%m-%d"))
            for reg in lote: datos_dict[reg["date_start"]] = reg
            log(f"  Meta {fecha_actual} -> {fecha_lote_fin}: {len(lote)} dias")
        except Exception as e:
            log(f"  [WARN] Meta lote {fecha_actual}: {e}")
        fecha_actual = fecha_lote_fin + timedelta(days=1)
        time.sleep(0.5)

    if s3_guardar("meta_gastos.json", list(datos_dict.values())):
        log(f"  Meta cache guardado S3 ({len(datos_dict)} dias)")

    gastos = defaultdict(float)
    for reg in datos_dict.values():
        spend = safe_float(reg.get("spend", 0))
        if spend:
            k = mes_key(reg.get("date_start",""))
            if k:
                mult = META_MULTIPLICADOR.get(k, META_MULTIPLICADOR_DEFAULT)
                gastos[k] -= spend * mult

    log(f"  Meta: {len(gastos)} meses")
    return {"pub_meta": dict(gastos)}

# ═══════════════════════════════════════════════════════════════
# FUENTE 4: GOOGLE ADS
# ═══════════════════════════════════════════════════════════════

def fetch_google_ads():
    log("Google Ads: leyendo desde Sheet...")
    gastos = defaultdict(float)

    for hoja in ["Historico","historico","Google Ads","Hoja 1"]:
        rows = leer_hoja(SHEET_ID_GOOGLE_ADS, hoja)
        if not rows or len(rows) < 2: continue
        h = rows[0]
        i_f    = _col_idx(h, "fecha","date","dia")
        i_tipo = _col_idx(h, "tipo","type")
        i_c    = _col_idx(h, "crédit","credit","costo","cost","importe")
        if i_f < 0 or i_c < 0:
            log(f"  Google Ads '{hoja}': columnas no encontradas en {h[:8]}"); continue

        log(f"  Google Ads '{hoja}': cols fecha={i_f} tipo={i_tipo} valor={i_c}")
        filas_ok = 0
        for row in rows[1:]:
            if i_tipo >= 0 and len(row) > i_tipo:
                tipo = str(row[i_tipo]).strip().lower()
                if tipo and "pago" not in tipo and "payment" not in tipo:
                    continue
            fecha = row[i_f] if len(row) > i_f else None
            val   = safe_float(row[i_c]) if len(row) > i_c and row[i_c] else 0.0
            if not fecha or not val: continue
            k = mes_key(fecha)
            if not k: continue
            gastos[k] -= abs(val)
            filas_ok += 1

        log(f"  Google Ads '{hoja}': {len(gastos)} meses ({filas_ok} filas Pagos)")
        break

    return {"pub_gads": dict(gastos)}

# ═══════════════════════════════════════════════════════════════
# FUENTE 5: PAGONUBE
# ═══════════════════════════════════════════════════════════════

def fetch_pagonube():
    log("PagoNube: leyendo desde S3...")
    comisiones = defaultdict(float)
    ret_iibb   = defaultdict(float)

    datos = s3_leer("pagonube.json")
    if not datos:
        log("  [WARN] pagonube.json no encontrado en S3"); return {}

    log(f"  PagoNube JSON: {len(datos)} registros")
    if datos: log(f"  PagoNube claves muestra: {list(datos[0].keys())[:8]}")

    sin_fecha = 0
    for row in datos:
        desc = str(row.get("Descripción", row.get("Descripcion","venta"))).lower()
        if any(x in desc for x in ["devolucion","devolución","refund","chargeback"]): continue
        fecha = (row.get("Fecha de creación") or row.get("Fecha de creacion")
                 or row.get("Fecha") or "")
        if not fecha: sin_fecha += 1; continue
        tasa    = safe_float(row.get("Tasa Pago Nube", 0))
        cuota_s = safe_float(row.get("Costo de Cuota Simple", 0))
        cuota_p = safe_float(row.get("Costo de cuotas Pago Nube", 0))
        iibb    = safe_float(row.get("Impuestos - IIBB", 0))
        com = tasa + cuota_s + cuota_p
        if com:   acumular(comisiones, fecha, com)
        if iibb:  acumular(ret_iibb,   fecha, iibb)

    if sin_fecha: log(f"  PagoNube: {sin_fecha} registros sin fecha ignorados")
    log(f"  PagoNube: {len(comisiones)} meses comisiones, {len(ret_iibb)} meses IIBB")
    return {"com_pagonube": dict(comisiones), "ret_iibb_pn": dict(ret_iibb)}

# ═══════════════════════════════════════════════════════════════
# FUENTE 6: MP GETNET HISTORICO
# ═══════════════════════════════════════════════════════════════

def fetch_mp_getnet_historico():
    log("MP Getnet historico: leyendo desde S3...")
    comisiones = defaultdict(float); ret_iibb = defaultdict(float)
    datos = s3_leer("mp_getnet_historico.json")
    if not datos:
        log("  [WARN] mp_getnet_historico.json no encontrado en S3"); return {}
    for row in datos:
        fecha = row.get("fecha","")
        com   = safe_float(row.get("comision", 0))
        iibb  = safe_float(row.get("iibb", 0))
        if fecha and com:  acumular(comisiones, fecha, -com)
        if fecha and iibb: acumular(ret_iibb,   fecha, -abs(iibb))
    log(f"  MP Getnet historico: {len(comisiones)} meses ({len(datos)} registros)")
    return {"com_pagonube_hist": dict(comisiones), "ret_iibb_hist": dict(ret_iibb)}

# ═══════════════════════════════════════════════════════════════
# FUENTE 7: DATOS MANUALES
# ═══════════════════════════════════════════════════════════════

def _leer_solapa(posibles_nombres, col_valor, es_ingreso, label):
    for nombre in posibles_nombres:
        rows = leer_hoja(SHEET_ID_GASTOS, nombre, unformatted=True)
        if not rows: continue
        h = [str(x).strip() for x in rows[0]]
        log(f"    '{nombre}': {len(rows)} filas | header={h[:5]}")
        if len(rows) > 1: log(f"    fila1={rows[1][:5]}")

        i_f = _col_idx(h, "fecha", "date")
        i_v = _col_idx(h, "ingreso" if es_ingreso else "egreso",
                       "entrada" if es_ingreso else "salida")
        if i_f < 0: i_f = 0
        if i_v < 0: i_v = col_valor

        primera = rows[0][i_f] if len(rows[0]) > i_f else ""
        data_rows = rows[1:] if mes_key(primera) is None else rows

        primer_anio_explicito = None
        for row in data_rows:
            f = row[i_f] if len(row) > i_f else None
            if not f: continue
            k_test = mes_key(str(f).strip(), ano_ctx=None)
            if k_test:
                primer_anio_explicito = k_test[0]; break

        if primer_anio_explicito is None:
            primer_anio_explicito = ANO

        acum = defaultdict(float)
        ultimo_anio = primer_anio_explicito

        for row in data_rows:
            f = row[i_f] if len(row) > i_f else None
            if not f or str(f).strip() in ("", "Fecha", "fecha", "FECHA"): continue
            f_str = str(f).strip()
            k = mes_key(f_str, ano_ctx=None)
            if k:
                ultimo_anio = k[0]
            else:
                k = mes_key(f_str, ano_ctx=ultimo_anio)
            if not k: continue
            v = safe_float(row[i_v]) if len(row) > i_v and row[i_v] else 0.0
            if v:
                acum[k] += v if es_ingreso else -v

        col_name = h[i_v] if i_v < len(h) else f"col{i_v}"
        log(f"    -> {label}: {len(acum)} meses ('{nombre}' col '{col_name}')")
        return dict(acum)

    log(f"    [WARN] {label}: solapa no encontrada en {posibles_nombres}")
    return {}

def fetch_manuales():
    log("Datos manuales: leyendo desde Sheet...")
    result = {}

    result["ventas_manual"] = _leer_solapa(["Ventas","ventas"], 2, True, "Ventas manuales")
    log(f"  Ventas manuales: {len(result['ventas_manual'])} meses")

    result["compras"] = _leer_solapa(
        ["Compra Materia prima - Producto","Compras","compras"], 3, False, "Compras")
    log(f"  Compras: {len(result['compras'])} meses")

    result["sueldos"] = _leer_solapa(["Sueldos","sueldos"], 3, False, "Sueldos")
    log(f"  Sueldos: {len(result['sueldos'])} meses")

    result["pub_agencia"] = _leer_solapa(["Publicidad","publicidad"], 3, False, "Agencia pub")
    log(f"  Agencia pub: {len(result['pub_agencia'])} meses")

    correo_s3 = s3_leer("correo_historico.json") or []
    correo_h  = defaultdict(float)
    for row in correo_s3:
        f = row.get("fecha",""); v = safe_float(row.get("importe", 0))
        if f and v: acumular(correo_h, f, -v)
    result["correo_hist"] = dict(correo_h)
    log(f"  Correo historico: {len(result['correo_hist'])} meses ({len(correo_s3)} registros)")

    tn_s3 = s3_leer("tn_abono.json") or []
    tn_acum = defaultdict(float)
    for row in tn_s3:
        f = row.get("fecha",""); v = safe_float(row.get("importe",0))
        if f and v: acumular(tn_acum, f, v)
    result["com_tn"] = {k: -v for k, v in tn_acum.items()}
    log(f"  TN abono: {len(result['com_tn'])} meses")
    # LOG TEMPORAL: ver detalle de TN abono para 2026
    for (anio, mes), monto in sorted(result["com_tn"].items()):
        if anio >= 2025:
            log(f"    TN abono ({anio},{mes:02d}): {monto:,.2f}")

    mono_s3 = s3_leer("monotributo.json") or []
    mono_acum = defaultdict(float)
    for row in mono_s3:
        f = row.get("fecha",""); v = safe_float(row.get("importe",0))
        if f and v: acumular(mono_acum, f, v)
    result["monotributo"] = {k: -v for k, v in mono_acum.items()}
    log(f"  Monotributo: {len(result['monotributo'])} meses")

    return result

# ═══════════════════════════════════════════════════════════════
# COMBINAR
# ═══════════════════════════════════════════════════════════════

def combinar_rubros(tn, mp, meta, gads, pagonube, mp_hist, manuales):
    datos = defaultdict(lambda: defaultdict(float))

    def merge(rubro, d):
        for k, v in d.items(): datos[rubro][k] += v

    for r in ["ventas_min","envio_min","dto_min","ventas_may","envio_may","dto_may"]:
        merge(r, tn.get(r,{}))

    merge("com_mp",   mp.get("com_mp",{}))
    merge("ret_iibb", mp.get("ret_iibb",{}))
    merge("envio_andreani", tn.get("envio_andreani",{}))
    merge("envio_moto",     tn.get("envio_moto",{}))
    merge("envio_otro",     tn.get("envio_otro",{}))

    for k, v in manuales.get("correo_hist",{}).items(): datos["envio_correo"][k] += v
    for k, v in tn.get("envio_correo_tn",{}).items():   datos["envio_correo"][k] += v

    merge("com_pagonube", mp_hist.get("com_pagonube_hist",{}))
    for k, v in pagonube.get("com_pagonube",{}).items():
        if k >= (2024, 1): datos["com_pagonube"][k] += v

    merge("ret_iibb", mp_hist.get("ret_iibb_hist",{}))
    for k, v in pagonube.get("ret_iibb_pn",{}).items():
        if k >= (2024, 1): datos["ret_iibb"][k] += v

    merge("pub_meta",    meta.get("pub_meta",{}))
    merge("pub_gads",    gads.get("pub_gads",{}))
    merge("pub_agencia", manuales.get("pub_agencia",{}))

    for r in ["ventas_manual","compras","sueldos","com_tn","monotributo"]:
        merge(r, manuales.get(r,{}))

    return {rubro: dict(meses) for rubro, meses in datos.items()}

# ═══════════════════════════════════════════════════════════════
# P&L Y ESCRITURA
# ═══════════════════════════════════════════════════════════════

def construir_pnl(datos):
    periodos = [(ANO, m) for m in range(1, 13)]
    tabla = {
        rid: {p: round(datos.get(rid,{}).get(p, 0.0), 2) for p in periodos}
        for rid, _, _ in PNL_FILAS
    }
    return periodos, tabla

def escribir_hoja1(periodos, tabla):
    log("Escribiendo Hoja 1 Nuevo...")
    filas = [["Row ID"] + [f"{y},{m}" for y, m in periodos]]

    for cat in ["Ingresos","Costo de Mercaderia","Gastos por Ventas",
                "Gastos de Comercializacion","Gastos de Administracion",
                "Publicidad","Impuestos"]:
        subtotal = [
            round(sum(tabla.get(r,{}).get(p, 0.0) for r, _, c in PNL_FILAS if c == cat), 2)
            for p in periodos
        ]
        filas.append([cat] + subtotal)

    ingresos  = {p: sum(tabla.get(r,{}).get(p,0.0) for r,_,c in PNL_FILAS if c=="Ingresos") for p in periodos}
    egresos   = {p: sum(tabla.get(r,{}).get(p,0.0) for r,_,c in PNL_FILAS if c in CATEGORIAS_EGRESO) for p in periodos}
    resultado = {p: round(ingresos[p]+egresos[p], 2) for p in periodos}
    filas.append(["Totales"] + [resultado[p] for p in periodos])

    escribir_hoja(SHEET_ID_RESUMEN, f"'Hoja 1 Nuevo'!A1:Z{len(filas)+3}", filas)
    log(f"  Hoja 1 Nuevo: {len(filas)} filas x {len(filas[0])} cols")
    log("  ─── Resumen P&L ───")
    for p in periodos:
        if ingresos[p] or egresos[p]:
            log(f"  {ANO}/{p[1]:02d}  Ing={ingresos[p]:>14,.0f}  "
                f"Egr={egresos[p]:>14,.0f}  Res={resultado[p]:>14,.0f}")

def escribir_trigger(estado, detalle):
    try:
        escribir_hoja(SHEET_ID_RESUMEN, "Trigger!A1:A2", [[estado], [detalle]])
    except Exception as e:
        log(f"  [WARN] trigger: {e}")

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    log("=" * 55)
    log(f"HECHIZO REPORTE NUEVO — año {ANO}")
    log(f"S3 bucket: {'configurado' if S3_BUCKET else 'NO configurado'}")
    log("=" * 55)

    if not SA_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON no configurada")

    try:
        tn, tn_orders = fetch_tiendanube()
        mp       = fetch_mercadopago()
        meta     = fetch_meta()
        gads     = fetch_google_ads()
        pagonube = fetch_pagonube()
        mp_hist  = fetch_mp_getnet_historico()
        manuales = fetch_manuales()

        datos           = combinar_rubros(tn, mp, meta, gads, pagonube, mp_hist, manuales)
        periodos, tabla = construir_pnl(datos)
        escribir_hoja1(periodos, tabla)

        if DATABASE_URL:
            log("Guardando en Supabase...")
            guardar_ventas_db(tn_orders)
            raw = mp.get("_raw", {})
            if raw:
                guardar_mp_db(raw["lines"], raw["header"], raw["sep"],
                              raw["i_date"], raw["i_fee"], raw["i_taxes"],
                              raw["i_net"], raw["i_type"])
            pagonube_datos = s3_leer("pagonube.json") or []
            guardar_pagonube_db(pagonube_datos)
            meta_cache = s3_leer("meta_gastos.json") or []
            guardar_meta_db({d["date_start"]: d for d in meta_cache})
            guardar_gastos_manuales_db(manuales)
            guardar_pnl_db(periodos, tabla)
            log("Supabase OK")
        else:
            log("  [SKIP] DATABASE_URL no configurada — sin Supabase")

        escribir_trigger("LISTO", f"OK {ahora_ar().strftime('%d/%m/%Y %H:%M')}")
        log("=" * 55)
        log("COMPLETADO OK")
        log("=" * 55)

    except Exception as e:
        log(f"[ERROR] {e}")
        traceback.print_exc()
        escribir_trigger("ERROR", str(e)[:200])
        raise

if __name__ == "__main__":
    main()
