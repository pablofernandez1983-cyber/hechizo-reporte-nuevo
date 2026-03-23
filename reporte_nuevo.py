"""
reporte_nuevo.py — Hechizo Bijou P&L mensual
Reemplaza el workflow KNIME.

Cache en Railway S3 Bucket:
  - tn_ordenes.json     -> ordenes de Tiendanube (acumulativo, todas)
  - meta_gastos.json    -> gastos diarios de Meta Ads
  - pagonube.json       -> movimientos de PagoNube
  - mp_getnet_historico.json -> Getnet historico ago-2023/ene-2024
  - correo_historico.json    -> Correo Argentino historico nov-2020/jul-2024
  - tn_abono.json            -> abono mensual TN
  - monotributo.json         -> monotributo mensual

Variables de entorno Railway (automaticas con Bucket):
  AWS_S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
  AWS_ENDPOINT_URL, AWS_DEFAULT_REGION

Variables requeridas:
  GOOGLE_SERVICE_ACCOUNT_JSON
  TIENDANUBE_STORE_ID, TIENDANUBE_ACCESS_TOKEN
  MP_ACCESS_TOKEN, MP_USER_ID

Variables opcionales:
  META_ACCESS_TOKEN, META_AD_ACCOUNT_ID
  SHEET_ID_RESUMEN, SHEET_ID_GASTOS, SHEET_ID_GOOGLE_ADS
  ANO_REPORTE
"""

import os, json, re, time, traceback, requests
from datetime import datetime, date, timedelta, timezone
from collections import defaultdict
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ===============================================================
# CONFIG
# ===============================================================

TZ_AR = timezone(timedelta(hours=-3))

def ahora_ar():
    return datetime.now(TZ_AR)

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

# Railway S3 Bucket (inyectadas automaticamente al agregar Bucket al proyecto)
S3_BUCKET   = os.environ.get("AWS_S3_BUCKET_NAME", "")
S3_KEY_ID   = os.environ.get("AWS_ACCESS_KEY_ID", "")
S3_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "")
S3_REGION   = os.environ.get("AWS_DEFAULT_REGION", "auto")

# --- Estructura del P&L (igual que KNIME Table Creator #240) ---
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
    ("pub_meta",       "Publicidad Meta",           "Publicidad"),
    ("pub_gads",       "Publicidad Google Ads",     "Publicidad"),
    ("pub_agencia",    "Agencia Publicidad",        "Publicidad"),
    ("ret_iibb",       "Retenciones IIBB",          "Impuestos"),
    ("monotributo",    "Monotributo",               "Impuestos"),
    ("sueldos",        "Sueldos",                   "Gastos de Administracion"),
]

CATEGORIAS_EGRESO = [
    "Costo de Mercaderia", "Gastos por Ventas", "Gastos de Comercializacion",
    "Publicidad", "Impuestos", "Gastos de Administracion",
]


# ===============================================================
# HELPERS
# ===============================================================

def log(msg):
    print(f"[{ahora_ar().strftime('%H:%M:%S')}] {msg}", flush=True)

def mes_key(fecha):
    """Cualquier fecha -> (año, mes) tuple o None.
    Soporta: datetime/date, YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY,
             formato Google Ads 'YYYY Mxx d' (ej: '2026 M02 9').
    """
    if isinstance(fecha, (datetime, date)):
        return (fecha.year, fecha.month)
    s = str(fecha).strip()
    # Formato Google Ads: "2026 M02 9" o "2026 M12 31"
    m = re.match(r"(\d{4})\s+M(\d{2})", s)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    # Formatos estandar
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
    """Convierte cualquier valor numerico a float.
    Soporta formatos: 1.234,56 (EU), 1,234.56 (US/ARS), ARS1,234.56, $1.234,56, etc.
    """
    if v is None or v == "":
        return 0.0
    s = str(v).strip()
    # Preservar signo negativo original (puede estar antes del prefijo de moneda)
    is_neg = s.startswith('-')
    # Quitar prefijos de moneda: ARS, USD, $, letras, espacios
    s = re.sub(r'[A-Za-z\$\s]', '', s)
    # Restaurar signo si habia un guion antes de la moneda
    if is_neg and not s.startswith('-'):
        s = '-' + s.lstrip('-')
    if not s or s in ('-', '+', '--'):
        return 0.0
    # Detectar formato por posicion relativa del ultimo , y ultimo .
    last_comma = s.rfind(',')
    last_dot   = s.rfind('.')
    if last_comma > 0 and last_dot > 0:
        if last_dot > last_comma:
            # Formato US/ARS: 1,234.56 -> quitar comas, conservar punto decimal
            s = s.replace(',', '')
        else:
            # Formato EU: 1.234,56 -> quitar puntos, coma es decimal
            s = s.replace('.', '').replace(',', '.')
    elif last_comma > 0:
        # Solo coma: si hay <= 2 digitos despues es decimal, si no es miles
        after = s[last_comma+1:]
        if len(after) <= 2:
            s = s.replace(',', '.')
        else:
            s = s.replace(',', '')
    # Si solo punto: ya es correcto
    try:
        return float(s)
    except:
        return 0.0

def _col_idx(header_list, *keywords):
    """Encuentra indice de columna buscando keywords en el header (case-insensitive)."""
    h = [str(x).strip().lower() for x in header_list]
    for kw in keywords:
        for i, c in enumerate(h):
            if kw.lower() in c:
                return i
    return -1


# ===============================================================
# RAILWAY S3 CACHE
# ===============================================================

_s3 = None

def get_s3():
    global _s3
    if _s3 is None:
        if not S3_BUCKET:
            return None
        try:
            import boto3
            from botocore.config import Config
            _s3 = boto3.client(
                "s3",
                endpoint_url=S3_ENDPOINT,
                aws_access_key_id=S3_KEY_ID,
                aws_secret_access_key=S3_SECRET,
                region_name=S3_REGION,
                config=Config(signature_version="s3v4")
            )
        except Exception as e:
            log(f"  [WARN] S3 init: {e}")
            return None
    return _s3

def s3_leer(key):
    """Lee un JSON del bucket. Retorna dict/list o None si no existe."""
    s3 = get_s3()
    if not s3:
        return None
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        if "NoSuchKey" in str(e) or "404" in str(e):
            return None
        log(f"  [WARN] S3 leer {key}: {e}")
        return None

def s3_guardar(key, data):
    """Guarda un dict/list como JSON en el bucket."""
    s3 = get_s3()
    if not s3:
        return False
    try:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body,
                      ContentType="application/json")
        return True
    except Exception as e:
        log(f"  [WARN] S3 guardar {key}: {e}")
        return False


# ===============================================================
# GOOGLE SHEETS
# ===============================================================

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

def leer_hoja(sheet_id, hoja, rango="A:Z"):
    try:
        res = get_svc().spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"'{hoja}'!{rango}"
        ).execute()
        return res.get("values", [])
    except Exception as e:
        log(f"  [WARN] leer_hoja '{hoja}': {e}")
        return []

def escribir_hoja(sheet_id, rango, valores):
    get_svc().spreadsheets().values().clear(
        spreadsheetId=sheet_id, range=rango
    ).execute()
    get_svc().spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=rango,
        valueInputOption="RAW",
        body={"values": valores}
    ).execute()


# ===============================================================
# FUENTE 1: TIENDANUBE API con cache S3
# Igual que KNIME: payment_status=paid,authorized, skip cancelled
# Minorista vs Mayorista: detectado por payment_method_id (transfer/deposito)
# Cache acumulativo por ID; refresca ultimos 30 dias por updated_at_min
# NOTA: created_at_min limita a ANO. Sacar para historico completo.
# ===============================================================

def fetch_tiendanube():
    log("Tiendanube: cargando cache S3...")
    if not TN_STORE_ID or not TN_TOKEN:
        log("  [SKIP] sin credenciales TN")
        return {}

    cache = s3_leer("tn_ordenes.json") or {}
    log(f"  TN cache: {len(cache)} ordenes")

    since_id = max((int(k) for k in cache.keys()), default=None)
    if since_id:
        log(f"  TN bajando desde ID > {since_id}")

    headers = {
        "Authentication": f"bearer {TN_TOKEN}",
        "User-Agent": "HechizoBijou-Reporte/1.0 (hechizobijou@gmail.com)"
    }
    base = f"https://api.tiendanube.com/v1/{TN_STORE_ID}"

    nuevas = 0
    page = 1

    # Paso 1: bajar ordenes NUEVAS (since_id > max conocido)
    # NOTA: created_at_min limita a solo ANO. Sacar para historico completo.
    while True:
        params = {"page": page, "per_page": 200,
                  "payment_status": "paid,authorized",
                  "created_at_min": f"{ANO}-01-01T00:00:00-03:00"}
        if since_id:
            params["since_id"] = since_id

        try:
            r = requests.get(f"{base}/orders", headers=headers,
                             params=params, timeout=30)
            r.raise_for_status()
            batch = r.json()
        except Exception as e:
            log(f"  [ERROR] TN pag {page}: {e}")
            break

        if not batch:
            break

        for o in batch:
            cache[str(o["id"])] = o
            nuevas += 1

        log(f"  TN pag {page}: {len(batch)} nuevas (total cache {len(cache)})")
        if len(batch) < 200:
            break
        page += 1
        time.sleep(0.3)

    # Paso 2: refrescar ultimos 30 dias para capturar cambios de estado
    fecha_30d = (ahora_ar() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S-03:00")
    page2 = 1
    actualizadas = 0
    while True:
        params2 = {"page": page2, "per_page": 200,
                   "updated_at_min": fecha_30d}
        try:
            r = requests.get(f"{base}/orders", headers=headers,
                             params=params2, timeout=30)
            r.raise_for_status()
            batch2 = r.json()
        except Exception as e:
            log(f"  [ERROR] TN refresh pag {page2}: {e}")
            break

        if not batch2:
            break

        for o in batch2:
            cache[str(o["id"])] = o
            actualizadas += 1

        if len(batch2) < 200:
            break
        page2 += 1
        time.sleep(0.3)

    log(f"  TN: {nuevas} nuevas + {actualizadas} actualizadas (ultimos 30 dias)")

    if nuevas > 0 or actualizadas > 0:
        if s3_guardar("tn_ordenes.json", cache):
            log(f"  TN cache guardado en S3 ({len(cache)} ordenes)")
        else:
            log("  [WARN] TN cache NO guardado (S3 no disponible)")

    orders = list(cache.values())
    log(f"  TN procesando {len(orders)} ordenes totales")

    acum = {k: defaultdict(float) for k in
            ["ventas_min", "envio_min", "dto_min",
             "ventas_may", "envio_may", "dto_may"]}

    for o in orders:
        # KNIME: skip cancelled
        if o.get("status") == "cancelled":
            continue
        try:
            dt = datetime.fromisoformat(
                o.get("created_at", "").replace("Z", "+00:00")
            ).astimezone(TZ_AR)
        except:
            continue
        k = (dt.year, dt.month)

        subtotal = safe_float(o.get("subtotal", 0))
        shipping = safe_float(o.get("shipping_cost_owner", 0))
        discount = safe_float(o.get("discount", 0))

        # KNIME: minorista vs mayorista segun payment_method_id
        # Mayorista = transferencia bancaria o deposito
        payment_details = o.get("payment_details") or []
        gateways = [
            p.get("payment_method_id", "") or ""
            for p in payment_details
            if isinstance(p, dict)
        ]
        is_may = any("transfer" in g.lower() or "deposito" in g.lower()
                     for g in gateways)

        prefix = "may" if is_may else "min"
        acum[f"ventas_{prefix}"][k] += subtotal
        acum[f"envio_{prefix}"][k]  += shipping
        acum[f"dto_{prefix}"][k]    -= discount

    return {k: dict(v) for k, v in acum.items()}


# ===============================================================
# FUENTE 2: MERCADOPAGO settlement
# Clasifica movimientos por descripcion (igual que KNIME)
# Polling igual que mercadopago_ventas.py: 20 intentos x 30s
# ===============================================================

_KW_CORREO   = ["correo argentino", "correo_argentino", "envio_correo", "correo arg", "oca"]
_KW_ANDREANI = ["andreani"]
_KW_IIBB     = ["retencion", "retención", "iibb", "ingresos brutos",
                "percepcion", "withholding", "sirtac"]
_KW_COM_MP   = ["comision", "commission", "fee", "cargo_financiero"]

def _clasificar_mp(desc):
    d = (desc or "").lower()
    if any(k in d for k in _KW_IIBB):      return "ret_iibb"
    if any(k in d for k in _KW_CORREO):    return "envio_correo"
    if any(k in d for k in _KW_ANDREANI):  return "envio_andreani"
    if any(k in d for k in _KW_COM_MP):    return "com_mp"
    return None

def fetch_mercadopago():
    log("MercadoPago: descargando settlement...")
    if not MP_TOKEN or not MP_USER_ID:
        log("  [SKIP] sin credenciales MP")
        return {}

    headers = {"Authorization": f"Bearer {MP_TOKEN}"}
    base    = "https://api.mercadopago.com"
    inicio  = f"{ANO}-01-01T00:00:00Z"
    fin     = f"{min(ahora_ar().date(), date(ANO,12,31)).strftime('%Y-%m-%d')}T23:59:59Z"

    # Solicitar generacion del reporte y capturar su ID
    report_id = None
    try:
        r = requests.post(f"{base}/v1/account/settlement_report", headers=headers,
                          json={"begin_date": inicio, "end_date": fin})
        log(f"  MP create: {r.status_code}")
        report_id = r.json().get("id")
        log(f"  MP reporte ID: {report_id}")
    except Exception as e:
        log(f"  [ERROR] MP create: {e}")
        return {}

    # Polling: 20 intentos x 30s = 10 minutos (igual que mercadopago_ventas.py)
    filename = None
    for intento in range(1, 21):
        time.sleep(30)
        try:
            reportes = requests.get(f"{base}/v1/account/settlement_report/list",
                                    headers=headers, timeout=30).json()
            # Buscar por ID especifico (igual que el script original)
            nuestro = next(
                (rep for rep in reportes if rep.get("id") == report_id),
                None
            )
            if nuestro:
                status    = nuestro.get("status", "")
                file_name = nuestro.get("file_name", "")
                log(f"  MP intento {intento}/20: {status}")
                if file_name and status == "processed":
                    filename = file_name
                    break
                elif status == "error":
                    log("  [ERROR] MP reporte fallo en la generacion")
                    return {}
            else:
                log(f"  MP intento {intento}/20: esperando...")
        except Exception as e:
            log(f"  [WARN] MP polling: {e}")

    if not filename:
        log("  [WARN] MP settlement no disponible despues de 10 minutos")
        return {}

    try:
        content = requests.get(
            f"{base}/v1/account/settlement_report/{filename}",
            headers=headers, timeout=60
        ).text
    except Exception as e:
        log(f"  [ERROR] MP download: {e}")
        return {}

    acum = {k: defaultdict(float) for k in
            ["com_mp", "envio_correo", "envio_andreani", "ret_iibb"]}

    lines = content.splitlines()
    if not lines:
        return {}
    h = [x.strip().upper() for x in lines[0].split(",")]
    i_date = next((i for i, c in enumerate(h) if "DATE" in c or "FECHA" in c), 0)
    i_desc = next((i for i, c in enumerate(h) if "DESC" in c), 3)
    i_net  = next((i for i, c in enumerate(h) if "NET_CREDIT" in c or "NET_DEBIT" in c
                   or "IMPORTE_NETO" in c), 7)

    for line in lines[1:]:
        cols = line.split(",")
        if len(cols) <= max(i_date, i_net):
            continue
        fecha = cols[i_date].strip().strip('"')
        desc  = cols[i_desc].strip().strip('"') if len(cols) > i_desc else ""
        monto = safe_float(cols[i_net]) if len(cols) > i_net else 0.0
        cat   = _clasificar_mp(desc)
        if cat and monto:
            acumular(acum[cat], fecha, monto)

    log("  MP settlement OK")
    return {k: dict(v) for k, v in acum.items()}


# ===============================================================
# FUENTE 3: META ADS con cache S3
# Replica logica de meta_ads_facturacion.py
# ===============================================================

def _meta_descargar_periodo(fecha_desde, fecha_hasta):
    # No duplicar prefijo act_
    account_id = META_ACCOUNT if META_ACCOUNT.startswith("act_") else f"act_{META_ACCOUNT}"
    url = f"https://graph.facebook.com/v19.0/{account_id}/insights"
    params = {
        "access_token": META_TOKEN,
        "level": "account",
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
        cursor = data.get("paging", {}).get("cursors", {}).get("after")
        if not cursor:
            break
        params["after"] = cursor
    return resultados

def fetch_meta():
    log("Meta Ads: cargando cache S3...")
    if not META_TOKEN or not META_ACCOUNT:
        log("  [SKIP] sin credenciales Meta")
        return {}

    cache_list = s3_leer("meta_gastos.json") or []
    datos_dict = {d["date_start"]: d for d in cache_list}
    log(f"  Meta cache: {len(datos_dict)} dias")

    fecha_fin = ahora_ar().date()
    fecha_ini = date(ANO, 1, 1) if not datos_dict else max(
        date(ANO, 1, 1), fecha_fin - timedelta(days=60)
    )

    nuevos = 0
    fecha_actual = fecha_ini
    while fecha_actual <= fecha_fin:
        fecha_lote_fin = min(fecha_actual + timedelta(days=89), fecha_fin)
        try:
            lote = _meta_descargar_periodo(
                fecha_actual.strftime("%Y-%m-%d"),
                fecha_lote_fin.strftime("%Y-%m-%d")
            )
            for reg in lote:
                datos_dict[reg["date_start"]] = reg
                nuevos += 1
            log(f"  Meta {fecha_actual} -> {fecha_lote_fin}: {len(lote)} dias")
        except Exception as e:
            log(f"  [WARN] Meta lote {fecha_actual}: {e}")
        fecha_actual = fecha_lote_fin + timedelta(days=1)
        time.sleep(0.5)

    if nuevos > 0:
        if s3_guardar("meta_gastos.json", list(datos_dict.values())):
            log(f"  Meta cache guardado en S3 ({len(datos_dict)} dias)")

    gastos = defaultdict(float)
    for reg in datos_dict.values():
        spend = safe_float(reg.get("spend", 0))
        if spend:
            acumular(gastos, reg.get("date_start", ""), -spend)

    log(f"  Meta: {len(gastos)} meses")
    return {"pub_meta": dict(gastos)}


# ===============================================================
# FUENTE 4: GOOGLE ADS (desde Sheet "Historico")
#
# KNIME filtraba SOLO filas tipo "Campañas"/"Campaigns".
# Las filas de "Impuestos y tarifas" (Taxes) y "Ajustes" (Adjustments)
# NO se incluyen — igual que en KNIME.
#
# Columnas del Sheet Historico:
#   Fecha ("2026 M02 9"), Tipo, Descripcion, Interacciones, Costos ("ARS19,729.51"), Creditos, Saldo actual
#
# safe_float maneja el prefijo ARS correctamente.
# mes_key maneja el formato "YYYY Mxx d" correctamente.
# ===============================================================

def fetch_google_ads():
    log("Google Ads: leyendo desde Sheet...")
    gastos = defaultdict(float)

    for hoja in ["Historico", "historico", "Google Ads", "Hoja 1"]:
        rows = leer_hoja(SHEET_ID_GOOGLE_ADS, hoja)
        if not rows or len(rows) < 2:
            continue
        h = rows[0]
        i_f    = _col_idx(h, "fecha", "date", "dia")
        i_tipo = _col_idx(h, "tipo", "type")
        i_c    = _col_idx(h, "costo", "cost", "importe", "gasto", "spend")
        if i_f < 0 or i_c < 0:
            log(f"  Google Ads hoja '{hoja}': no se encontraron columnas Fecha/Costos")
            continue

        filas_ok = 0
        for row in rows[1:]:
            # KNIME: solo filas de tipo "Campaigns"/"Campañas"
            # Ignorar: "Taxes and fees"/"Impuestos y tarifas", "Adjustments"/"Ajustes"
            if i_tipo >= 0 and len(row) > i_tipo:
                tipo = str(row[i_tipo]).strip().lower()
                if "campaign" not in tipo and "campa" not in tipo:
                    continue  # skip Taxes, Adjustments, etc.

            fecha = row[i_f] if len(row) > i_f else None
            val   = safe_float(row[i_c]) if len(row) > i_c and row[i_c] else 0.0
            if fecha and val and val != 0.0:
                acumular(gastos, fecha, -abs(val))  # siempre egreso
                filas_ok += 1

        log(f"  Google Ads '{hoja}': {len(gastos)} meses ({filas_ok} filas Campaigns)")
        break

    return {"pub_gads": dict(gastos)}


# ===============================================================
# FUENTE 5: PAGONUBE — desde S3 (pagonube.json)
# Columnas originales del CSV de PagoNube (con tildes preservadas en JSON)
# Comision = Tasa + Costo Cuota Simple + Costo Cuotas PagoNube (ya negativos)
# Skip devoluciones/refunds
# ===============================================================

def fetch_pagonube():
    log("PagoNube: leyendo desde S3...")
    comisiones = defaultdict(float)
    ret_iibb   = defaultdict(float)

    datos = s3_leer("pagonube.json")
    if not datos:
        log("  [WARN] pagonube.json no encontrado en S3")
        return {}

    for row in datos:
        # Skip devoluciones (igual que KNIME)
        desc = str(row.get("Descripción", row.get("Descripcion", "venta"))).lower()
        if any(x in desc for x in ["devolucion", "devolución", "refund", "chargeback"]):
            continue

        fecha = row.get("Fecha de creación", row.get("Fecha de creacion", ""))
        if not fecha:
            continue

        tasa    = safe_float(row.get("Tasa Pago Nube", 0))
        cuota_s = safe_float(row.get("Costo de Cuota Simple", 0))
        cuota_p = safe_float(row.get("Costo de cuotas Pago Nube", 0))
        iibb    = safe_float(row.get("Impuestos - IIBB", 0))

        # Los valores de tasa/cuotas ya vienen negativos en el CSV
        com = tasa + cuota_s + cuota_p
        if com:
            acumular(comisiones, fecha, com)
        if iibb:
            acumular(ret_iibb, fecha, iibb)

    log(f"  PagoNube: {len(comisiones)} meses ({len(datos)} registros)")
    return {"com_pagonube": dict(comisiones), "ret_iibb_pn": dict(ret_iibb)}


# ===============================================================
# FUENTE 6: MP GETNET HISTORICO — desde S3 (mp_getnet_historico.json)
# ago-2023 a ene-2024 (de los sales*.xls exportados de Getnet/MP)
# ===============================================================

def fetch_mp_getnet_historico():
    log("MP Getnet historico: leyendo desde S3...")
    comisiones = defaultdict(float)
    ret_iibb   = defaultdict(float)

    datos = s3_leer("mp_getnet_historico.json")
    if not datos:
        log("  [WARN] mp_getnet_historico.json no encontrado en S3")
        return {}

    for row in datos:
        fecha = row.get("fecha", "")
        com   = safe_float(row.get("comision", 0))
        iibb  = safe_float(row.get("iibb", 0))
        if fecha and com:
            acumular(comisiones, fecha, -com)   # egreso -> negativo
        if fecha and iibb:
            acumular(ret_iibb, fecha, -abs(iibb))

    log(f"  MP Getnet historico: {len(comisiones)} meses ({len(datos)} registros)")
    return {"com_pagonube_hist": dict(comisiones), "ret_iibb_hist": dict(ret_iibb)}


# ===============================================================
# FUENTE 7: DATOS MANUALES (Sheet "Ingresos y Gastos")
# Solapas: Ventas (col2=Ingreso), Compra Materia prima (col3=Egreso),
#          Sueldos (col3=Egreso), Publicidad (col3=Egreso)
# Historicos desde S3: correo_historico, tn_abono, monotributo
# ===============================================================

def fetch_manuales():
    log("Datos manuales: leyendo desde Sheet...")
    result = {}

    def egreso(hoja, col_f=0, col_e=3):
        """Lee solapa y suma col_e como egreso (negativo en P&L)."""
        rows = leer_hoja(SHEET_ID_GASTOS, hoja)
        acum = defaultdict(float)
        for row in rows[1:]:
            f = row[col_f] if len(row) > col_f else None
            v = safe_float(row[col_e]) if len(row) > col_e and row[col_e] else 0.0
            if f and v:
                acumular(acum, f, -v)
        return dict(acum)

    def ingreso(hoja, col_f=0, col_i=2):
        """Lee solapa y suma col_i como ingreso (positivo en P&L)."""
        rows = leer_hoja(SHEET_ID_GASTOS, hoja)
        acum = defaultdict(float)
        for row in rows[1:]:
            f = row[col_f] if len(row) > col_f else None
            v = safe_float(row[col_i]) if len(row) > col_i and row[col_i] else 0.0
            if f and v:
                acumular(acum, f, v)
        return dict(acum)

    # Ventas manuales: col 0=Fecha, col 2=Ingreso
    result["ventas_manual"] = ingreso("Ventas")
    log(f"  Ventas manuales: {len(result['ventas_manual'])} meses")

    # Compras: col 0=Fecha, col 3=Egreso
    result["compras"] = egreso("Compra Materia prima - Producto")
    log(f"  Compras: {len(result['compras'])} meses")

    # Sueldos: col 0=Fecha, col 3=Egreso
    result["sueldos"] = egreso("Sueldos")
    log(f"  Sueldos: {len(result['sueldos'])} meses")

    # Agencia Publicidad: col 0=Fecha, col 3=Egreso
    result["pub_agencia"] = egreso("Publicidad")
    log(f"  Agencia pub: {len(result['pub_agencia'])} meses")

    # Correo historico desde S3 (nov-2020 a jul-2024)
    # importe positivo=factura (egreso), negativo=nota credito (reduce costo)
    correo_s3 = s3_leer("correo_historico.json") or []
    correo_h = defaultdict(float)
    for row in correo_s3:
        f = row.get("fecha", "")
        v = safe_float(row.get("importe", 0))
        if f and v:
            acumular(correo_h, f, -v)  # factura positiva -> egreso negativo en P&L
    result["correo_hist"] = dict(correo_h)
    log(f"  Correo historico: {len(result['correo_hist'])} meses ({len(correo_s3)} registros)")

    # TN abono desde S3
    tn_s3 = s3_leer("tn_abono.json") or []
    tn_acum = defaultdict(float)
    for row in tn_s3:
        f = row.get("fecha", "")
        v = safe_float(row.get("importe", 0))
        if f and v:
            acumular(tn_acum, f, v)
    result["com_tn"] = {k: -v for k, v in tn_acum.items()}
    log(f"  TN abono: {len(result['com_tn'])} meses")

    # Monotributo desde S3
    mono_s3 = s3_leer("monotributo.json") or []
    mono_acum = defaultdict(float)
    for row in mono_s3:
        f = row.get("fecha", "")
        v = safe_float(row.get("importe", 0))
        if f and v:
            acumular(mono_acum, f, v)
    result["monotributo"] = {k: -v for k, v in mono_acum.items()}
    log(f"  Monotributo: {len(result['monotributo'])} meses")

    return result


# ===============================================================
# COMBINAR TODAS LAS FUENTES
# Replica la logica de join del KNIME:
# - Correo Argentino: historico hasta jul-2024, MP desde ago-2024
# - PagoNube/Getnet: historico sales*.xls hasta ene-2024, PagoNube CSV desde ene-2024
# ===============================================================

def combinar_rubros(tn, mp, meta, gads, pagonube, mp_hist, manuales):
    datos = defaultdict(lambda: defaultdict(float))

    def merge(rubro, d):
        for k, v in d.items():
            datos[rubro][k] += v

    # Tiendanube: ventas min/may, envios, descuentos
    for r in ["ventas_min", "envio_min", "dto_min", "ventas_may", "envio_may", "dto_may"]:
        merge(r, tn.get(r, {}))

    # MercadoPago settlement
    merge("com_mp",         mp.get("com_mp", {}))
    merge("envio_andreani", mp.get("envio_andreani", {}))
    merge("ret_iibb",       mp.get("ret_iibb", {}))

    # Correo Argentino:
    #   - Historico (Sheet/S3): cubre nov-2020 a jul-2024
    #   - MP settlement: cubre ago-2024 en adelante
    for k, v in manuales.get("correo_hist", {}).items():
        datos["envio_correo"][k] += v
    for k, v in mp.get("envio_correo", {}).items():
        # Solo agregar si no hay dato historico para ese mes, o si es ago-2024+
        if k >= (2024, 8) or k not in datos["envio_correo"]:
            datos["envio_correo"][k] += v

    # PagoNube / Getnet:
    #   - Historico sales*.xls (S3): ago-2023 a ene-2024
    #   - PagoNube CSV (S3): ene-2024 en adelante
    merge("com_pagonube", mp_hist.get("com_pagonube_hist", {}))
    merge("ret_iibb",     mp_hist.get("ret_iibb_hist", {}))
    for k, v in pagonube.get("com_pagonube", {}).items():
        if k >= (2024, 1):
            datos["com_pagonube"][k] += v
    for k, v in pagonube.get("ret_iibb_pn", {}).items():
        if k >= (2024, 1):
            datos["ret_iibb"][k] += v

    # Publicidad
    merge("pub_meta",    meta.get("pub_meta", {}))
    merge("pub_gads",    gads.get("pub_gads", {}))
    merge("pub_agencia", manuales.get("pub_agencia", {}))

    # Manuales
    for r in ["ventas_manual", "compras", "sueldos", "com_tn", "monotributo"]:
        merge(r, manuales.get(r, {}))

    return {rubro: dict(meses) for rubro, meses in datos.items()}


# ===============================================================
# CONSTRUIR Y ESCRIBIR P&L
# ===============================================================

def construir_pnl(datos):
    periodos = [(ANO, m) for m in range(1, 13)]
    tabla = {
        rid: {p: round(datos.get(rid, {}).get(p, 0.0), 2) for p in periodos}
        for rid, _, _ in PNL_FILAS
    }
    return periodos, tabla

def escribir_hoja1(periodos, tabla):
    log("Escribiendo Hoja 1 Nuevo...")

    filas = [["Row ID"] + [f"{y},{m}" for y, m in periodos]]

    # KNIME escribe solo subtotales por categoria (imagen de referencia),
    # NO filas individuales por rubro.
    categorias_orden = [
        "Ingresos",
        "Costo de Mercaderia",
        "Gastos por Ventas",
        "Gastos de Comercializacion",
        "Gastos de Administracion",
        "Publicidad",
        "Impuestos",
    ]

    for cat in categorias_orden:
        subtotal = [
            round(sum(tabla.get(r, {}).get(p, 0.0)
                      for r, _, c in PNL_FILAS if c == cat), 2)
            for p in periodos
        ]
        filas.append([cat] + subtotal)

    ingresos = {p: sum(tabla.get(r, {}).get(p, 0.0)
                       for r, _, c in PNL_FILAS if c == "Ingresos")
                for p in periodos}
    egresos  = {p: sum(tabla.get(r, {}).get(p, 0.0)
                       for r, _, c in PNL_FILAS if c in CATEGORIAS_EGRESO)
                for p in periodos}
    resultado = {p: round(ingresos[p] + egresos[p], 2) for p in periodos}

    filas.append(["Totales"] + [resultado[p] for p in periodos])

    escribir_hoja(SHEET_ID_RESUMEN, f"'Hoja 1 Nuevo'!A1:Z{len(filas)+3}", filas)
    log(f"  Hoja 1 Nuevo: {len(filas)} filas x {len(filas[0])} cols")

    log("  --- Resumen P&L ---")
    for p in periodos:
        if ingresos[p] or egresos[p]:
            log(f"  {ANO}/{p[1]:02d}  Ing={ingresos[p]:>14,.0f}  "
                f"Egr={egresos[p]:>14,.0f}  Res={resultado[p]:>14,.0f}")


# ===============================================================
# TRIGGER
# ===============================================================

def escribir_trigger(estado, detalle):
    try:
        escribir_hoja(SHEET_ID_RESUMEN, "Trigger!A1:A2",
                      [[estado], [detalle]])
    except Exception as e:
        log(f"  [WARN] trigger: {e}")


# ===============================================================
# MAIN
# ===============================================================

def main():
    log("=" * 55)
    log(f"HECHIZO REPORTE NUEVO — año {ANO}")
    log(f"S3 bucket: {'configurado' if S3_BUCKET else 'NO configurado'}")
    log("=" * 55)

    if not SA_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON no configurada")

    try:
        tn       = fetch_tiendanube()
        mp       = fetch_mercadopago()
        meta     = fetch_meta()
        gads     = fetch_google_ads()
        pagonube = fetch_pagonube()
        mp_hist  = fetch_mp_getnet_historico()
        manuales = fetch_manuales()

        datos           = combinar_rubros(tn, mp, meta, gads, pagonube, mp_hist, manuales)
        periodos, tabla = construir_pnl(datos)
        escribir_hoja1(periodos, tabla)

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
