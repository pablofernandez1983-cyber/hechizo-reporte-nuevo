"""
reporte_nuevo.py — Hechizo Bijou P&L mensual
Reemplaza el workflow KNIME. No toca ningún archivo existente.

Fuentes de datos:
  1. Tiendanube API          → Ventas, Envíos, Descuentos
  2. MercadoPago settlement  → Comisiones MP, Andreani, Correo (post ago-2024), Retenciones
  3. Meta Ads API            → Publicidad Meta
  4. Google Sheets (Google Ads Historico) → Publicidad Google Ads
  5. Google Sheets (Ingresos y Gastos):
       Ventas, Compra Materia prima - Producto, Sueldos, Publicidad,
       Correo_argentino, Tiendanube_abono, Monotributo,
       MP_Getnet_Historico, PagoNube

Variables de entorno requeridas:
  GOOGLE_SERVICE_ACCOUNT_JSON
  TIENDANUBE_STORE_ID, TIENDANUBE_ACCESS_TOKEN
  MP_ACCESS_TOKEN, MP_USER_ID

Variables opcionales:
  META_ACCESS_TOKEN, META_AD_ACCOUNT_ID
  SHEET_ID_RESUMEN, SHEET_ID_GASTOS, SHEET_ID_GOOGLE_ADS
  ANO_REPORTE
"""

import os, json, time, traceback, requests
from datetime import datetime, date, timedelta, timezone
from collections import defaultdict
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

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

META_JSON_PATH = "/tmp/meta_gastos.json"

# ─── Estructura del P&L ─────────────────────────────────────
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


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def log(msg):
    print(f"[{ahora_ar().strftime('%H:%M:%S')}] {msg}", flush=True)

def mes_key(fecha):
    """Cualquier fecha → (año, mes) tuple o None."""
    if isinstance(fecha, (datetime, date)):
        return (fecha.year, fecha.month)
    s = str(fecha).strip()
    # Formatos con hora: tomar solo los 10 primeros chars
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
    s = str(v).strip().replace("$", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0


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

def leer_hoja_2col(sheet_id, hoja, col_fecha=0, col_valor=1):
    rows = leer_hoja(sheet_id, hoja)
    acum = defaultdict(float)
    for row in rows[1:]:
        fecha = row[col_fecha] if len(row) > col_fecha else None
        val   = safe_float(row[col_valor]) if len(row) > col_valor and row[col_valor] else 0.0
        if fecha and val:
            acumular(acum, fecha, val)
    return acum

def _col_idx(header_list, *keywords):
    """Encuentra índice de columna buscando keywords en el header."""
    h = [str(x).strip().lower() for x in header_list]
    for kw in keywords:
        for i, c in enumerate(h):
            if kw.lower() in c:
                return i
    return -1


# ═══════════════════════════════════════════════════════════════
# FUENTE 1: TIENDANUBE API
# ═══════════════════════════════════════════════════════════════

def fetch_tiendanube():
    log("Tiendanube: descargando órdenes...")
    if not TN_STORE_ID or not TN_TOKEN:
        log("  [SKIP] sin credenciales TN")
        return {}

    headers = {
        "Authentication": f"bearer {TN_TOKEN}",
        "User-Agent": "HechizoBijou-Reporte/1.0 (hechizobijou@gmail.com)"
    }
    base = f"https://api.tiendanube.com/v1/{TN_STORE_ID}"

    orders, page = [], 1
    while True:
        try:
            r = requests.get(
                f"{base}/orders", headers=headers,
                params={"page": page, "per_page": 200,
                        "payment_status": "paid,authorized"},
                timeout=30
            )
            r.raise_for_status()
            batch = r.json()
        except Exception as e:
            log(f"  [ERROR] TN pág {page}: {e}"); break
        if not batch:
            break
        orders.extend(batch)
        log(f"  TN pág {page}: {len(batch)} (total {len(orders)})")
        if len(batch) < 200:
            break
        page += 1
        time.sleep(0.3)

    log(f"  Total TN: {len(orders)} órdenes")

    acum = {k: defaultdict(float) for k in
            ["ventas_min", "envio_min", "dto_min",
             "ventas_may", "envio_may", "dto_may"]}

    for o in orders:
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

        gateways = [p.get("payment_method_id", "") or ""
                    for p in (o.get("payment_details") or [])]
        is_may = any("transfer" in g.lower() or "deposito" in g.lower()
                     for g in gateways)

        prefix = "may" if is_may else "min"
        acum[f"ventas_{prefix}"][k] += subtotal
        acum[f"envio_{prefix}"][k]  += shipping
        acum[f"dto_{prefix}"][k]    -= discount

    return {k: dict(v) for k, v in acum.items()}


# ═══════════════════════════════════════════════════════════════
# FUENTE 2: MERCADOPAGO settlement
# ═══════════════════════════════════════════════════════════════

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
        log("  [SKIP] sin credenciales MP"); return {}

    headers = {"Authorization": f"Bearer {MP_TOKEN}"}
    base    = "https://api.mercadopago.com"
    inicio  = f"{ANO}-01-01T00:00:00Z"
    fin     = f"{min(ahora_ar().date(), date(ANO,12,31)).strftime('%Y-%m-%d')}T23:59:59Z"

    try:
        r = requests.post(f"{base}/v1/account/settlement_report", headers=headers,
                          json={"begin_date": inicio, "end_date": fin})
        log(f"  MP create: {r.status_code}")
    except Exception as e:
        log(f"  [ERROR] MP create: {e}"); return {}

    filename = None
    for _ in range(20):
        time.sleep(3)
        try:
            files = requests.get(f"{base}/v1/account/settlement_report/list",
                                 headers=headers).json()
            if files:
                latest = max(files, key=lambda x: x.get("date_created", ""))
                if latest.get("status") in ("ready", "available"):
                    filename = latest.get("file_name"); break
        except:
            pass

    if not filename:
        log("  [WARN] MP settlement no disponible"); return {}

    try:
        content = requests.get(
            f"{base}/v1/account/settlement_report/{filename}", headers=headers
        ).text
    except Exception as e:
        log(f"  [ERROR] MP download: {e}"); return {}

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


# ═══════════════════════════════════════════════════════════════
# FUENTE 3: META ADS API
# Replica lógica de meta_ads_facturacion.py con JSON local en /tmp
# ═══════════════════════════════════════════════════════════════

def _meta_descargar_periodo(fecha_desde, fecha_hasta):
    url = f"https://graph.facebook.com/v19.0/act_{META_ACCOUNT}/insights"
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
    log("Meta Ads: descargando...")
    if not META_TOKEN or not META_ACCOUNT:
        log("  [SKIP] sin credenciales Meta"); return {}

    # Cargar caché local
    datos_dict = {}
    if os.path.exists(META_JSON_PATH):
        try:
            with open(META_JSON_PATH, "r", encoding="utf-8") as f:
                datos_dict = {d["date_start"]: d for d in json.load(f)}
            log(f"  Meta caché: {len(datos_dict)} días")
        except:
            pass

    # Siempre actualizar últimos 60 días; si no hay caché, bajar desde ene del año
    fecha_fin = ahora_ar().date()
    fecha_ini = date(ANO, 1, 1) if not datos_dict else max(
        date(ANO, 1, 1), fecha_fin - timedelta(days=60)
    )

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
            log(f"  Meta {fecha_actual} → {fecha_lote_fin}: {len(lote)} días")
        except Exception as e:
            log(f"  [WARN] Meta lote {fecha_actual}: {e}")
        fecha_actual = fecha_lote_fin + timedelta(days=1)
        time.sleep(0.5)

    # Guardar caché
    try:
        with open(META_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(list(datos_dict.values()), f)
    except:
        pass

    gastos = defaultdict(float)
    for reg in datos_dict.values():
        spend = safe_float(reg.get("spend", 0))
        if spend:
            acumular(gastos, reg.get("date_start", ""), -spend)

    log(f"  Meta: {len(gastos)} meses")
    return {"pub_meta": dict(gastos)}


# ═══════════════════════════════════════════════════════════════
# FUENTE 4: GOOGLE ADS (desde Sheet)
# ═══════════════════════════════════════════════════════════════

def fetch_google_ads():
    log("Google Ads: leyendo desde Sheet...")
    gastos = defaultdict(float)

    for hoja in ["Historico", "historico", "Google Ads", "Hoja 1"]:
        rows = leer_hoja(SHEET_ID_GOOGLE_ADS, hoja)
        if not rows or len(rows) < 2:
            continue
        h = rows[0]
        i_f = _col_idx(h, "fecha", "date", "dia")
        i_c = _col_idx(h, "costo", "cost", "importe", "gasto", "spend")
        if i_f < 0 or i_c < 0:
            continue
        for row in rows[1:]:
            fecha = row[i_f] if len(row) > i_f else None
            val   = safe_float(row[i_c]) if len(row) > i_c and row[i_c] else 0.0
            if fecha and val:
                acumular(gastos, fecha, -abs(val))
        log(f"  Google Ads '{hoja}': {len(gastos)} meses")
        break

    return {"pub_gads": dict(gastos)}


# ═══════════════════════════════════════════════════════════════
# FUENTE 5: PAGONUBE (Sheet "PagoNube")
# Columnas: Cliente | Medio de pago | Descripción | Número de venta |
#   Fecha de creación | Disponible para transferir | Monto de la venta |
#   Tasa Pago Nube | Cantidad de cuotas | Costo de Cuota Simple |
#   Costo de cuotas Pago Nube | IVA | Ganancias | IIBB | SIRTAC | Valor neto
# ═══════════════════════════════════════════════════════════════

def fetch_pagonube():
    log("PagoNube: leyendo desde Sheet...")
    comisiones = defaultdict(float)
    ret_iibb   = defaultdict(float)

    rows = leer_hoja(SHEET_ID_GASTOS, "PagoNube")
    if not rows or len(rows) < 2:
        log("  [WARN] hoja PagoNube vacía"); return {}

    h = rows[0]
    i_fecha   = _col_idx(h, "fecha de creación", "fecha de creacion", "fecha")
    i_tasa    = _col_idx(h, "tasa pago nube", "tasa")
    i_cuota_s = _col_idx(h, "costo de cuota simple")
    i_cuota_p = _col_idx(h, "costo de cuotas pago nube")
    i_iibb    = _col_idx(h, "iibb")
    i_desc    = _col_idx(h, "descripción", "descripcion")

    for row in rows[1:]:
        desc = str(row[i_desc]).lower() if i_desc >= 0 and len(row) > i_desc else "venta"
        if any(x in desc for x in ["devolucion", "devolución", "refund", "chargeback"]):
            continue

        fecha = row[i_fecha] if i_fecha >= 0 and len(row) > i_fecha else None
        if not fecha:
            continue

        tasa    = safe_float(row[i_tasa])    if i_tasa >= 0    and len(row) > i_tasa    else 0.0
        cuota_s = safe_float(row[i_cuota_s]) if i_cuota_s >= 0 and len(row) > i_cuota_s else 0.0
        cuota_p = safe_float(row[i_cuota_p]) if i_cuota_p >= 0 and len(row) > i_cuota_p else 0.0
        iibb    = safe_float(row[i_iibb])    if i_iibb >= 0    and len(row) > i_iibb    else 0.0

        # Tasa + cuotas ya son negativos en el CSV → comisión como egreso
        com = tasa + cuota_s + cuota_p
        if com:
            acumular(comisiones, fecha, com)
        if iibb:
            acumular(ret_iibb, fecha, iibb)

    log(f"  PagoNube: {len(comisiones)} meses")
    return {"com_pagonube": dict(comisiones), "ret_iibb_pn": dict(ret_iibb)}


# ═══════════════════════════════════════════════════════════════
# FUENTE 6: MP GETNET HISTÓRICO (Sheet "MP_Getnet_Historico")
# Columnas: Fecha | Importe | Comision_Getnet | Retencion_IIBB | Retencion_Ganancias
# Cubre ago-2023 a ene-2024 (de los sales*.xls de Getnet)
# ═══════════════════════════════════════════════════════════════

def fetch_mp_getnet_historico():
    log("MP Getnet histórico: leyendo desde Sheet...")
    comisiones = defaultdict(float)
    ret_iibb   = defaultdict(float)

    rows = leer_hoja(SHEET_ID_GASTOS, "MP_Getnet_Historico")
    if not rows or len(rows) < 2:
        log("  [WARN] hoja MP_Getnet_Historico vacía"); return {}

    h = rows[0]
    i_fecha = _col_idx(h, "fecha")
    i_com   = _col_idx(h, "comision_getnet", "comision", "getnet")
    i_iibb  = _col_idx(h, "retencion_iibb", "iibb")

    for row in rows[1:]:
        fecha = row[i_fecha] if i_fecha >= 0 and len(row) > i_fecha else None
        com   = safe_float(row[i_com])  if i_com >= 0  and len(row) > i_com  else 0.0
        iibb  = safe_float(row[i_iibb]) if i_iibb >= 0 and len(row) > i_iibb else 0.0
        if fecha and com:
            acumular(comisiones, fecha, -com)   # egreso → negativo
        if fecha and iibb:
            acumular(ret_iibb, fecha, -abs(iibb))

    log(f"  MP Getnet histórico: {len(comisiones)} meses")
    return {"com_pagonube_hist": dict(comisiones), "ret_iibb_hist": dict(ret_iibb)}


# ═══════════════════════════════════════════════════════════════
# FUENTE 7: DATOS MANUALES (Sheet Ingresos y Gastos)
# ═══════════════════════════════════════════════════════════════

def fetch_manuales():
    log("Datos manuales: leyendo desde Sheet...")
    result = {}

    def egreso(hoja, col_f=0, col_e=3):
        rows = leer_hoja(SHEET_ID_GASTOS, hoja)
        acum = defaultdict(float)
        for row in rows[1:]:
            f = row[col_f] if len(row) > col_f else None
            v = safe_float(row[col_e]) if len(row) > col_e and row[col_e] else 0.0
            if f and v:
                acumular(acum, f, -v)
        return dict(acum)

    def ingreso(hoja, col_f=0, col_i=2):
        rows = leer_hoja(SHEET_ID_GASTOS, hoja)
        acum = defaultdict(float)
        for row in rows[1:]:
            f = row[col_f] if len(row) > col_f else None
            v = safe_float(row[col_i]) if len(row) > col_i and row[col_i] else 0.0
            if f and v:
                acumular(acum, f, v)
        return dict(acum)

    result["ventas_manual"] = ingreso("Ventas")
    log(f"  Ventas manuales: {len(result['ventas_manual'])} meses")

    result["compras"] = egreso("Compra Materia prima - Producto")
    log(f"  Compras: {len(result['compras'])} meses")

    result["sueldos"] = egreso("Sueldos")
    log(f"  Sueldos: {len(result['sueldos'])} meses")

    result["pub_agencia"] = egreso("Publicidad")
    log(f"  Agencia pub: {len(result['pub_agencia'])} meses")

    # Correo histórico: col 0=Fecha, col 3=Importe (positivo=factura, negativo=NC)
    rows = leer_hoja(SHEET_ID_GASTOS, "Correo_argentino")
    correo_h = defaultdict(float)
    for row in rows[1:]:
        f = row[0] if len(row) > 0 else None
        v = safe_float(row[3]) if len(row) > 3 and row[3] else 0.0
        if f and v:
            acumular(correo_h, f, -v)   # egreso → negativo
    result["correo_hist"] = dict(correo_h)
    log(f"  Correo histórico: {len(result['correo_hist'])} meses")

    tn_abono = leer_hoja_2col(SHEET_ID_GASTOS, "Tiendanube_abono")
    result["com_tn"] = {k: -v for k, v in tn_abono.items()}
    log(f"  TN abono: {len(result['com_tn'])} meses")

    mono = leer_hoja_2col(SHEET_ID_GASTOS, "Monotributo")
    result["monotributo"] = {k: -v for k, v in mono.items()}
    log(f"  Monotributo: {len(result['monotributo'])} meses")

    return result


# ═══════════════════════════════════════════════════════════════
# COMBINAR TODAS LAS FUENTES
# ═══════════════════════════════════════════════════════════════

def combinar_rubros(tn, mp, meta, gads, pagonube, mp_hist, manuales):
    datos = defaultdict(lambda: defaultdict(float))

    def merge(rubro, d):
        for k, v in d.items():
            datos[rubro][k] += v

    # Tiendanube
    for r in ["ventas_min", "envio_min", "dto_min", "ventas_may", "envio_may", "dto_may"]:
        merge(r, tn.get(r, {}))

    # MercadoPago settlement
    merge("com_mp",         mp.get("com_mp", {}))
    merge("envio_andreani", mp.get("envio_andreani", {}))
    merge("ret_iibb",       mp.get("ret_iibb", {}))

    # Correo Argentino: histórico hasta jul-2024, MP settlement desde ago-2024
    for k, v in manuales.get("correo_hist", {}).items():
        datos["envio_correo"][k] += v
    for k, v in mp.get("envio_correo", {}).items():
        if k >= (2024, 8) or k not in datos["envio_correo"]:
            datos["envio_correo"][k] += v

    # PagoNube/Getnet:
    #   histórico sales*.xls ago-2023→ene-2024
    #   PagoNube CSV ene-2024 en adelante (se suman, no se solapan por construcción)
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


# ═══════════════════════════════════════════════════════════════
# CONSTRUIR Y ESCRIBIR P&L
# ═══════════════════════════════════════════════════════════════

def construir_pnl(datos):
    periodos = [(ANO, m) for m in range(1, 13)]
    tabla = {
        rid: {p: round(datos.get(rid, {}).get(p, 0.0), 2) for p in periodos}
        for rid, _, _ in PNL_FILAS
    }
    return periodos, tabla

def escribir_hoja1(periodos, tabla):
    log("Escribiendo Hoja 1...")

    filas = [["Row ID"] + [f"{y},{m}" for y, m in periodos]]
    cat_actual = None
    for rid, nombre, cat in PNL_FILAS:
        if cat != cat_actual:
            cat_actual = cat
            filas.append([cat] + [
                round(sum(tabla.get(r, {}).get(p, 0.0)
                          for r, _, c in PNL_FILAS if c == cat), 2)
                for p in periodos
            ])
        filas.append([nombre] + [tabla[rid].get(p, 0.0) for p in periodos])

    ingresos = {p: sum(tabla.get(r, {}).get(p, 0.0)
                       for r, _, c in PNL_FILAS if c == "Ingresos")
                for p in periodos}
    egresos  = {p: sum(tabla.get(r, {}).get(p, 0.0)
                       for r, _, c in PNL_FILAS if c in CATEGORIAS_EGRESO)
                for p in periodos}
    resultado = {p: round(ingresos[p] + egresos[p], 2) for p in periodos}

    filas.append([])
    filas.append(["Totales"] + [resultado[p] for p in periodos])

    escribir_hoja(SHEET_ID_RESUMEN, f"Hoja 1 Nueva!A1:Z{len(filas)+3}", filas)
    log(f"  Hoja 1: {len(filas)} filas × {len(filas[0])} cols")

    log("  ─── Resumen P&L ───")
    for p in periodos:
        if ingresos[p] or egresos[p]:
            log(f"  {ANO}/{p[1]:02d}  Ing={ingresos[p]:>14,.0f}  "
                f"Egr={egresos[p]:>14,.0f}  Res={resultado[p]:>14,.0f}")


# ═══════════════════════════════════════════════════════════════
# TRIGGER
# ═══════════════════════════════════════════════════════════════

def escribir_trigger(estado, detalle):
    try:
        escribir_hoja(SHEET_ID_RESUMEN, "Trigger!A1:A2",
                      [[estado], [detalle]])
    except Exception as e:
        log(f"  [WARN] trigger: {e}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    log("=" * 55)
    log(f"HECHIZO REPORTE NUEVO — año {ANO}")
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
