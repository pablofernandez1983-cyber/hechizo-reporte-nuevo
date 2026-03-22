"""
reporte_nuevo.py — Hechizo Bijou P&L mensual
Reemplaza el workflow KNIME. No toca ningún archivo existente.

Fuentes:
  - Tiendanube API          → Ventas, Envíos, Descuentos
  - MercadoPago API         → Comisiones MP, Correo Arg (post jul-2024),
                              Andreani, Retenciones IIBB
  - Meta Ads API            → Publicidad Meta
  - Google Sheets (ResumenHechizo → Google Ads Historico)
  - Google Sheets (Ingresos y Gastos):
      Ventas, Compra Materia prima - Producto, Sueldos, Publicidad,
      Correo_argentino, Tiendanube_abono, Monotributo

Variables de entorno:
  TIENDANUBE_STORE_ID, TIENDANUBE_ACCESS_TOKEN
  MP_ACCESS_TOKEN, MP_USER_ID
  META_ADS_TOKEN, META_ADS_ACCOUNT_ID      (opcional)
  GOOGLE_SERVICE_ACCOUNT_JSON
  SHEET_ID_RESUMEN       default: 1nUWfj9u0y7M7n2fNG6v55WnIxAedPpBxQZFUXk28nlI
  SHEET_ID_GASTOS        default: 1Eswje16JVngNEPTpq8f2-_2tjO8XAbsBBJnCIaqNkRY
  SHEET_ID_GOOGLE_ADS    default: 1dNJReQ2zxMxRcs0tFTdhdPPAGaXzxqOJIRWDcY9KMPI
  ANO_REPORTE            default: año actual en AR
"""

import os, json, time, requests, traceback
from datetime import datetime, date, timedelta, timezone
from collections import defaultdict
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

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
META_TOKEN   = os.environ.get("META_ADS_TOKEN", "")
META_ACCOUNT = os.environ.get("META_ADS_ACCOUNT_ID", "")
SA_JSON      = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# ─── Estructura del P&L (orden y categorías) ──────────────
# (rubro_id, nombre_display, categoría)
PNL_FILAS = [
    # Ingresos
    ("ventas_min",     "Ventas",                    "Ingresos"),
    ("envio_min",      "Cobro Envio Minorista",      "Ingresos"),
    ("dto_min",        "Descuento Minorista",        "Ingresos"),
    ("ventas_may",     "Ventas Mayoristas",          "Ingresos"),
    ("envio_may",      "Cobro Envio Mayorista",      "Ingresos"),
    ("dto_may",        "Descuento Mayorista",        "Ingresos"),
    ("ventas_manual",  "Ventas Manuales",            "Ingresos"),
    # Costo de Mercadería
    ("compras",        "Compras",                   "Costo de Mercaderia"),
    # Gastos por Ventas
    ("com_mp",         "Comisiones Mercadolibre",   "Gastos por Ventas"),
    ("com_pagonube",   "Comision Getnet/Pagonube",  "Gastos por Ventas"),
    ("com_tn",         "Comision Tiendanube",       "Gastos por Ventas"),
    # Gastos de Comercialización
    ("envio_correo",   "Envio Correo Argentino",    "Gastos de Comercializacion"),
    ("envio_andreani", "Envio Andreani",             "Gastos de Comercializacion"),
    ("envio_moto",     "Envio Moto",                "Gastos de Comercializacion"),
    # Publicidad
    ("pub_meta",       "Publicidad Meta",           "Publicidad"),
    ("pub_gads",       "Publicidad Google Ads",     "Publicidad"),
    ("pub_agencia",    "Agencia Publicidad",        "Publicidad"),
    # Impuestos
    ("ret_iibb",       "Retenciones IIBB",          "Impuestos"),
    ("monotributo",    "Monotributo",               "Impuestos"),
    # Gastos de Administración
    ("sueldos",        "Sueldos",                   "Gastos de Administracion"),
]

CATEGORIAS_EGRESO = [
    "Costo de Mercaderia",
    "Gastos por Ventas",
    "Gastos de Comercializacion",
    "Publicidad",
    "Impuestos",
    "Gastos de Administracion",
]


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def log(msg):
    print(f"[{ahora_ar().strftime('%H:%M:%S')}] {msg}", flush=True)

def mes_key(fecha):
    """datetime/date/str → (año, mes) int tuple"""
    if isinstance(fecha, (datetime, date)):
        return (fecha.year, fecha.month)
    s = str(fecha)[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return (dt.year, dt.month)
        except ValueError:
            pass
    return None

def acumular(dic, fecha, valor):
    k = mes_key(fecha)
    if k:
        dic[k] += valor

def safe_float(v):
    try:
        return float(str(v).replace(",", ".").replace(" ", "").replace("$", ""))
    except:
        return 0.0


# ═══════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════

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

def leer_gastos_sheet(hoja, col_fecha=0, col_egreso=3, col_ingreso=2):
    """
    Lee una solapa de Ingresos y Gastos.
    Retorna dos dicts: {(año,mes): total_egreso}, {(año,mes): total_ingreso}
    """
    rows = leer_hoja(SHEET_ID_GASTOS, hoja)
    egresos  = defaultdict(float)
    ingresos = defaultdict(float)
    if not rows:
        return egresos, ingresos
    for row in rows[1:]:   # skip header
        fecha = row[col_fecha] if len(row) > col_fecha else None
        egr   = safe_float(row[col_egreso])  if len(row) > col_egreso  and row[col_egreso]  else 0.0
        ing   = safe_float(row[col_ingreso]) if len(row) > col_ingreso and row[col_ingreso] else 0.0
        if fecha and (egr or ing):
            acumular(egresos,  fecha, egr)
            acumular(ingresos, fecha, ing)
    return egresos, ingresos

def leer_hoja_2col(sheet_id, hoja, col_fecha=0, col_valor=1):
    """Lee solapa con solo Fecha e Importe (Correo_argentino, Tiendanube_abono, Monotributo)."""
    rows = leer_hoja(sheet_id, hoja)
    acum = defaultdict(float)
    if not rows:
        return acum
    for row in rows[1:]:
        fecha = row[col_fecha] if len(row) > col_fecha else None
        val   = safe_float(row[col_valor]) if len(row) > col_valor and row[col_valor] else 0.0
        if fecha and val:
            acumular(acum, fecha, val)
    return acum


# ═══════════════════════════════════════════════════════════
# FUENTE 1: TIENDANUBE
# ═══════════════════════════════════════════════════════════

def fetch_tiendanube():
    log("Tiendanube: descargando órdenes...")
    if not TN_STORE_ID or not TN_TOKEN:
        log("  [SKIP] sin credenciales")
        return {}

    headers = {
        "Authentication": f"bearer {TN_TOKEN}",
        "User-Agent": "HechizoBijou-Reporte/1.0 (hechizobijou@gmail.com)"
    }
    base = f"https://api.tiendanube.com/v1/{TN_STORE_ID}"

    orders = []
    page = 1
    while True:
        try:
            r = requests.get(f"{base}/orders", headers=headers,
                             params={"page": page, "per_page": 200,
                                     "payment_status": "paid,authorized"},
                             timeout=30)
            r.raise_for_status()
            batch = r.json()
        except Exception as e:
            log(f"  [ERROR] TN página {page}: {e}")
            break
        if not batch:
            break
        orders.extend(batch)
        log(f"  TN pág {page}: {len(batch)} órdenes (total {len(orders)})")
        if len(batch) < 200:
            break
        page += 1
        time.sleep(0.3)

    log(f"  Total TN: {len(orders)} órdenes")

    ventas_min  = defaultdict(float)
    envio_min   = defaultdict(float)
    dto_min     = defaultdict(float)
    ventas_may  = defaultdict(float)
    envio_may   = defaultdict(float)
    dto_may     = defaultdict(float)

    for o in orders:
        if o.get("status") == "cancelled":
            continue

        raw_date = o.get("created_at", "")
        try:
            dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00")).astimezone(TZ_AR)
        except:
            continue
        k = (dt.year, dt.month)

        subtotal = safe_float(o.get("subtotal", 0))
        shipping = safe_float(o.get("shipping_cost_owner", 0))
        discount = safe_float(o.get("discount", 0))

        # Detectar mayorista: método de pago = transferencia bancaria
        gateways = [p.get("payment_method_id", "") or ""
                    for p in (o.get("payment_details") or [])]
        is_may = any("transfer" in g.lower() or "deposito" in g.lower()
                     for g in gateways)

        if is_may:
            ventas_may[k] += subtotal
            envio_may[k]  += shipping
            dto_may[k]    -= discount
        else:
            ventas_min[k] += subtotal
            envio_min[k]  += shipping
            dto_min[k]    -= discount

    return {
        "ventas_min":  dict(ventas_min),
        "envio_min":   dict(envio_min),
        "dto_min":     dict(dto_min),
        "ventas_may":  dict(ventas_may),
        "envio_may":   dict(envio_may),
        "dto_may":     dict(dto_may),
    }


# ═══════════════════════════════════════════════════════════
# FUENTE 2: MERCADOPAGO settlement
# ═══════════════════════════════════════════════════════════

# Palabras clave en DESCRIPTION para clasificar movimientos del settlement
_KW_COM_MP      = ["comision", "commission", "fee", "cargo_financiero"]
_KW_CORREO      = ["correo argentino", "correo_argentino", "andreani_correo",
                   "envio_correo", "correo arg"]
_KW_ANDREANI    = ["andreani", "envio_andreani"]
_KW_IIBB        = ["retencion", "retención", "iibb", "ingresos brutos",
                   "percepcion", "withholding"]

def _clasificar_mp(descripcion):
    d = (descripcion or "").lower()
    if any(k in d for k in _KW_IIBB):
        return "ret_iibb"
    if any(k in d for k in _KW_CORREO):
        return "envio_correo"
    if any(k in d for k in _KW_ANDREANI):
        return "envio_andreani"
    if any(k in d for k in _KW_COM_MP):
        return "com_mp"
    return None

def fetch_mercadopago():
    log("MercadoPago: descargando settlement...")
    if not MP_TOKEN or not MP_USER_ID:
        log("  [SKIP] sin credenciales MP")
        return {}

    # Generar reporte de settlement
    inicio = f"{ANO}-01-01T00:00:00Z"
    fin_dt = min(ahora_ar().date(), date(ANO, 12, 31))
    fin    = f"{fin_dt.strftime('%Y-%m-%d')}T23:59:59Z"

    headers = {"Authorization": f"Bearer {MP_TOKEN}"}
    base    = "https://api.mercadopago.com"

    # Disparar generación
    try:
        r = requests.post(
            f"{base}/v1/account/settlement_report",
            headers=headers,
            json={"begin_date": inicio, "end_date": fin}
        )
        log(f"  MP settlement create: {r.status_code}")
    except Exception as e:
        log(f"  [ERROR] MP create: {e}")
        return {}

    # Esperar y obtener el archivo más reciente
    filename = None
    for _ in range(20):
        time.sleep(3)
        try:
            r = requests.get(f"{base}/v1/account/settlement_report/list",
                             headers=headers)
            files = r.json()
            if files:
                latest = max(files, key=lambda x: x.get("date_created", ""))
                if latest.get("status") in ("ready", "available"):
                    filename = latest.get("file_name")
                    break
        except:
            pass

    if not filename:
        log("  [WARN] MP settlement no disponible en tiempo")
        return {}

    # Descargar CSV
    try:
        r = requests.get(f"{base}/v1/account/settlement_report/{filename}",
                         headers=headers)
        content = r.text
    except Exception as e:
        log(f"  [ERROR] MP download: {e}")
        return {}

    # Parsear
    acum = {
        "com_mp":         defaultdict(float),
        "envio_correo":   defaultdict(float),
        "envio_andreani": defaultdict(float),
        "ret_iibb":       defaultdict(float),
    }

    lines = content.splitlines()
    if not lines:
        return {}

    # Detectar header
    headers_csv = [h.strip().upper() for h in lines[0].split(",")]
    idx = {h: i for i, h in enumerate(headers_csv)}

    i_date = idx.get("DATE") or idx.get("FECHA") or 0
    i_type = idx.get("TRANSACTION_TYPE") or idx.get("TIPO") or 2
    i_desc = idx.get("DESCRIPTION") or idx.get("DESCRIPCION") or 3
    i_net  = idx.get("NET_CREDIT_AMOUNT") or idx.get("IMPORTE_NETO") or 7

    for line in lines[1:]:
        cols = line.split(",")
        if len(cols) < max(i_date, i_net) + 1:
            continue
        fecha = cols[i_date].strip().strip('"')
        desc  = cols[i_desc].strip().strip('"') if len(cols) > i_desc else ""
        try:
            monto = safe_float(cols[i_net])
        except:
            continue

        cat = _clasificar_mp(desc)
        if cat and monto != 0:
            acumular(acum[cat], fecha, monto)

    log(f"  MP settlement parseado OK")
    return {k: dict(v) for k, v in acum.items()}


# ═══════════════════════════════════════════════════════════
# FUENTE 3: META ADS
# ═══════════════════════════════════════════════════════════

def fetch_meta():
    log("Meta Ads: descargando...")
    if not META_TOKEN or not META_ACCOUNT:
        log("  [SKIP] sin credenciales Meta")
        return {}

    gastos = defaultdict(float)
    try:
        url = f"https://graph.facebook.com/v19.0/act_{META_ACCOUNT}/insights"
        params = {
            "access_token": META_TOKEN,
            "level": "account",
            "fields": "spend,date_start",
            "time_increment": 1,
            "time_range": json.dumps({
                "since": f"{ANO}-01-01",
                "until": min(ahora_ar().date(), date(ANO, 12, 31)).strftime("%Y-%m-%d")
            }),
            "limit": 500
        }
        while True:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            for row in data.get("data", []):
                spend = safe_float(row.get("spend", 0))
                if spend:
                    acumular(gastos, row.get("date_start", ""), -spend)  # egreso → negativo
            cursor = data.get("paging", {}).get("cursors", {}).get("after")
            if not cursor:
                break
            params["after"] = cursor
    except Exception as e:
        log(f"  [ERROR] Meta: {e}")

    log(f"  Meta: {len(gastos)} meses")
    return {"pub_meta": dict(gastos)}


# ═══════════════════════════════════════════════════════════
# FUENTE 4: GOOGLE ADS (desde Sheet)
# ═══════════════════════════════════════════════════════════

def fetch_google_ads():
    log("Google Ads: leyendo desde Sheet...")
    gastos = defaultdict(float)

    for hoja in ["Historico", "historico", "Hoja 1", "Google Ads"]:
        rows = leer_hoja(SHEET_ID_GOOGLE_ADS, hoja)
        if not rows or len(rows) < 2:
            continue

        headers = [str(h).strip().upper() for h in rows[0]]
        i_fecha = next((i for i, h in enumerate(headers)
                        if "FECHA" in h or "DATE" in h or "DIA" in h), -1)
        i_costo = next((i for i, h in enumerate(headers)
                        if "COSTO" in h or "COST" in h or "IMPORTE" in h or "GASTO" in h), -1)

        if i_fecha < 0 or i_costo < 0:
            log(f"  Google Ads hoja '{hoja}' sin columnas esperadas")
            continue

        for row in rows[1:]:
            fecha = row[i_fecha] if len(row) > i_fecha else None
            val   = safe_float(row[i_costo]) if len(row) > i_costo and row[i_costo] else 0.0
            if fecha and val:
                acumular(gastos, fecha, -abs(val))  # siempre egreso
        log(f"  Google Ads '{hoja}': {len(gastos)} meses")
        break

    return {"pub_gads": dict(gastos)}


# ═══════════════════════════════════════════════════════════
# FUENTE 5: DATOS MANUALES (Ingresos y Gastos Sheet)
# ═══════════════════════════════════════════════════════════

def fetch_manuales():
    log("Datos manuales: leyendo desde Sheet...")
    result = {}

    # Ventas manuales (col Ingreso)
    ing, _ = leer_gastos_sheet("Ventas", col_fecha=0, col_ingreso=2, col_egreso=3)
    # Acumular ingresos (col 2 = Ingreso)
    rows = leer_hoja(SHEET_ID_GASTOS, "Ventas")
    vm = defaultdict(float)
    for row in rows[1:]:
        fecha = row[0] if row else None
        val   = safe_float(row[2]) if len(row) > 2 and row[2] else 0.0
        if fecha and val:
            acumular(vm, fecha, val)
    result["ventas_manual"] = dict(vm)
    log(f"  Ventas manuales: {len(result['ventas_manual'])} meses")

    # Compras (col Egreso → negativo)
    rows = leer_hoja(SHEET_ID_GASTOS, "Compra Materia prima - Producto")
    comp = defaultdict(float)
    for row in rows[1:]:
        fecha = row[0] if row else None
        val   = safe_float(row[3]) if len(row) > 3 and row[3] else 0.0
        if fecha and val:
            acumular(comp, fecha, -val)
    result["compras"] = dict(comp)
    log(f"  Compras: {len(result['compras'])} meses")

    # Sueldos (col Egreso → negativo)
    rows = leer_hoja(SHEET_ID_GASTOS, "Sueldos")
    sued = defaultdict(float)
    for row in rows[1:]:
        fecha = row[0] if row else None
        val   = safe_float(row[3]) if len(row) > 3 and row[3] else 0.0
        if fecha and val:
            acumular(sued, fecha, -val)
    result["sueldos"] = dict(sued)
    log(f"  Sueldos: {len(result['sueldos'])} meses")

    # Agencia Publicidad (col Egreso → negativo)
    rows = leer_hoja(SHEET_ID_GASTOS, "Publicidad")
    pub = defaultdict(float)
    for row in rows[1:]:
        fecha = row[0] if row else None
        val   = safe_float(row[3]) if len(row) > 3 and row[3] else 0.0
        if fecha and val:
            acumular(pub, fecha, -val)
    result["pub_agencia"] = dict(pub)
    log(f"  Agencia pub: {len(result['pub_agencia'])} meses")

    # Correo Argentino histórico (Importe ya es costo neto, positivo = egreso)
    correo = leer_hoja_2col(SHEET_ID_GASTOS, "Correo_argentino")
    # Notas de crédito ya están en negativo en el CSV → se restan del costo
    # Invertir signo porque en el P&L queremos negativo para egresos
    result["correo_hist"] = {k: -v for k, v in correo.items()}
    log(f"  Correo histórico: {len(result['correo_hist'])} meses")

    # Tiendanube abono (Importe = costo mensual → negativo en P&L)
    tn_abono = leer_hoja_2col(SHEET_ID_GASTOS, "Tiendanube_abono")
    result["com_tn"] = {k: -v for k, v in tn_abono.items()}
    log(f"  TN abono: {len(result['com_tn'])} meses")

    # Monotributo (→ negativo en P&L)
    mono = leer_hoja_2col(SHEET_ID_GASTOS, "Monotributo")
    result["monotributo"] = {k: -v for k, v in mono.items()}
    log(f"  Monotributo: {len(result['monotributo'])} meses")

    return result


# ═══════════════════════════════════════════════════════════
# MERGE: combinar todas las fuentes
# ═══════════════════════════════════════════════════════════

def merge_datos(*fuentes):
    """Combina múltiples dicts {rubro: {mes_key: valor}}"""
    merged = defaultdict(lambda: defaultdict(float))
    for fuente in fuentes:
        for rubro, meses in fuente.items():
            for k, v in meses.items():
                merged[rubro][k] += v
    return merged


# ═══════════════════════════════════════════════════════════
# CORREO ARGENTINO: combinar histórico + MP settlement
# ═══════════════════════════════════════════════════════════

def combinar_correo(datos_mp, manuales):
    """
    Correo histórico (Sheet) cubre hasta jul-2024.
    Desde ago-2024 en adelante, los costos de Correo salen del settlement de MP.
    Combina ambas fuentes sin solapamiento.
    """
    correo = defaultdict(float)

    # Histórico del Sheet (todo lo que haya, pre jul-2024 principalmente)
    for k, v in manuales.get("correo_hist", {}).items():
        correo[k] += v

    # Del settlement de MP (post jul-2024, o lo que venga)
    for k, v in datos_mp.get("envio_correo", {}).items():
        # Solo agregar si no hay dato histórico para ese mes,
        # o si ya pasó ago-2024
        if k >= (2024, 8) or k not in correo:
            correo[k] += v

    return dict(correo)


# ═══════════════════════════════════════════════════════════
# CONSTRUIR P&L
# ═══════════════════════════════════════════════════════════

def construir_pnl(datos):
    """
    datos: {rubro_id: {(año,mes): valor}}
    Retorna (periodos, tabla)
    tabla: {rubro_id: {(año,mes): valor}}
    """
    # Periodos: todos los meses del año de reporte
    periodos = [(ANO, m) for m in range(1, 13)]

    tabla = {}
    for rubro_id, _, _ in PNL_FILAS:
        tabla[rubro_id] = {
            p: round(datos.get(rubro_id, {}).get(p, 0.0), 2)
            for p in periodos
        }

    return periodos, tabla


# ═══════════════════════════════════════════════════════════
# ESCRIBIR EN HOJA 1
# ═══════════════════════════════════════════════════════════

def escribir_hoja1(periodos, tabla):
    """
    Escribe el P&L en Hoja 1 del ResumenHechizo.
    Formato esperado por la PWA:
      Fila 1: Row ID | año,mes | año,mes | ...
      Filas siguientes: nombre_rubro | valor | valor | ...
    """
    log("Escribiendo Hoja 1...")

    # Header: primera col = "Row ID", resto = "año,mes"
    col_headers = ["Row ID"] + [f"{y},{m}" for y, m in periodos]
    filas = [col_headers]

    cat_actual = None
    for rubro_id, nombre, cat in PNL_FILAS:
        # Separador de categoría (fila con el nombre de la categoría)
        if cat != cat_actual:
            cat_actual = cat
            fila_cat = [cat] + [
                round(sum(
                    tabla.get(r, {}).get(p, 0.0)
                    for r, _, c in PNL_FILAS if c == cat
                ), 2)
                for p in periodos
            ]
            filas.append(fila_cat)

        fila = [nombre] + [tabla[rubro_id].get(p, 0.0) for p in periodos]
        filas.append(fila)

    # Totales
    ingresos_tot = {
        p: sum(tabla.get(r, {}).get(p, 0.0)
               for r, _, c in PNL_FILAS if c == "Ingresos")
        for p in periodos
    }
    egresos_tot = {
        p: sum(tabla.get(r, {}).get(p, 0.0)
               for r, _, c in PNL_FILAS if c in CATEGORIAS_EGRESO)
        for p in periodos
    }
    resultado = {p: round(ingresos_tot[p] + egresos_tot[p], 2) for p in periodos}

    filas.append([])
    filas.append(["Totales"] + [resultado[p] for p in periodos])

    rng = f"Hoja 1!A1:Z{len(filas) + 3}"
    escribir_hoja(SHEET_ID_RESUMEN, rng, filas)
    log(f"  Hoja 1: {len(filas)} filas x {len(col_headers)} cols")

    # Mostrar resumen por consola
    log("  ─── Resumen P&L ───")
    for p in periodos:
        ing = ingresos_tot[p]
        egr = egresos_tot[p]
        res = resultado[p]
        if ing or egr:
            log(f"  {ANO}/{p[1]:02d}  Ingresos={ing:,.0f}  Egresos={egr:,.0f}  Resultado={res:,.0f}")


# ═══════════════════════════════════════════════════════════
# ACTUALIZAR TRIGGER
# ═══════════════════════════════════════════════════════════

def escribir_trigger(estado, detalle):
    try:
        escribir_hoja(SHEET_ID_RESUMEN, "Trigger!A1:A2",
                      [[estado], [detalle]])
    except Exception as e:
        log(f"  [WARN] trigger: {e}")


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    log("=" * 55)
    log(f"HECHIZO REPORTE NUEVO — año {ANO}")
    log("=" * 55)

    if not SA_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON no configurada")

    try:
        # 1. Tiendanube
        tn = fetch_tiendanube()

        # 2. MercadoPago
        mp = fetch_mercadopago()

        # 3. Meta Ads
        meta = fetch_meta()

        # 4. Google Ads
        gads = fetch_google_ads()

        # 5. Manuales
        manuales = fetch_manuales()

        # 6. Combinar Correo Argentino (histórico + MP)
        correo_combinado = combinar_correo(mp, manuales)

        # 7. Ensamblar todos los rubros
        datos_todos = merge_datos(
            tn,
            {
                "com_mp":         mp.get("com_mp", {}),
                "envio_correo":   correo_combinado,
                "envio_andreani": mp.get("envio_andreani", {}),
                "ret_iibb":       mp.get("ret_iibb", {}),
            },
            meta,
            gads,
            {k: manuales[k] for k in [
                "ventas_manual", "compras", "sueldos",
                "pub_agencia", "com_tn", "monotributo"
            ] if k in manuales},
        )

        # 8. Construir y escribir P&L
        periodos, tabla = construir_pnl(datos_todos)
        escribir_hoja1(periodos, tabla)

        # 9. Marcar trigger como listo
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
