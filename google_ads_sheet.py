"""
Carga automatica del gasto mensual de Google Ads en el Sheet de pagos
(SHEET_ID_GOOGLE_ADS, pestana "Historico") que lee reporte_nuevo.fetch_google_ads().

Reemplaza la carga manual mensual: si el mes objetivo todavia no tiene ninguna
fila, calcula el costo del mes via Google Ads API (cost_micros) y lo multiplica
x1.21 (el IVA que la API no incluye pero el pago real si).
Ya se verifico que Sheet_real ~= API x 1.21 con 98-100% de precision en 2025-2026.

Si el mes ya tiene alguna fila (cargada a mano o por una corrida anterior) no
hace nada -> es seguro correrlo mas de una vez. La fila queda marcada como
"Estimado automatico" para poder reemplazarla por el importe real de la tarjeta.

Credenciales: por variables de entorno en CI (GOOGLE_ADS_YAML con el contenido
del google-ads.yaml, GOOGLE_SERVICE_ACCOUNT_JSON con el JSON de la service
account). Si no estan, cae a los archivos locales de la maquina de Lore.

Uso: python google_ads_sheet.py [YYYY-MM]   (default: mes calendario anterior)
"""
import json
import os
import re
import sys
from datetime import date, timedelta

import yaml
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.oauth2 import service_account
from googleapiclient.discovery import build

CUSTOMER_ID = "5371891362"  # 537-189-1362
SHEET_ID_GOOGLE_ADS = (
    os.environ.get("SHEET_ID_GOOGLE_ADS")
    or "1dNJReQ2zxMxRcs0tFTdhdPPAGaXzxqOJIRWDcY9KMPI"
)
SHEET_TAB = "Historico"
IVA_MULTIPLIER = 1.21

LOCAL_ADS_YAML = r"C:\claude\Hechizo\google-ads-api\google-ads.yaml"
LOCAL_SERVICE_ACCOUNT = r"C:\claude\Hechizo\service_account.json"


def ads_client():
    raw = os.environ.get("GOOGLE_ADS_YAML")
    if raw:
        cfg = yaml.safe_load(raw)
        cfg.setdefault("use_proto_plus", True)
        return GoogleAdsClient.load_from_dict(cfg, version="v25")
    return GoogleAdsClient.load_from_storage(LOCAL_ADS_YAML, version="v25")


def sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        creds = service_account.Credentials.from_service_account_info(
            json.loads(raw), scopes=scopes
        )
    else:
        creds = service_account.Credentials.from_service_account_file(
            LOCAL_SERVICE_ACCOUNT, scopes=scopes
        )
    return build("sheets", "v4", credentials=creds)


def mes_objetivo():
    if len(sys.argv) > 1:
        y, m = sys.argv[1].split("-")
        return int(y), int(m)
    today = date.today()
    y, m = today.year, today.month - 1
    if m == 0:
        y, m = y - 1, 12
    return y, m


def primer_y_ultimo_dia(year, month):
    first = date(year, month, 1)
    if month == 12:
        last = date(year, 12, 31)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    return first, last


def mes_ya_cargado(sheet_values, year, month):
    target = f"{year:04d}-{month:02d}"
    for row in sheet_values[1:]:
        if not row:
            continue
        fecha = str(row[0]).strip()
        if fecha.startswith(target):
            return True
        m = re.match(r"^\d{4}-\d{2}", fecha)
        if m and m.group(0) == target:
            return True
    return False


def costo_mes_ads(year, month):
    first, last = primer_y_ultimo_dia(year, month)
    ga_service = ads_client().get_service("GoogleAdsService")
    query = f"""
        SELECT metrics.cost_micros
        FROM customer
        WHERE segments.date BETWEEN '{first.isoformat()}' AND '{last.isoformat()}'
    """
    total_micros = 0
    for batch in ga_service.search_stream(customer_id=CUSTOMER_ID, query=query):
        for row in batch.results:
            total_micros += row.metrics.cost_micros
    return total_micros / 1_000_000


def main():
    year, month = mes_objetivo()
    print(f"Mes objetivo: {year}-{month:02d}")

    svc = sheets_service()
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID_GOOGLE_ADS, range=f"'{SHEET_TAB}'!A:G"
    ).execute()
    values = res.get("values", [])

    if mes_ya_cargado(values, year, month):
        print(f"{year}-{month:02d} ya tiene datos en el Sheet. No se hace nada.")
        return

    try:
        costo_api = costo_mes_ads(year, month)
    except GoogleAdsException as ex:
        print(f"Error de la API de Google Ads (request id: {ex.request_id}):")
        for error in ex.failure.errors:
            print(f"  - {error.message}")
        sys.exit(1)

    if costo_api <= 0:
        print(f"Costo de la API para {year}-{month:02d} es 0. No se carga nada.")
        return

    importe_estimado = costo_api * IVA_MULTIPLIER
    fila = [
        f"{year:04d}-{month:02d}-01",
        "Pagos",
        f"Estimado automatico (Google Ads API cost x{IVA_MULTIPLIER} IVA)",
        "--",
        "--",
        f"-ARS{importe_estimado:,.2f}",
        "--",
    ]

    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID_GOOGLE_ADS,
        range=f"'{SHEET_TAB}'!A:G",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [fila]},
    ).execute()

    print(
        f"Fila agregada para {year}-{month:02d}: "
        f"costo API=${costo_api:,.2f} -> estimado con IVA=${importe_estimado:,.2f}"
    )


if __name__ == "__main__":
    main()
