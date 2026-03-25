"""
pagonube_export.py — versión GitHub Actions / headless

Descarga el CSV de PagoNube desde el admin de Tiendanube usando Playwright
en modo headless y sube el resultado a S3.

Cómo funciona:
  1. Lee la sesión de Tiendanube desde la variable de entorno TN_STATE_JSON
     (contenido del tiendanube_state.json como string JSON)
  2. Navega al admin, abre PagoNube, descarga el CSV
  3. Convierte CSV → JSON y sube a S3 (pagonube.json)
  4. Railway lo lee automáticamente en el próximo reporte

Variables de entorno requeridas:
  TN_STATE_JSON          → contenido de tiendanube_state.json (JSON string)
  AWS_S3_BUCKET_NAME
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_ENDPOINT_URL
  AWS_DEFAULT_REGION

Cuándo actualizar TN_STATE_JSON:
  Si el script falla con "Sesión vencida" → correr bootstrap en la PC de Lore
  una vez para regenerar tiendanube_state.json → actualizar el GitHub Secret.
"""

import json
import os
import csv
import random
import time
import tempfile
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
BASE_ADMIN        = "https://hechizobijou.mitiendanube.com/admin/v2"
MIN_STEP_SLEEP    = 0.30
MAX_STEP_SLEEP    = 0.70
SECTION_SLEEP_MIN = 0.80
SECTION_SLEEP_MAX = 1.50
MAX_TOTAL_SECONDS = 5 * 60

# S3
S3_BUCKET   = os.environ.get("AWS_S3_BUCKET_NAME", "")
S3_KEY_ID   = os.environ.get("AWS_ACCESS_KEY_ID", "")
S3_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "")
S3_REGION   = os.environ.get("AWS_DEFAULT_REGION", "auto")

# Sesión de Tiendanube (JSON string desde secret)
TN_STATE_JSON = os.environ.get("TN_STATE_JSON", "")


# =========================
# HELPERS
# =========================
def pace(section=False):
    time.sleep(
        random.uniform(SECTION_SLEEP_MIN, SECTION_SLEEP_MAX)
        if section
        else random.uniform(MIN_STEP_SLEEP, MAX_STEP_SLEEP)
    )

def safe_goto(page, url):
    page.goto(url, wait_until="domcontentloaded")
    pace(section=True)


# =========================
# S3
# =========================
def subir_a_s3(data_bytes: bytes, key: str):
    if not S3_BUCKET:
        print("[S3] Sin credenciales — no se subió")
        return False
    try:
        import boto3
        from botocore.config import Config
        s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_KEY_ID,
            aws_secret_access_key=S3_SECRET,
            region_name=S3_REGION,
            config=Config(signature_version="s3v4")
        )
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data_bytes,
                      ContentType="application/json")
        print(f"[S3] {key} subido ({len(data_bytes):,} bytes)")
        return True
    except Exception as e:
        print(f"[S3][ERROR] {e}")
        return False


def csv_a_json(csv_text: str) -> list:
    """Convierte el texto CSV de PagoNube a lista de dicts."""
    sep = ";" if csv_text.count(";") > csv_text.count(",") else ","
    reader = csv.DictReader(csv_text.splitlines(), delimiter=sep)
    rows = [dict(row) for row in reader]
    print(f"[OK] CSV parseado: {len(rows)} registros")
    return rows


# =========================
# SESSION
# =========================
def cargar_session(context):
    """Carga cookies y localStorage desde TN_STATE_JSON."""
    if not TN_STATE_JSON:
        raise RuntimeError(
            "TN_STATE_JSON no configurada. "
            "Corré bootstrap en la PC de Lore y copiá tiendanube_state.json "
            "como secret TN_STATE_JSON en GitHub."
        )
    state = json.loads(TN_STATE_JSON)

    if state.get("cookies"):
        context.add_cookies(state["cookies"])
        print(f"[OK] {len(state['cookies'])} cookies cargadas")

    if state.get("origins"):
        p = context.new_page()
        for o in state["origins"]:
            origin = o.get("origin")
            if not origin:
                continue
            try:
                p.goto(origin, wait_until="domcontentloaded", timeout=15000)
                for it in o.get("localStorage", []):
                    p.evaluate("([k,v]) => localStorage.setItem(k,v)",
                               [it["name"], it["value"]])
                for it in o.get("sessionStorage", []):
                    p.evaluate("([k,v]) => sessionStorage.setItem(k,v)",
                               [it["name"], it["value"]])
            except Exception as e:
                print(f"[WARN] localStorage {origin}: {e}")
        p.close()


# =========================
# EXPORT PAGO NUBE
# Con GitHub Actions el archivo se descarga en un directorio temp
# y se intercepta via Playwright download event (headless-friendly)
# =========================
def export_pagonube(page) -> str | None:
    """Navega a PagoNube y dispara el export. Retorna el texto del CSV."""

    safe_goto(page, f"{BASE_ADMIN}/dashboard/")

    # Verificar que la sesión sigue activa
    if "login" in page.url.lower():
        raise RuntimeError(
            "Sesión vencida. Actualizá el secret TN_STATE_JSON en GitHub "
            "corriendo el bootstrap una vez en la PC de Lore."
        )
    print(f"[OK] Sesión activa: {page.url}")

    # Ir a PagoNube via link del dashboard
    # (evita redirect de re-auth que ocurre navegando directo a /payments/pago-nube)
    print("[NAV] Buscando link de Pago Nube en el dashboard...")
    try:
        link = page.get_by_role("link", name="Pago Nube")
        link.wait_for(state="visible", timeout=15000)
        link.click()
        page.wait_for_load_state("domcontentloaded", timeout=30000)
        pace(section=True)
        print(f"[OK] Navegado a: {page.url}")
    except PWTimeout:
        print("[WARN] Link no encontrado, navegando directo...")
        page.goto(f"{BASE_ADMIN}/payments/pago-nube",
                  wait_until="domcontentloaded", timeout=30000)
        if "code=" in page.url or "sessionId=" in page.url:
            print("[WAIT] Redirect de re-auth detectado, esperando...")
            page.wait_for_url("**/payments/pago-nube**", timeout=30000)
        pace(section=True)

    # Esperar el iframe de PagoNube
    frame = page.frame_locator('[data-testid="iframe-app"]')
    try:
        frame.get_by_role("button", name="Exportar").wait_for(
            timeout=30000, state="visible"
        )
    except PWTimeout:
        print(f"[WARN] iframe no encontrado. URL actual: {page.url}")
        raise RuntimeError("No se encontró el botón Exportar de PagoNube")

    # Click en "Exportar listado" si existe (abre modal)
    try:
        frame.get_by_role("button", name="Exportar listado").click(timeout=5000)
        pace()
    except PWTimeout:
        pass

    # Interceptar la descarga
    print("[OK] Disparando export...")
    with page.expect_download(timeout=MAX_TOTAL_SECONDS * 1000) as dl_info:
        frame.get_by_role("button", name="Exportar").click()

    download = dl_info.value
    print(f"[OK] Descarga iniciada: {download.suggested_filename}")

    # Esperar mensaje de listo (opcional, la descarga ya arrancó)
    try:
        frame.get_by_text("¡Listo! Ya podés descargar el listado").wait_for(
            state="visible", timeout=30000
        )
        print("[OK] PagoNube: listado listo")
    except PWTimeout:
        print("[WARN] No apareció el mensaje de listo (igual seguimos)")

    # Guardar en temp y leer
    tmp = tempfile.mktemp(suffix=".csv")
    download.save_as(tmp)
    with open(tmp, "r", encoding="utf-8-sig", errors="replace") as f:
        content = f.read()
    Path(tmp).unlink(missing_ok=True)

    print(f"[OK] CSV leído: {len(content)} chars, "
          f"{len(content.splitlines())} líneas")
    return content


# =========================
# MAIN
# =========================
def run():
    print("=" * 55)
    print("PAGONUBE EXPORT — GitHub Actions")
    print("=" * 55)

    csv_text = None

    with sync_playwright() as p:
        # Lanzar Chromium headless (sin persistent context, sin rutas Windows)
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            accept_downloads=True,
        )

        cargar_session(context)

        page = context.new_page()
        csv_text = export_pagonube(page)

        context.close()
        browser.close()

    if not csv_text:
        raise RuntimeError("No se obtuvo el CSV de PagoNube")

    # Convertir a JSON
    rows = csv_a_json(csv_text)
    json_bytes = json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8")

    # Subir a S3
    ok = subir_a_s3(json_bytes, "pagonube.json")
    if not ok:
        raise RuntimeError("No se pudo subir pagonube.json a S3")

    print("=" * 55)
    print("COMPLETADO OK")
    print("=" * 55)


if __name__ == "__main__":
    run()
