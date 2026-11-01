"""
andreani_seguimiento.py — Lee el seguimiento público de Andreani (corre en GitHub Actions, 6 h).

Andreani no tiene seguimiento consultable sin credenciales: la página
https://www.andreani.com/envio/<nro> la arma el navegador. Esta Action la abre con Playwright,
como una persona, solo para los envíos de Hechizo que siguen en viaje (andreani_pendientes.json,
que deja la revisión de las 7 en el reporte) y guarda lo que muestra en andreani_estado.json.
Son pocos por día y con pausa entre uno y otro.
"""

import json
import os
import re
import time
from datetime import datetime

import boto3
from playwright.sync_api import sync_playwright

S3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name=os.environ.get("AWS_DEFAULT_REGION", "auto"),
)
BUCKET = os.environ["AWS_S3_BUCKET_NAME"]

ETAPAS = {"pendiente de ingreso", "ingresado", "en camino", "en sucursal", "entregado", "devuelto",
          "en devolución", "en devolucion"}
RX_FECHA = re.compile(r"^(\d{2})-(\d{2})-(\d{4})$")
RX_HORA = re.compile(r"^(\d{2}):(\d{2}) hs\.?$")


def leer(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8"))
    except Exception:
        return None


def parsear(texto):
    """Texto de <main> → {cabecera, eventos: [[iso, texto]]}."""
    lineas = [l.strip() for l in texto.split("\n") if l.strip()]
    cab = ""
    for i, l in enumerate(lineas):
        if l.startswith("El envío de") and i + 1 < len(lineas):
            cab = lineas[i + 1]
            break
    try:
        ini = lineas.index("Paso a paso del envío") + 1
    except ValueError:
        return {"cabecera": cab, "eventos": []}
    fin = lineas.index("Preguntas frecuentes") if "Preguntas frecuentes" in lineas else len(lineas)
    eventos, fecha, actual = [], None, None
    i = ini
    while i < fin:
        l = lineas[i]
        mf = RX_FECHA.match(l)
        if mf and i + 1 < fin and RX_HORA.match(lineas[i + 1]):
            mh = RX_HORA.match(lineas[i + 1])
            fecha = datetime(int(mf.group(3)), int(mf.group(2)), int(mf.group(1)), int(mh.group(1)), int(mh.group(2)))
            actual = [fecha.isoformat() + "-03:00", ""]
            eventos.append(actual)
            i += 2
            continue
        if l.lower() in ETAPAS:
            i += 1
            continue
        if actual is not None:
            actual[1] = (actual[1] + "\n" + l).strip()
        i += 1
    return {"cabecera": cab, "eventos": eventos}


def main():
    pend = (leer("andreani_pendientes.json") or {}).get("trackings", [])
    estado = leer("andreani_estado.json") or {"envios": {}}
    print(f"{len(pend)} envíos de Andreani para leer")
    if not pend:
        return
    nuevos = {}
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(locale="es-AR")
        for n in pend:
            try:
                pg.goto(f"https://www.andreani.com/envio/{n}", wait_until="domcontentloaded", timeout=60000)
                pg.wait_for_selector("text=Paso a paso", timeout=40000)
                datos = parsear(pg.inner_text("main"))
                datos["leido"] = datetime.now().isoformat()
                nuevos[n] = datos
                print(f"  {n}: {datos['cabecera']} ({len(datos['eventos'])} eventos)")
            except Exception as e:
                print(f"  {n}: error {e}")
                if n in estado["envios"]:
                    nuevos[n] = estado["envios"][n]   # se queda con lo último que leyó
            time.sleep(4)
        b.close()
    S3.put_object(Bucket=BUCKET, Key="andreani_estado.json",
                  Body=json.dumps({"actualizado": datetime.now().isoformat(), "envios": nuevos}, ensure_ascii=False).encode("utf-8"),
                  ContentType="application/json")
    print("andreani_estado.json guardado")


if __name__ == "__main__":
    main()
