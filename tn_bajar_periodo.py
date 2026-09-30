"""
tn_bajar_periodo.py — Rebaja órdenes de Tiendanube de un rango de fechas y las mergea
en el caché S3 (tn_ordenes.json) que usa el P&L.

El reporte normal solo refresca los últimos 30 días, así que si alguna ventana falló
en el pasado queda un hueco para siempre (pasó con 22-dic-2024 → 5-ene-2025).

Uso:
  python tn_bajar_periodo.py 2022-01-01 2026-09-30            # solo informa (no escribe)
  python tn_bajar_periodo.py 2022-01-01 2026-09-30 --guardar  # backup + merge en S3

Después correr el reporte (POST /ejecutar) para recalcular P&L, Sheet y Supabase.
"""

import sys, time, json
from collections import Counter
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv

load_dotenv(".env")
import reporte_nuevo as rn


def bajar(desde, hasta):
    headers = {
        "Authentication": f"bearer {rn.TN_TOKEN}",
        "User-Agent": "HechizoBijou-Reporte/1.0 (hechizobijou@gmail.com)",
    }
    base = f"https://api.tiendanube.com/v1/{rn.TN_STORE_ID}"
    out = {}
    cursor = desde
    while cursor <= hasta:
        fin = min(cursor + timedelta(days=29), hasta)
        page = 1
        while True:
            for intento in range(4):
                try:
                    r = requests.get(f"{base}/orders", headers=headers, timeout=40, params={
                        "page": page, "per_page": 200,
                        "created_at_min": cursor.strftime("%Y-%m-%dT00:00:00-03:00"),
                        "created_at_max": fin.strftime("%Y-%m-%dT23:59:59-03:00"),
                    })
                    if r.status_code == 404:  # TN devuelve 404 cuando la página está vacía
                        batch = []
                        break
                    r.raise_for_status()
                    batch = r.json()
                    break
                except Exception as e:
                    print(f"  reintento {intento + 1} {cursor} pág {page}: {e}")
                    time.sleep(5 * (intento + 1))
            else:
                raise RuntimeError(f"No se pudo bajar {cursor}..{fin} pág {page}; se aborta sin guardar")
            for o in batch:
                out[str(o["id"])] = o
            if len(batch) < 200:
                break
            page += 1
            time.sleep(0.6)
        print(f"  {cursor} → {fin}: {len(out)} acumuladas")
        cursor = fin + timedelta(days=1)
        time.sleep(0.6)
    return out


def main():
    desde = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    hasta = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
    guardar = "--guardar" in sys.argv

    cache = rn.s3_leer("tn_ordenes.json") or {}
    print(f"Caché actual: {len(cache)} órdenes")
    nuevas = bajar(desde, hasta)
    print(f"Bajadas de TN: {len(nuevas)} órdenes")

    faltantes = [k for k in nuevas if k not in cache]
    cambios = [k for k in nuevas if k in cache and (
        cache[k].get("status"), cache[k].get("payment_status")) != (nuevas[k].get("status"), nuevas[k].get("payment_status"))]
    por_mes = Counter(nuevas[k]["created_at"][:7] for k in faltantes)
    print(f"Faltaban en el caché: {len(faltantes)}  |  cambiaron de estado: {len(cambios)}")
    for m, n in sorted(por_mes.items()):
        print(f"  faltantes {m}: {n}")

    if not guardar:
        print("Modo prueba: no se escribió nada. Agregar --guardar para aplicar.")
        return
    backup = f"tn_ordenes_backup_{datetime.now():%Y%m%d_%H%M}.json"
    if not rn.s3_guardar(backup, cache):
        raise RuntimeError("No se pudo guardar el backup; no se toca el caché")
    print(f"Backup: {backup}")
    cache.update(nuevas)
    if rn.s3_guardar("tn_ordenes.json", cache):
        print(f"Caché actualizado: {len(cache)} órdenes")


if __name__ == "__main__":
    main()
