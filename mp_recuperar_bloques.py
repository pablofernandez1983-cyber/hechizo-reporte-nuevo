"""
mp_recuperar_bloques.py
=======================
Script puntual para recuperar bloques de MP settlement que quedaron
pendientes en corridas anteriores.

Lógica:
  1. Lee el caché parcial de S3 para saber qué bloques ya están OK
  2. Calcula todos los bloques desde ANO_DESDE hasta hoy
  3. Para cada bloque faltante:
     a. Busca en la lista de MP si ya existe un reporte processed
        (puede haber quedado de corridas anteriores que timeoutearon)
     b. Si existe → descarga directo
     c. Si no → crea uno nuevo y espera hasta 40 intentos (20 min)
  4. Guarda progreso en S3 después de cada bloque exitoso
  5. Al final, si todos los bloques están OK → mueve a caché definitivo

Variables de entorno requeridas (las mismas de Railway):
  MP_ACCESS_TOKEN, MP_USER_ID
  AWS_S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
  AWS_ENDPOINT_URL, AWS_DEFAULT_REGION
  ANO_DESDE  (default: 2022)
"""

import os, json
from datetime import datetime, date, timedelta, timezone

TZ_AR     = timezone(timedelta(hours=-3))
def ahora_ar(): return datetime.now(TZ_AR)

ANO_DESDE  = int(os.environ.get("ANO_DESDE",  2022))
MP_TOKEN   = os.environ.get("MP_ACCESS_TOKEN", "")
MP_USER_ID = os.environ.get("MP_USER_ID",      "")
S3_BUCKET  = os.environ.get("AWS_S3_BUCKET_NAME", "")
S3_KEY_ID  = os.environ.get("AWS_ACCESS_KEY_ID",  "")
S3_SECRET  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT= os.environ.get("AWS_ENDPOINT_URL",  "")
S3_REGION  = os.environ.get("AWS_DEFAULT_REGION", "auto")

def log(msg):
    print(f"[{ahora_ar().strftime('%H:%M:%S')}] {msg}", flush=True)

# ═══════════════════════════════════════════════════════════════
# S3
# ═══════════════════════════════════════════════════════════════

_s3 = None

def get_s3():
    global _s3
    if _s3 is None:
        import boto3
        from botocore.config import Config
        _s3 = boto3.client(
            "s3", endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_KEY_ID, aws_secret_access_key=S3_SECRET,
            region_name=S3_REGION, config=Config(signature_version="s3v4")
        )
    return _s3

def s3_leer(key):
    try:
        obj = get_s3().get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        if "NoSuchKey" in str(e) or "404" in str(e): return None
        log(f"  [WARN] S3 leer {key}: {e}"); return None

def s3_guardar(key, data):
    try:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        get_s3().put_object(Bucket=S3_BUCKET, Key=key, Body=body,
                            ContentType="application/json")
        return True
    except Exception as e:
        log(f"  [WARN] S3 guardar {key}: {e}"); return False

def s3_borrar(key):
    try:
        get_s3().delete_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception as e:
        log(f"  [WARN] S3 borrar {key}: {e}"); return False

# ═══════════════════════════════════════════════════════════════
# MP
# ═══════════════════════════════════════════════════════════════

import requests as req

def mp_listar_reportes(headers, base):
    """Devuelve lista completa de reportes de MP."""
    try:
        r = req.get(f"{base}/v1/account/settlement_report/list",
                    headers=headers, timeout=30)
        return r.json() if r.ok else []
    except Exception as e:
        log(f"  [WARN] MP listar: {e}"); return []

def mp_descargar_archivo(file_name, headers, base):
    """Descarga el CSV de un reporte ya processed."""
    try:
        r = req.get(f"{base}/v1/account/settlement_report/{file_name}",
                    headers=headers, timeout=60)
        return r.text if r.ok else None
    except Exception as e:
        log(f"  [WARN] MP descargar {file_name}: {e}"); return None

def mp_descargar_si_existe(desde_str, lista_mp, headers, base):
    """
    Solo descarga si ya existe un reporte processed en la lista de MP.
    No crea nada nuevo.
    """
    # MP a veces genera el begin_date 1-2 días antes del pedido
    # por eso buscamos por proximidad en lugar de exacto
    try:
        desde_dt = date.fromisoformat(desde_str)
    except:
        desde_dt = None

    def _begin_cerca(rep):
        bd = rep.get("begin_date", "")[:10]
        try:
            bd_dt = date.fromisoformat(bd)
            return abs((bd_dt - desde_dt).days) <= 2
        except:
            return False

    existente = next((
        rep for rep in lista_mp
        if rep.get("status") == "processed"
        and rep.get("file_name")
        and (desde_dt and _begin_cerca(rep))
    ), None)

    if not existente:
        return None  # no está disponible, sin drama

    log(f"  → Encontrado: ID={existente.get('id')} "
        f"{existente.get('begin_date','')[:10]} → {existente.get('end_date','')[:10]}")
    content = mp_descargar_archivo(existente["file_name"], headers, base)
    if content:
        log(f"  → Descargado: {len(content.splitlines())} líneas")
    else:
        log(f"  → [WARN] descarga falló")
    return content

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    log("=" * 60)
    log("MP RECUPERAR BLOQUES — script puntual")
    log(f"ANO_DESDE={ANO_DESDE} | bucket={'OK' if S3_BUCKET else 'NO'}")
    log("=" * 60)

    if not MP_TOKEN:
        log("[ERROR] MP_ACCESS_TOKEN no configurado"); return
    if not S3_BUCKET:
        log("[ERROR] S3 no configurado"); return

    headers = {"Authorization": f"Bearer {MP_TOKEN}"}
    base    = "https://api.mercadopago.com"

    # ── Calcular todos los bloques desde ANO_DESDE hasta hoy ─────
    cursor    = date(ANO_DESDE, 1, 1)
    hoy       = ahora_ar().date()
    bloques   = []
    while cursor <= hoy:
        hasta = min(cursor + timedelta(days=89), hoy)
        bloques.append((cursor.strftime("%Y-%m-%d"), hasta.strftime("%Y-%m-%d")))
        cursor = hasta + timedelta(days=1)

    log(f"Total bloques a cubrir: {len(bloques)}")

    # ── Cargar caché parcial existente ────────────────────────────
    cache_parcial   = s3_leer("mp_settlement_parcial.json") or {}
    cache_completo  = s3_leer("mp_settlement_lines.json")   or {}

    if cache_completo.get("lines"):
        log("[INFO] Ya existe caché completo (mp_settlement_lines.json)")
        log("       Para forzar reconstrucción borrá ese archivo de S3 y corré de nuevo.")
        log("       Continuando de todas formas con el parcial...")

    bloques_ok      = set(cache_parcial.get("bloques_ok", []))
    todas_las_lines = cache_parcial.get("lines", [])
    header_ref      = todas_las_lines[0] if todas_las_lines else None

    faltantes = [(d, h) for d, h in bloques if f"{d}_{h}" not in bloques_ok]
    log(f"Bloques ya en caché: {len(bloques_ok)}")
    log(f"Bloques faltantes:   {len(faltantes)}")

    if not faltantes:
        log("¡Todo el histórico ya está en caché! Nada que hacer.")
        return

    # ── Consultar lista de MP una sola vez ───────────────────────
    log("Consultando lista de reportes disponibles en MP...")
    lista_mp = mp_listar_reportes(headers, base)
    log(f"MP tiene {len(lista_mp)} reportes en total:")
    for rep in lista_mp:
        log(f"  ID={rep.get('id')} | status={rep.get('status','?'):12s} | "
            f"{rep.get('begin_date','')[:10]} → {rep.get('end_date','')[:10]}")
    log("")

    # ── Una sola pasada: descarga solo los que ya están processed ─
    log("── Descargando bloques disponibles (sin crear nada nuevo) ─")
    no_disponibles = []

    for i, (desde, hasta) in enumerate(faltantes, 1):
        clave = f"{desde}_{hasta}"
        log(f"[{i}/{len(faltantes)}] Bloque {desde} → {hasta}")

        content = mp_descargar_si_existe(desde, lista_mp, headers, base)

        if not content:
            log(f"  → No disponible en MP todavía")
            no_disponibles.append((desde, hasta))
            continue

        lines_bloque = content.splitlines()
        if not lines_bloque:
            no_disponibles.append((desde, hasta))
            continue

        if header_ref is None:
            header_ref = lines_bloque[0]
            todas_las_lines.extend(lines_bloque)
        else:
            todas_las_lines.extend(lines_bloque[1:])

        bloques_ok.add(clave)
        s3_guardar("mp_settlement_parcial.json", {
            "bloques_ok": list(bloques_ok),
            "lines": todas_las_lines
        })
        log(f"  → OK ({len(bloques_ok)}/{len(bloques)} bloques en caché)")
        log("")

    # ── Resumen ───────────────────────────────────────────────────
    log("=" * 60)
    log("RESUMEN")
    log(f"  Bloques totales:       {len(bloques)}")
    log(f"  Bloques en caché:      {len(bloques_ok)}")
    log(f"  Descargados ahora:     {len(faltantes) - len(no_disponibles)}")
    log(f"  No disponibles en MP:  {len(no_disponibles)}")

    if no_disponibles:
        log("")
        log("  Bloques que NO estaban en MP:")
        for d, h in no_disponibles:
            log(f"    {d} → {h}")
        log("")
        log("  Estos bloques todavía no tienen reporte generado en MP.")
        log("  Para generarlos: corré reporte_nuevo.py con MP_FORCE_REFRESH=true")
        log("  o esperá a la próxima corrida automática.")

    # Si todos completos → mover a caché definitivo
    if len(bloques_ok) >= len(bloques):
        log("")
        log("✓ Todos los bloques completos → guardando caché definitivo")
        s3_guardar("mp_settlement_lines.json", {"lines": todas_las_lines})
        s3_borrar("mp_settlement_parcial.json")
        log("  mp_settlement_lines.json ✓")
        log("  mp_settlement_parcial.json borrado ✓")
        log("  La próxima corrida de reporte_nuevo.py bajará solo los últimos 30 días.")
    else:
        faltantes_aun = len(bloques) - len(bloques_ok)
        log("")
        log(f"  Caché parcial guardado — faltan {faltantes_aun} bloques.")

    log("=" * 60)

if __name__ == "__main__":
    main()
