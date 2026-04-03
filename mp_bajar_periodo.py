"""
mp_bajar_periodo.py
===================
Script puntual para bajar un período específico de MP settlement,
esperar que procese, y agregarlo al caché S3 deduplicando.

Período hardcodeado: 2025-03-16 → 2025-06-12
"""

import os, json, time
from datetime import datetime, timedelta, timezone

TZ_AR = timezone(timedelta(hours=-3))
def ahora_ar(): return datetime.now(TZ_AR)

MP_TOKEN   = os.environ.get("MP_ACCESS_TOKEN", "")
S3_BUCKET  = os.environ.get("AWS_S3_BUCKET_NAME", "")
S3_KEY_ID  = os.environ.get("AWS_ACCESS_KEY_ID", "")
S3_SECRET  = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT= os.environ.get("AWS_ENDPOINT_URL", "")
S3_REGION  = os.environ.get("AWS_DEFAULT_REGION", "auto")

DESDE = "2025-03-16"
HASTA = "2025-06-12"

def log(msg):
    print(f"[{ahora_ar().strftime('%H:%M:%S')}] {msg}", flush=True)

# ─── S3 ────────────────────────────────────────────────────────

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

# ─── MP ────────────────────────────────────────────────────────

import requests as req

def main():
    log("=" * 60)
    log(f"MP BAJAR PERÍODO: {DESDE} → {HASTA}")
    log("=" * 60)

    if not MP_TOKEN:
        log("[ERROR] MP_ACCESS_TOKEN no configurado"); return
    if not S3_BUCKET:
        log("[ERROR] S3 no configurado"); return

    headers = {"Authorization": f"Bearer {MP_TOKEN}"}
    base    = "https://api.mercadopago.com"

    # ── Paso 1: crear reporte en MP ───────────────────────────
    log(f"\nCreando reporte MP para {DESDE} → {HASTA}...")
    try:
        r = req.post(
            f"{base}/v1/account/settlement_report",
            headers=headers,
            json={"begin_date": f"{DESDE}T00:00:00Z",
                  "end_date":   f"{HASTA}T23:59:59Z"}
        )
        if r.status_code not in (200, 202):
            log(f"[ERROR] create: {r.status_code} {r.text[:300]}"); return
        report_id = r.json().get("id")
        log(f"Reporte creado — ID: {report_id}")
    except Exception as e:
        log(f"[ERROR] create: {e}"); return

    # ── Paso 2: polling hasta 40 intentos × 30s = 20 min ─────
    log("\nEsperando que MP procese el reporte...")
    file_name = None
    for intento in range(1, 41):
        time.sleep(30)
        try:
            lista = req.get(
                f"{base}/v1/account/settlement_report/list",
                headers=headers, timeout=30
            ).json()
            nuestro = next((r for r in lista if r.get("id") == report_id), None)
            if nuestro:
                status = nuestro.get("status", "")
                log(f"  Intento {intento}/40: {status}")
                if nuestro.get("file_name") and status == "processed":
                    file_name = nuestro["file_name"]
                    break
                elif status == "error":
                    log("[ERROR] MP reportó error"); return
            else:
                log(f"  Intento {intento}/40: esperando...")
        except Exception as e:
            log(f"  [WARN] polling: {e}")

    if not file_name:
        log("[ERROR] Timeout — reporte no procesado en 20 minutos"); return

    # ── Paso 3: descargar CSV ─────────────────────────────────
    log(f"\nDescargando CSV: {file_name}")
    try:
        r = req.get(
            f"{base}/v1/account/settlement_report/{file_name}",
            headers=headers, timeout=60
        )
        content = r.text
        lines_nuevas = content.splitlines()
        log(f"Descargado: {len(lines_nuevas)} líneas")
    except Exception as e:
        log(f"[ERROR] descarga: {e}"); return

    if not lines_nuevas:
        log("[ERROR] CSV vacío"); return

    # Mostrar muestra de fechas del CSV nuevo
    sep = ";" if lines_nuevas[0].count(";") >= lines_nuevas[0].count(",") else ","
    try:
        hdr = [c.strip().strip('"').upper() for c in lines_nuevas[0].split(sep)]
        i_date = next((i for i, c in enumerate(hdr) if c in ("SETTLEMENT_DATE", "TRANSACTION_DATE")), 0)
        fechas = [l.split(sep)[i_date].strip().strip('"')[:10]
                  for l in lines_nuevas[1:] if l.strip()]
        fechas_unicas = sorted(set(fechas))
        log(f"Fechas en el CSV: {fechas_unicas[0]} → {fechas_unicas[-1]} ({len(fechas_unicas)} días distintos)")
    except:
        pass

    # ── Paso 4: agregar al caché S3 y deduplicar ─────────────
    log("\nAcutalizando caché S3...")
    cache = s3_leer("mp_settlement_lines.json") or {}
    lines_historico = cache.get("lines", [])

    if not lines_historico:
        log("[ERROR] mp_settlement_lines.json vacío — corrí check_mp_cache.py primero"); return

    log(f"  Histórico actual: {len(lines_historico)} líneas")

    # Agregar nuevas al final
    todas = lines_historico + lines_nuevas[1:]  # skip header duplicado
    log(f"  Combinado: {len(todas)} líneas (antes de deduplicar)")

    # Deduplicar por source_id + transaction_type
    sep_det = ";" if todas[0].count(";") >= todas[0].count(",") else ","
    try:
        hdr    = [c.strip().strip('"').upper() for c in todas[0].split(sep_det)]
        i_src  = next((i for i, c in enumerate(hdr) if c == "SOURCE_ID"), 1)
        i_type = next((i for i, c in enumerate(hdr) if c == "TRANSACTION_TYPE"), -1)
    except:
        i_src = 1; i_type = -1

    visto = set()
    lines_dedup = []
    for line in reversed(todas[1:]):
        if not line.strip(): continue
        cols  = line.split(sep_det)
        src   = cols[i_src].strip().strip('"')  if len(cols) > i_src  else ""
        ttype = cols[i_type].strip().strip('"') if i_type >= 0 and len(cols) > i_type else ""
        clave = f"{src}|{ttype}"
        if src and clave in visto: continue
        if src: visto.add(clave)
        lines_dedup.append(line)

    lines_dedup.reverse()
    resultado = [todas[0]] + lines_dedup
    duplicados = len(todas) - len(resultado)
    log(f"  Deduplicado: {len(resultado)} líneas ({duplicados} duplicados eliminados)")

    # Guardar en S3
    s3_guardar("mp_settlement_lines.json", {"lines": resultado})
    log(f"  mp_settlement_lines.json guardado ✓")

    log("\n" + "=" * 60)
    log("COMPLETADO")
    log(f"  Líneas nuevas agregadas: {len(lines_nuevas) - 1}")
    log(f"  Total en caché:          {len(resultado)}")
    log("")
    log("Próximos pasos:")
    log("  1. Verificar fechas en Supabase")
    log("  2. TRUNCATE TABLE mp_settlement + detalle_pnl + pnl_mensual")
    log("  3. Correr con MP_REWRITE_DB=true")
    log("=" * 60)

if __name__ == "__main__":
    main()
