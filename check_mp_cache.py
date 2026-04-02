"""
check_mp_cache.py
=================
Diagnóstico del estado de cachés MP en S3.
Correr en Railway con las mismas variables de entorno que reporte_nuevo.py.
"""

import os, json, boto3
from botocore.config import Config
from datetime import date, timedelta

S3_BUCKET   = os.environ.get("AWS_S3_BUCKET_NAME", "")
S3_KEY_ID   = os.environ.get("AWS_ACCESS_KEY_ID", "")
S3_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "")
S3_REGION   = os.environ.get("AWS_DEFAULT_REGION", "auto")
ANO_DESDE   = int(os.environ.get("ANO_DESDE", 2022))

s3 = boto3.client(
    "s3", endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY_ID, aws_secret_access_key=S3_SECRET,
    region_name=S3_REGION, config=Config(signature_version="s3v4")
)

def leer(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        if "NoSuchKey" in str(e) or "404" in str(e):
            return None
        print(f"  [ERROR] S3 leer {key}: {e}")
        return None

print("=" * 55)
print("DIAGNÓSTICO CACHÉS MP EN S3")
print("=" * 55)

# ── 1. Caché completo ─────────────────────────────────────────
completo = leer("mp_settlement_lines.json")
if completo:
    lines = completo.get("lines", [])
    print(f"\n✓ mp_settlement_lines.json EXISTE")
    print(f"  Total líneas: {len(lines)}")
    if len(lines) > 1:
        fechas = []
        sep = ";" if lines[0].count(";") > lines[0].count(",") else ","
        for line in lines[1:]:
            if not line.strip():
                continue
            cols = line.split(sep)
            if cols:
                f = cols[0].strip().strip('"')[:10]
                if len(f) == 10 and f[4] == "-":
                    fechas.append(f)
        if fechas:
            print(f"  Fecha más vieja: {min(fechas)}")
            print(f"  Fecha más nueva: {max(fechas)}")
        else:
            print(f"  (no se pudieron parsear fechas)")
else:
    print(f"\n✗ mp_settlement_lines.json NO EXISTE")

# ── 2. Caché parcial ──────────────────────────────────────────
parcial = leer("mp_settlement_parcial.json")
if parcial and parcial.get("bloques_ok"):
    bloques_ok = parcial.get("bloques_ok", [])
    lines_p    = parcial.get("lines", [])
    print(f"\n⚠  mp_settlement_parcial.json EXISTE")
    print(f"   Bloques OK en parcial: {len(bloques_ok)}")
    print(f"   Líneas acumuladas:     {len(lines_p)}")
    print(f"   Bloques:")
    for b in sorted(bloques_ok):
        print(f"     {b}")
else:
    print(f"\n✓ mp_settlement_parcial.json vacío o inexistente (correcto)")

# ── 3. Bloques esperados ──────────────────────────────────────
print(f"\n── Bloques esperados desde {ANO_DESDE} hasta hoy ──")
cursor  = date(ANO_DESDE, 1, 1)
hoy     = date.today()
bloques = []
while cursor <= hoy:
    hasta = min(cursor + timedelta(days=89), hoy)
    bloques.append((cursor.strftime("%Y-%m-%d"), hasta.strftime("%Y-%m-%d")))
    cursor = hasta + timedelta(days=1)

print(f"  Total bloques: {len(bloques)}")
for d, h in bloques:
    print(f"  {d} → {h}")

# ── 4. Diagnóstico final ──────────────────────────────────────
print(f"\n── DIAGNÓSTICO FINAL ──────────────────────────────────")
if completo and not (parcial and parcial.get("bloques_ok")):
    print("✓ TODO OK")
    print("  La próxima corrida de reporte_nuevo.py va a:")
    print("  → Usar el caché completo")
    print("  → Bajar SOLO los últimos 30 días de MP")
    print("  → No tocar el histórico")
elif parcial and parcial.get("bloques_ok"):
    bloques_ok = set(parcial.get("bloques_ok", []))
    faltantes  = [(d, h) for d, h in bloques if f"{d}_{h}" not in bloques_ok]
    print(f"⚠  DESCARGA INCOMPLETA")
    print(f"   Bloques en caché parcial: {len(bloques_ok)}/{len(bloques)}")
    print(f"   Bloques faltantes: {len(faltantes)}")
    for d, h in faltantes:
        print(f"     {d} → {h}")
    print(f"\n   → Correr mp_recuperar_bloques.py para completar")
else:
    print("✗ SIN CACHÉ COMPLETO")
    print("  La próxima corrida va a bajar el histórico completo desde cero")

print("=" * 55)
