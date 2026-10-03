"""
recurrentes.py — Gastos fijos que se repiten cada mes en el Sheet de gastos.

- Sueldo Juli, Agencia (y aguinaldo en julio/diciembre): la app del celu muestra una
  tarjeta "¿igual que el mes pasado?" mientras el mes no tenga su fila; al confirmar
  se agrega la fila en la solapa, en el mismo formato que la carga a mano.
- Abono Tiendanube: lo escribe solo `sync_tn_abono()` con las facturas que baja
  pagonube_export.py (GitHub Actions → S3 tn_facturas.json), sumando Plan + Nuvem Chat.
  Solo pasa a tarjeta si a partir del día 20 el mes sigue sin fila.

Si ese mes ya tiene la fila (cargada a mano o desde la app), no pregunta nada.
"""

import re
from datetime import date

from flask import Blueprint, jsonify, request

import reporte_nuevo as rn
from auth_clave import exigir_clave

rec_bp = Blueprint("recurrentes", __name__)
exigir_clave(rec_bp)

SOLAPA_TN = "Tiendanube_abono"
DIA_TARJETA_TN = 20      # antes de esto el abono del mes todavía puede no estar facturado
MESES_TN_AUTO = 2        # el sync solo toca el mes actual y el anterior (nunca historia)

# Detalle con que se graba la fila nueva = el que se venía tipeando
CONCEPTOS = {
    "sueldo_juli": {"label": "Sueldo Juli", "solapa": "Sueldos",
                    "patron": r"^\s*sueldo\s+juli", "detalle": "sueldo juli"},
    "aguinaldo_juli": {"label": "Aguinaldo Juli", "solapa": "Sueldos",
                       "patron": r"^\s*aguinaldo\s+juli", "detalle": "aguinaldo juli",
                       "meses": (7, 12)},
    "agencia": {"label": "Agencia", "solapa": "Publicidad",
                "patron": r"agencia", "detalle": "agencia"},
}

MESES_ES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
            "agosto", "septiembre", "octubre", "noviembre", "diciembre"]


def _hoy():
    return rn.ahora_ar().date()


def _mes_anterior(a, m):
    return (a - 1, 12) if m == 1 else (a, m - 1)


def _nombre_mes(k):
    return MESES_ES[k[1] - 1]


def _omitidos():
    return rn.s3_leer("recurrentes_omitidos.json") or {}


# ─────────────────────────────── solapas de gastos ───────────────────────────────

def _filas_concepto(cid):
    """[(mes_key, importe, detalle)] de las filas de la solapa que son de ese concepto."""
    c = CONCEPTOS[cid]
    filas, _ = rn._leer_solapa_filas([c["solapa"]], 3, False, c["label"], verbose=False)
    return [(x["k"], -x["valor"], x["detalle"]) for x in filas
            if re.search(c["patron"], x["detalle"], re.I)]


def _filas_tn():
    return [(x["k"], x["importe"]) for x in rn._leer_tn_abono_filas(verbose=False)]


def _agregar_fila_gasto(cid, importe, dia):
    c = CONCEPTOS[cid]
    fecha = f"{dia.day}/{dia.month:02d}/{dia.year}"   # Sheet en es_ES: d/mm/aaaa
    rn.get_svc().spreadsheets().values().append(
        spreadsheetId=rn.SHEET_ID_GASTOS, range=f"'{c['solapa']}'!A:D",
        valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS",
        body={"values": [[fecha, c["detalle"], "", importe]]}).execute()


def _sheet_gid(nombre):
    meta = rn.get_svc().spreadsheets().get(
        spreadsheetId=rn.SHEET_ID_GASTOS, fields="sheets.properties(sheetId,title)").execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == nombre:
            return s["properties"]["sheetId"]
    raise RuntimeError(f"No existe la solapa {nombre}")


def _escribir_tn(k, importe, nota):
    """Crea o corrige la fila del mes k en Tiendanube_abono (la más nueva va arriba)."""
    svc = rn.get_svc().spreadsheets()
    filas = rn.leer_hoja(rn.SHEET_ID_GASTOS, SOLAPA_TN)
    fecha = f"{k[0]}-{k[1]:02d}-01"
    for i, row in enumerate(filas[1:], start=2):
        if row and rn.mes_key(str(row[0]).strip()) == k:
            svc.values().update(
                spreadsheetId=rn.SHEET_ID_GASTOS, range=f"'{SOLAPA_TN}'!B{i}:C{i}",
                valueInputOption="USER_ENTERED", body={"values": [[importe, nota]]}).execute()
            return "corregida"
    svc.batchUpdate(spreadsheetId=rn.SHEET_ID_GASTOS, body={"requests": [{
        "insertDimension": {"range": {"sheetId": _sheet_gid(SOLAPA_TN), "dimension": "ROWS",
                                      "startIndex": 1, "endIndex": 2},
                            "inheritFromBefore": False}}]}).execute()
    svc.values().update(
        spreadsheetId=rn.SHEET_ID_GASTOS, range=f"'{SOLAPA_TN}'!A2:C2",
        valueInputOption="USER_ENTERED", body={"values": [[fecha, importe, nota]]}).execute()
    return "agregada"


# ─────────────────────────────── abono Tiendanube ───────────────────────────────

def abono_desde_facturas(facturas):
    """{mes_key: Plan + Nuvem Chat} — solo meses que ya tienen la factura del Plan."""
    suma, con_plan = {}, set()
    for f in facturas:
        concepto = f.get("concepto", "").lower()
        es_plan = concepto.startswith("plan")
        if not (es_plan or "nuvem chat" in concepto):
            continue
        a, m = map(int, f["mes"].split("-"))
        suma[(a, m)] = round(suma.get((a, m), 0.0) + float(f["importe"]), 2)
        if es_plan:
            con_plan.add((a, m))
    return {k: v for k, v in suma.items() if k in con_plan}


def sync_tn_abono(verbose=True):
    """Graba en Tiendanube_abono el abono del mes actual y el anterior según Facturación.

    Si la fila no está, la agrega; si está con otro importe (ej. cargada a mano por
    adelantado "a revisar"), la corrige. Meses más viejos no se tocan.
    """
    datos = rn.s3_leer("tn_facturas.json")
    if not datos or not datos.get("facturas"):
        if verbose: rn.log("  TN abono auto: sin tn_facturas.json en S3")
        return
    hoy = _hoy()
    actual = (hoy.year, hoy.month)
    meses = {actual, _mes_anterior(*actual)}
    abono = {k: v for k, v in abono_desde_facturas(datos["facturas"]).items() if k in meses}
    en_sheet = {}
    for k, v in _filas_tn():
        en_sheet[k] = en_sheet.get(k, 0.0) + v
    for k, v in sorted(abono.items()):
        if abs(en_sheet.get(k, 0.0) - v) < 0.01:
            continue
        accion = _escribir_tn(k, v, f"auto Facturación TN {datos.get('leido', '')[:10]}")
        rn.log(f"  TN abono auto: {k[0]}-{k[1]:02d} = {v:,.2f} ({accion})")


# ─────────────────────────────── tarjetas de la app ───────────────────────────────

def pendientes():
    hoy = _hoy()
    k = (hoy.year, hoy.month)
    omit = set(_omitidos().get(f"{k[0]}-{k[1]:02d}", []))
    out = []

    filas_sueldo = None
    for cid, c in CONCEPTOS.items():
        if cid in omit or (c.get("meses") and k[1] not in c["meses"]):
            continue
        filas = _filas_concepto(cid)
        if cid == "sueldo_juli":
            filas_sueldo = filas
        if any(fk == k for fk, _, _ in filas):
            continue
        anteriores = [f for f in filas if f[0] < k]
        if cid == "aguinaldo_juli":
            # medio sueldo del mes (o del último cargado)
            base = [f for f in (filas_sueldo or _filas_concepto("sueldo_juli")) if f[0] <= k]
            sugerido = round(base[-1][1] / 2, 2) if base else None
            ref = "medio sueldo"
        else:
            ult = max(anteriores, key=lambda f: f[0]) if anteriores else None
            sugerido = ult[1] if ult else None
            ref = f"como {_nombre_mes(ult[0])}" if ult else ""
        out.append({"id": cid, "label": c["label"], "sugerido": sugerido, "referencia": ref})

    if "tn_abono" not in omit and hoy.day >= DIA_TARJETA_TN:
        filas = _filas_tn()
        if not any(fk == k for fk, _ in filas):
            ult = max((f for f in filas if f[0] < k), key=lambda f: f[0], default=None)
            out.append({"id": "tn_abono", "label": "Abono Tiendanube",
                        "sugerido": ult[1] if ult else None,
                        "referencia": f"como {_nombre_mes(ult[0])}" if ult else "",
                        "nota": "No se pudo leer de Facturación de Tiendanube"})

    return {"mes": f"{MESES_ES[k[1] - 1].capitalize()} {k[0]}", "pendientes": out}


@rec_bp.route("/recurrentes")
def ver_recurrentes():
    try:
        return jsonify({"ok": True, **pendientes()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:300]}), 502


@rec_bp.route("/recurrentes/confirmar", methods=["POST"])
def confirmar_recurrente():
    body = request.get_json(silent=True) or {}
    cid = body.get("id")
    if cid not in CONCEPTOS and cid != "tn_abono":
        return jsonify({"ok": False, "error": "concepto desconocido"}), 400
    hoy = _hoy()
    k = (hoy.year, hoy.month)
    clave_mes = f"{k[0]}-{k[1]:02d}"

    if body.get("omitir"):
        om = _omitidos()
        om.setdefault(clave_mes, [])
        if cid not in om[clave_mes]:
            om[clave_mes].append(cid)
        rn.s3_guardar("recurrentes_omitidos.json", om)
        return jsonify({"ok": True, "accion": "omitido"})

    try:
        importe = round(float(body.get("importe")), 2)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "importe inválido"}), 400
    if importe <= 0:
        return jsonify({"ok": False, "error": "importe inválido"}), 400

    try:
        if cid == "tn_abono":
            if any(fk == k for fk, _ in _filas_tn()):
                return jsonify({"ok": True, "accion": "ya estaba"})
            _escribir_tn(k, importe, "cargado desde la app")
        else:
            if any(fk == k for fk, _, _ in _filas_concepto(cid)):
                return jsonify({"ok": True, "accion": "ya estaba"})
            _agregar_fila_gasto(cid, importe, hoy)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:300]}), 502
    return jsonify({"ok": True, "accion": "grabado"})
