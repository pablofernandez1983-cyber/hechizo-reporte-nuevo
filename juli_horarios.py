"""
juli_horarios.py — Calendario de horarios de Juli.

- juli.html (Juli, con su propia clave JULI_KEY en X-Juli-Key) carga su semana habitual
  y los cambios de días puntuales (otro horario, "no vengo", nota).
- La app del celu (index.html, clave del reporte en X-Web-Key) los ve en Otros → Calendario de Juli.

Tablas en Supabase hechizo:
  juli_semana_tipo(dia 1=lunes..7, bloques [["09:00","16:00"],...], almuerzo_min)
  juli_dias(fecha, bloques, almuerzo_min, nota)   → pisa la semana habitual ese día
"""

import hmac
import json
import os
import re
import time
from datetime import date, timedelta

from flask import Blueprint, jsonify, request

from auth_clave import chequear_clave

DATABASE_URL = os.environ.get("DATABASE_URL", "")

juli_bp = Blueprint("juli", __name__, url_prefix="/juli")

HORA = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
MAX_DIAS = 120


@juli_bp.before_request
def _auth():
    if request.method == "OPTIONS":
        return None
    juli = os.environ.get("JULI_KEY", "")
    k = request.headers.get("X-Juli-Key", "")
    if juli and k and hmac.compare_digest(k.encode(), juli.encode()):
        if request.endpoint == "juli.clave_juli":   # la clave de Juli solo la ve el dueño
            return jsonify({"ok": False, "error": "no autorizado"}), 403
        return None
    if k:
        time.sleep(0.5)
        return jsonify({"ok": False, "error": "Clave incorrecta"}), 401
    return chequear_clave()


def _q(sql, params=None, fetch=True):
    import psycopg2, psycopg2.extras
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or {})
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _min(h):
    a, b = h.split(":")
    return int(a) * 60 + int(b)


def _horas(bloques, almuerzo):
    tot = sum(max(0, _min(b) - _min(a)) for a, b in bloques) - (almuerzo or 0)
    return round(max(0, tot) / 60, 2)


def _validar(body):
    """(bloques, almuerzo) validados o lanza ValueError."""
    bloques = body.get("bloques")
    if not isinstance(bloques, list) or len(bloques) > 4:
        raise ValueError("bloques inválidos")
    limpio = []
    for b in bloques:
        if not (isinstance(b, (list, tuple)) and len(b) == 2 and all(isinstance(x, str) and HORA.match(x) for x in b)):
            raise ValueError("hora inválida (usar HH:MM)")
        if _min(b[1]) <= _min(b[0]):
            raise ValueError(f"{b[0]}–{b[1]}: la salida tiene que ser después de la entrada")
        limpio.append([b[0], b[1]])
    limpio.sort(key=lambda b: _min(b[0]))
    for x, y in zip(limpio, limpio[1:]):
        if _min(y[0]) < _min(x[1]):
            raise ValueError("los horarios se superponen")
    alm = int(body.get("almuerzo_min") or 0)
    if alm < 0 or alm > 180:
        raise ValueError("almuerzo inválido")
    if not limpio:
        alm = 0
    return limpio, alm


def _fecha(s):
    return date.fromisoformat(str(s)[:10])


@juli_bp.route("/calendario")
def calendario():
    try:
        desde = _fecha(request.args.get("desde"))
        hasta = _fecha(request.args.get("hasta"))
    except Exception:
        return jsonify({"ok": False, "error": "desde/hasta inválidos"}), 400
    if hasta < desde or (hasta - desde).days > MAX_DIAS:
        return jsonify({"ok": False, "error": "rango inválido"}), 400

    tipo = {r["dia"]: r for r in _q("select dia, bloques, almuerzo_min from juli_semana_tipo order by dia")}
    cargados = {r["fecha"]: r for r in _q(
        "select fecha, bloques, almuerzo_min, nota, updated_at from juli_dias where fecha between %(d)s and %(h)s",
        {"d": desde, "h": hasta})}

    dias = []
    f = desde
    while f <= hasta:
        if f in cargados:
            r = cargados[f]
            d = {"origen": "cambio", "bloques": r["bloques"], "almuerzo_min": r["almuerzo_min"], "nota": r["nota"] or ""}
        else:
            t = tipo.get(f.isoweekday()) or {"bloques": [], "almuerzo_min": 0}
            d = {"origen": "habitual", "bloques": t["bloques"], "almuerzo_min": t["almuerzo_min"], "nota": ""}
        d["fecha"] = f.isoformat()
        d["horas"] = _horas(d["bloques"], d["almuerzo_min"])
        dias.append(d)
        f += timedelta(days=1)

    semana = [{"dia": k, "bloques": v["bloques"], "almuerzo_min": v["almuerzo_min"],
               "horas": _horas(v["bloques"], v["almuerzo_min"])} for k, v in sorted(tipo.items())]
    return jsonify({"ok": True, "semana_tipo": semana,
                    "horas_semana_tipo": round(sum(s["horas"] for s in semana), 2), "dias": dias})


@juli_bp.route("/semana-tipo", methods=["PUT"])
def guardar_semana_tipo():
    body = request.get_json(silent=True) or {}
    filas = body.get("dias")
    if not isinstance(filas, list) or not filas:
        return jsonify({"ok": False, "error": "faltan días"}), 400
    try:
        datos = []
        for d in filas:
            dia = int(d.get("dia"))
            if not 1 <= dia <= 7:
                raise ValueError("día inválido")
            bl, alm = _validar(d)
            datos.append((dia, bl, alm))
    except (ValueError, TypeError) as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    for dia, bl, alm in datos:
        _q("""insert into juli_semana_tipo (dia, bloques, almuerzo_min, updated_at) values (%(d)s, %(b)s, %(a)s, now())
              on conflict (dia) do update set bloques = excluded.bloques, almuerzo_min = excluded.almuerzo_min, updated_at = now()""",
           {"d": dia, "b": json.dumps(bl), "a": alm}, fetch=False)
    return jsonify({"ok": True})


@juli_bp.route("/dia", methods=["PUT"])
def guardar_dia():
    body = request.get_json(silent=True) or {}
    try:
        f = _fecha(body.get("fecha"))
        bl, alm = _validar(body)
    except (ValueError, TypeError) as e:
        return jsonify({"ok": False, "error": str(e) or "fecha inválida"}), 400
    nota = str(body.get("nota") or "").strip()[:300]
    _q("""insert into juli_dias (fecha, bloques, almuerzo_min, nota, updated_at) values (%(f)s, %(b)s, %(a)s, %(n)s, now())
          on conflict (fecha) do update set bloques = excluded.bloques, almuerzo_min = excluded.almuerzo_min,
          nota = excluded.nota, updated_at = now()""",
       {"f": f, "b": json.dumps(bl), "a": alm, "n": nota or None}, fetch=False)
    return jsonify({"ok": True})


@juli_bp.route("/dia", methods=["DELETE"])
def volver_a_habitual():
    try:
        f = _fecha(request.args.get("fecha"))
    except Exception:
        return jsonify({"ok": False, "error": "fecha inválida"}), 400
    _q("delete from juli_dias where fecha = %(f)s", {"f": f}, fetch=False)
    return jsonify({"ok": True})


@juli_bp.route("/clave")
def clave_juli():
    """Para el botón de WhatsApp de la app: arma el mensaje con el link y la clave de Juli."""
    return jsonify({"ok": True, "clave": os.environ.get("JULI_KEY", "")})
