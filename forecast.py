"""
forecast.py — Proyección de cierre del período en curso (día / semana / mes) para la
solapa Estadísticas de la app.

    proyección = vendido + base × (1 − F) × k

- base: lo que se vende "normalmente" en un período así, sacado SOLO de historia reciente
  (28 días / 8 semanas / 3 meses) y llevado a pesos de hoy con el IPC. Con la inflación y
  el crecimiento del negocio, comparar contra el año pasado da peor.
- F: qué fracción del período suele estar vendida a esta altura (perfil por hora del día,
  por día+hora de la semana, o por avance del mes). La historia vieja solo se usa para
  esta forma, que no depende del tamaño del negocio.
- k: corrección por cómo viene el período, amortiguada (C_CONFIANZA): al arrancar el
  período (vendido = 0, F = 0) la proyección es la base, no 0.

Backtest oct-25 → sep-26 (error absoluto medio sobre el cierre real, promedio de varios
momentos del período), contra la regla de tres lineal que se usaba antes:
día 33% vs 60%, semana 13% vs 41%, mes 8% vs 41%.
"""

import os
import time
import calendar
from datetime import datetime, timedelta

import requests
from flask import Blueprint, jsonify, request

from auth_clave import exigir_clave

forecast_bp = Blueprint("forecast", __name__)
exigir_clave(forecast_bp)

TZ = "America/Argentina/Buenos_Aires"
C_CONFIANZA = 2          # cuánto pesa la base frente al ritmo del período (backtest: 1-2 ≈ igual)
DIAS_PERFIL = 182        # historia para el perfil por hora (día y semana)
MESES_PERFIL = 12        # meses promediados para el perfil del mes
BINS_MES = 200

IPC_URL = ("https://apis.datos.gob.ar/series/api/series/"
           "?ids=148.3_INIVELNAL_DICI_M_26&limit=100&format=json&start_date=2021-01-01")
IPC_TTL = 12 * 3600
IPC_REINTENTO = 10 * 60
_ipc_cache = {"ts": 0, "fallo_ts": 0, "meses": None}
_resp_cache = {}
RESP_TTL = 120


# ─────────────────────────── IPC ───────────────────────────

def _ipc_mensual():
    """{(año, mes): nivel} del IPC nacional (INDEC vía datos.gob.ar). None si no se pudo bajar."""
    if _ipc_cache["meses"] is not None and time.time() - _ipc_cache["ts"] < IPC_TTL:
        return _ipc_cache["meses"]
    if time.time() - _ipc_cache["fallo_ts"] < IPC_REINTENTO:
        return _ipc_cache["meses"]   # el último bueno (si hubo), sin reintentar en cada request
    try:
        r = requests.get(IPC_URL, timeout=15)
        r.raise_for_status()
        meses = {}
        for fecha, valor in r.json()["data"]:
            if valor is not None:
                y, m = int(fecha[:4]), int(fecha[5:7])
                meses[(y, m)] = float(valor)
        if len(meses) >= 4:
            _ipc_cache.update(ts=time.time(), meses=meses)
    except Exception as e:
        print(f"[WARN] forecast: no se pudo bajar el IPC ({e})")
        _ipc_cache["fallo_ts"] = time.time()
    return _ipc_cache["meses"]


def _indice_precios():
    """Función fecha → nivel de precios (interpolado diario, anclado a mitad de mes y
    extrapolado con la inflación promedio de los últimos 3 meses publicados).
    Sin IPC devuelve una constante (= pesos nominales)."""
    meses = _ipc_mensual()
    if not meses:
        return (lambda d: 1.0), False
    claves = sorted(meses)
    tasa = (meses[claves[-1]] / meses[claves[-4]]) ** (1 / 3)

    def nivel_mes(y, m):
        if (y, m) in meses:
            return meses[(y, m)]
        if (y, m) < claves[0]:
            return meses[claves[0]]
        uy, um = claves[-1]
        n = (y - uy) * 12 + (m - um)
        return meses[claves[-1]] * tasa ** n

    def indice(d):
        ancla = datetime(d.year, d.month, 15)
        if d >= ancla:
            sy, sm = (d.year + (d.month == 12), d.month % 12 + 1)
            sig = datetime(sy, sm, 15)
            a, b = nivel_mes(d.year, d.month), nivel_mes(sy, sm)
            t = (d - ancla) / (sig - ancla)
        else:
            py, pm = (d.year - (d.month == 1), (d.month - 2) % 12 + 1)
            ant = datetime(py, pm, 15)
            a, b = nivel_mes(py, pm), nivel_mes(d.year, d.month)
            t = (d - ant) / (ancla - ant)
        return a * (b / a) ** t   # interpolación geométrica

    return indice, True


# ─────────────────────────── períodos ───────────────────────────

def _limites(gran, ahora):
    hoy = datetime(ahora.year, ahora.month, ahora.day)
    if gran == "mes":
        ini = datetime(ahora.year, ahora.month, 1)
        fin = ini + timedelta(days=calendar.monthrange(ahora.year, ahora.month)[1])
        return ini, fin, ini.strftime("%Y-%m")
    if gran == "semana":
        ini = hoy - timedelta(days=hoy.weekday())
        return ini, ini + timedelta(days=7), ini.strftime("%Y-%m-%d")
    return hoy, hoy + timedelta(days=1), hoy.strftime("%Y-%m-%d")


def _menos_meses(d, n):
    y, m = d.year, d.month - n
    while m <= 0:
        y, m = y - 1, m + 12
    return datetime(y, m, 1)


def _curva(pesos):
    """Acumulada normalizada (len+1 puntos, de 0 a 1)."""
    tot = sum(pesos) or 1
    c, acc = [0.0], 0.0
    for p in pesos:
        acc += p
        c.append(acc / tot)
    return c


def _F(curva, frac):
    n = len(curva) - 1
    p = max(0.0, min(1.0, frac)) * n
    i = min(int(p), n - 1)
    return curva[i] + (curva[i + 1] - curva[i]) * (p - i)


# ─────────────────────────── cálculo ───────────────────────────

def calcular(gran, ventas, ahora):
    """ventas: lista de (creada_AR naive, total). Devuelve el dict de la proyección."""
    ini, fin, periodo = _limites(gran, ahora)
    indice, hay_ipc = _indice_precios()
    ref = indice(ini)
    ajuste = indice(ini + (fin - ini) / 2) / ref   # inflación esperada dentro del período

    # Base (en pesos del inicio del período) + perfil
    if gran == "dia":
        w_ini = ini - timedelta(days=28)
        n_periodos = 28
    elif gran == "semana":
        w_ini = ini - timedelta(days=56)
        n_periodos = 8
    else:
        w_ini = _menos_meses(ini, 3)
        n_periodos = (ini - w_ini).days / ((fin - ini).days)

    base_total = base_cant = 0.0
    for t, tot in ventas:
        if w_ini <= t < ini:
            base_total += tot * ref / indice(t)
            base_cant += 1
    base_total = base_total / n_periodos * ajuste
    base_cant = base_cant / n_periodos

    if gran in ("dia", "semana"):
        nb = 24 if gran == "dia" else 168
        pesos = [0.0] * nb
        p_ini = ini - timedelta(days=DIAS_PERFIL)
        for t, tot in ventas:
            if p_ini <= t < ini:
                b = t.hour if gran == "dia" else t.weekday() * 24 + t.hour
                pesos[b] += tot
        curva = _curva(pesos)
    else:
        curvas = []
        for k in range(1, MESES_PERFIL + 1):
            a, b = _menos_meses(ini, k), _menos_meses(ini, k - 1)
            pesos = [0.0] * BINS_MES
            for t, tot in ventas:
                if a <= t < b:
                    pesos[min(int((t - a) / (b - a) * BINS_MES), BINS_MES - 1)] += tot
            if sum(pesos) > 0:
                curvas.append(_curva(pesos))
        curva = [sum(c[i] for c in curvas) / len(curvas) for i in range(BINS_MES + 1)] if curvas else None

    frac_tiempo = (ahora - ini) / (fin - ini)
    F = _F(curva, frac_tiempo) if curva else frac_tiempo

    vendido_total = sum(tot for t, tot in ventas if ini <= t <= ahora)
    vendido_cant = sum(1 for t, tot in ventas if ini <= t <= ahora)

    def proyectar(A, B):
        if B <= 0:
            return A / F if F > 0 else A
        k = (A + C_CONFIANZA * B) / (B * F + C_CONFIANZA * B)
        return A + B * (1 - F) * k

    return {
        "ok": True,
        "granularidad": gran,
        "periodo": periodo,
        "total": round(proyectar(vendido_total, base_total)),
        "cantidad": round(proyectar(vendido_cant, base_cant), 1),
        "vendido_total": round(vendido_total),
        "vendido_cantidad": vendido_cant,
        "base_total": round(base_total),
        "base_cantidad": round(base_cant, 1),
        "fraccion_esperada": round(F, 4),
        "fraccion_tiempo": round(frac_tiempo, 4),
        "ajuste_ipc": hay_ipc,
    }


def futuros(gran, ventas, ahora, hasta):
    """Proyección de los períodos completos que faltan después del actual, hasta `hasta`
    (inclusive; el último se recorta en esa fecha). Ritmo diario de los últimos 3 meses en
    pesos de hoy × peso del día de la semana × inflación esperada hasta ese día.
    Backtest (meses 1..6 adelante): error 14-22%, sin sesgo; agregarle estacionalidad del
    año pasado dio peor, incluso para noviembre/diciembre."""
    indice, _ = _indice_precios()
    hoy = datetime(ahora.year, ahora.month, ahora.day)
    ref = indice(hoy)
    _, fin_actual, _ = _limites(gran, ahora)

    w_ini = _menos_meses(hoy, 3)
    n_w = (hoy - w_ini).days
    real_w = cant_w = 0.0
    por_dow = [0.0] * 7
    p_ini = hoy - timedelta(days=DIAS_PERFIL)
    for t, tot in ventas:
        if t >= hoy:
            continue
        if t >= w_ini:
            real_w += tot * ref / indice(t)
            cant_w += 1
        if t >= p_ini:
            por_dow[t.weekday()] += tot
    tot_dow = sum(por_dow) or 1
    factor_dow = [7 * x / tot_dow for x in por_dow] if sum(por_dow) else [1.0] * 7
    diario_real, diario_cant = real_w / n_w, cant_w / n_w

    out, ini = [], fin_actual
    while ini <= hasta:
        fin = _limites(gran, ini)[1]
        tot = cant = 0.0
        d = ini
        while d < fin and d <= hasta:
            f = factor_dow[d.weekday()]
            tot += diario_real * f * indice(d + timedelta(hours=12)) / ref
            cant += diario_cant * f
            d += timedelta(days=1)
        out.append({"periodo": _limites(gran, ini)[2], "total": round(tot), "cantidad": round(cant, 1)})
        ini = fin
    return out


def _leer_ventas(desde):
    import psycopg2
    conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=10)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT (creada AT TIME ZONE %(tz)s) AS creada_ar, total
            FROM ventas
            WHERE estado_pago IN ('paid', 'authorized')
              AND creada >= (%(desde)s::timestamp AT TIME ZONE %(tz)s)
        """, {"tz": TZ, "desde": desde})
        return [(r[0], float(r[1] or 0)) for r in cur.fetchall()]
    finally:
        conn.close()


@forecast_bp.route("/forecast/ipc")
def forecast_ipc():
    """Nivel de precios por mes ("YYYY-MM" → índice, mitad de mes) para pasar montos a pesos
    de hoy en la app. Incluye los meses todavía no publicados (extrapolados) hasta fin del
    año que viene, con la misma regla que usa el forecast."""
    indice, hay_ipc = _indice_precios()
    if not hay_ipc:
        return jsonify({"ok": False, "error": "IPC no disponible"}), 503
    meses = _ipc_mensual()
    ultimo = max(meses)
    hoy = datetime.now()
    out, d = {}, datetime(2022, 1, 15)
    while d.year <= hoy.year + 1:
        out[d.strftime("%Y-%m")] = round(indice(d), 4)
        d = datetime(d.year + (d.month == 12), d.month % 12 + 1, 15)
    return jsonify({"ok": True, "meses": out, "publicado_hasta": f"{ultimo[0]}-{ultimo[1]:02d}"})


@forecast_bp.route("/forecast")
def forecast():
    """Proyección de cierre del período en curso. Query: granularidad=dia|semana|mes y
    opcional hasta=YYYY-MM-DD para proyectar también los períodos siguientes (`futuros`)."""
    gran = request.args.get("granularidad", "dia")
    if gran not in ("dia", "semana", "mes"):
        gran = "dia"
    hasta_txt = request.args.get("hasta", "")
    try:
        hasta = datetime.strptime(hasta_txt, "%Y-%m-%d") if hasta_txt else None
    except ValueError:
        return jsonify({"ok": False, "error": "hasta debe ser YYYY-MM-DD"}), 400
    if hasta and hasta > datetime.now() + timedelta(days=400):
        return jsonify({"ok": False, "error": "hasta: como máximo ~1 año adelante"}), 400
    cache = _resp_cache.get((gran, hasta_txt))
    if cache and time.time() - cache[0] < RESP_TTL:
        return jsonify(cache[1])
    try:
        from zoneinfo import ZoneInfo
        ahora = datetime.now(ZoneInfo(TZ)).replace(tzinfo=None)
        ini, _, _ = _limites(gran, ahora)
        desde = _menos_meses(ini, MESES_PERFIL) - timedelta(days=DIAS_PERFIL if gran != "mes" else 0)
        ventas = _leer_ventas(desde)
        res = calcular(gran, ventas, ahora)
        if hasta:
            res["futuros"] = futuros(gran, ventas, ahora, hasta)
        _resp_cache[(gran, hasta_txt)] = (time.time(), res)
        return jsonify(res)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
