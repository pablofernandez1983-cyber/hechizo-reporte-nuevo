"""
forecast.py — Proyección de cierre del período en curso (día / semana / mes) para la
solapa Estadísticas de la app.

    proyección = vendido + base × (1 − F) × k

- base: lo que se vende "normalmente" en un período así, sacado SOLO de historia reciente
  (28 días / 8 semanas / 3 meses) y llevado a pesos de hoy con la inflación propia de
  Hechizo (índice de precios de la tienda; IPC general sólo si no se puede calcular). Con la
  inflación y el crecimiento del negocio, comparar contra el año pasado da peor.
  Backtest índice Hechizo vs IPC (meses 1-3 adelante): mismo error; desde nov-25, cuando los
  precios de la tienda empezaron a subir menos que el IPC, la mitad de sesgo (IPC se pasaba).
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
import math
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


def _funcion_indice(meses):
    """{(año, mes): nivel} → función fecha → nivel de precios (interpolado diario, anclado
    a mitad de mes y extrapolado con la variación promedio de los últimos 3 meses conocidos)."""
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

    return indice


def _indice_precios():
    """Índice del IPC general. Sin IPC devuelve una constante (= pesos nominales)."""
    meses = _ipc_mensual()
    if not meses:
        return (lambda d: 1.0), False
    return _funcion_indice(meses), True


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

def calcular(gran, ventas, ahora, indice=None):
    """ventas: lista de (creada_AR naive, total). Devuelve el dict de la proyección.
    indice: función fecha → nivel de precios; por defecto la inflación de Hechizo."""
    ini, fin, periodo = _limites(gran, ahora)
    hay_ipc = True
    if indice is None:
        indice, hay_ipc = _indice_por_defecto()
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


def futuros(gran, ventas, ahora, hasta, indice=None):
    """Proyección de los períodos completos que faltan después del actual, hasta `hasta`
    (inclusive; el último se recorta en esa fecha). Ritmo diario de los últimos 3 meses en
    pesos de hoy × peso del día de la semana × inflación esperada hasta ese día.
    Backtest (meses 1..6 adelante): error 14-22%, sin sesgo; agregarle estacionalidad del
    año pasado dio peor, incluso para noviembre/diciembre."""
    if indice is None:
        indice, _ = _indice_por_defecto()
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


# ─────────────────────────── índice de precios Hechizo ───────────────────────────
# Cuánto subieron los precios de la tienda: mismo producto (variante) mes contra mes,
# GEKS-Törnqvist con ventana móvil de 13 meses y empalme por movimiento (método de los
# institutos de estadística para datos de ventas; no se desvía con cambios de catálogo ni
# promociones). Verificado contra la comparación interanual directa del mismo producto.
HZ_TTL = 12 * 3600
HZ_VENTANA = 13
HZ_MIN_ITEMS = 8
_hz_cache = {"ts": 0, "res": None}


def _tornqvist(A, B):
    """ln del índice entre dos meses. A, B: {item: (precio_mediano, facturacion)}."""
    comunes = A.keys() & B.keys()
    if len(comunes) < HZ_MIN_ITEMS:
        return None
    ra = sum(A[i][1] for i in comunes) or 1
    rb = sum(B[i][1] for i in comunes) or 1
    tot = 0.0
    for i in comunes:
        lr = max(-1.5, min(1.5, math.log(B[i][0] / A[i][0])))
        tot += (A[i][1] / ra + B[i][1] / rb) / 2 * lr
    return tot


def indice_hechizo(filas):
    """filas: [(mes 'YYYY-MM', item, precio_mediano, facturacion)] → {mes: nivel}."""
    por_mes = {}
    for mes, item, precio, rev in filas:
        if precio and precio > 0:
            por_mes.setdefault(mes, {})[item] = (float(precio), float(rev or 0))
    meses = sorted(por_mes)
    cache = {}

    def lt(i, j):
        if i == j:
            return 0.0
        if (i, j) not in cache:
            v = _tornqvist(por_mes[meses[i]], por_mes[meses[j]])
            cache[(i, j)], cache[(j, i)] = v, (None if v is None else -v)
        return cache[(i, j)]

    def geks(ini, t):
        vals = [lt(ini, l) + lt(l, t) for l in range(ini, t + 1)
                if lt(ini, l) is not None and lt(l, t) is not None]
        return sum(vals) / len(vals) if vals else None

    ln = [0.0]
    for t in range(1, len(meses)):
        ini = max(0, t - HZ_VENTANA + 1)
        a, b = geks(ini, t - 1), geks(ini, t)
        ln.append(ln[-1] + (b - a if a is not None and b is not None else 0.0))
    return {m: math.exp(v) for m, v in zip(meses, ln)}


def _leer_precios_mensuales():
    import psycopg2
    conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=10)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT TO_CHAR(v.fecha, 'YYYY-MM') AS mes,
                   COALESCE(d.variante_id, d.producto_id) AS item,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d.precio_unitario) AS precio,
                   SUM(d.precio_unitario * d.cantidad) AS rev
            FROM ventas_detalle d JOIN ventas v USING (orden_id)
            WHERE v.estado_pago IN ('paid', 'authorized')
              AND d.precio_unitario > 0 AND d.cantidad > 0
            GROUP BY 1, 2
        """)
        return cur.fetchall()
    finally:
        conn.close()


def _niveles_hechizo():
    """{'YYYY-MM': nivel} del índice Hechizo (cacheado HZ_TTL)."""
    if _hz_cache.get("niveles") is None or time.time() - _hz_cache.get("niveles_ts", 0) >= HZ_TTL:
        _hz_cache["niveles"] = indice_hechizo(_leer_precios_mensuales())
        _hz_cache["niveles_ts"] = time.time()
    return _hz_cache["niveles"]


def indice_hechizo_fn(niveles, antes_de):
    """Función fecha → nivel con los meses COMPLETOS anteriores a `antes_de` (el mes en
    curso tiene pocas ventas y mueve el índice); de ahí en más, extrapolado."""
    lim = antes_de.strftime("%Y-%m")
    meses = {(int(k[:4]), int(k[5:])): v for k, v in niveles.items() if k < lim}
    return _funcion_indice(meses) if len(meses) >= 4 else None


def _indice_por_defecto():
    """Inflación de Hechizo (sus precios siguen a los proveedores). Si no se puede calcular,
    IPC general; si tampoco, nominal. Devuelve (función, hay_ajuste)."""
    try:
        f = indice_hechizo_fn(_niveles_hechizo(), datetime.now())
        if f:
            return f, True
    except Exception as e:
        print(f"[WARN] forecast: índice Hechizo no disponible ({e}); se usa IPC")
    return _indice_precios()


@forecast_bp.route("/forecast/indice-hechizo")
def forecast_indice_hechizo():
    """Nivel de precios de Hechizo por mes ("YYYY-MM" → índice), para ver montos en pesos de
    hoy según la inflación propia. Los meses siguientes al último con ventas se extrapolan
    con la variación promedio de los 3 meses previos, hasta fin del año que viene."""
    if _hz_cache["res"] and time.time() - _hz_cache["ts"] < HZ_TTL:
        return jsonify(_hz_cache["res"])
    try:
        niveles = _niveles_hechizo()
        meses = sorted(niveles)
        ultimo = meses[-1]
        # el mes en curso suele estar incompleto: la extrapolación parte del último completo
        base = meses[-2] if len(meses) > 4 else ultimo
        i = meses.index(base)
        tasa = (niveles[base] / niveles[meses[i - 3]]) ** (1 / 3)
        out = {m: round(v * 100 / niveles[meses[0]], 4) for m, v in niveles.items()}
        y, m = int(ultimo[:4]), int(ultimo[5:])
        n = 1
        while y <= datetime.now().year + 1:
            m += 1
            if m > 12:
                y, m = y + 1, 1
            out[f"{y}-{m:02d}"] = round(out[ultimo] * tasa ** n, 4)
            n += 1
        res = {"ok": True, "meses": out, "ultimo_real": ultimo}
        _hz_cache.update(ts=time.time(), res=res)
        return jsonify(res)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
