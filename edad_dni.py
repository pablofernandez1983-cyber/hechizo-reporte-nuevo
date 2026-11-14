"""
edad_dni.py — Estima el año de nacimiento a partir del número de DNI argentino.

El DNI se asigna en forma correlativa, así que el número da una idea del año de nacimiento.
Los puntos de 14M a 43M salen de calibrar con 250 personas reales (DNI + fecha de nacimiento,
listado público de becas UNC, nov-2026); los extremos son aproximados. Error típico ±2-3 años
por persona (hay gente que tramita tarde), pero el promedio de muchas clientas es confiable.
Extranjeros (DNI 9x millones) no dan edad. Solo se usa para estadística agregada.
"""

import re

_T = [(6, 1950), (11, 1956), (14.5, 1962.3), (16.5, 1965.4), (18.5, 1967.1), (22.5, 1972.7),
      (23.5, 1973.5), (24.5, 1975.2), (25.5, 1976.9), (26.5, 1978.0), (27.5, 1979.5), (28.5, 1980.9),
      (29.5, 1982.0), (30.5, 1984.0), (31.5, 1984.8), (32.5, 1986.7), (33.5, 1987.5), (34.5, 1989.5),
      (35.5, 1990.8), (36.5, 1992.6), (37.5, 1993.4), (38.5, 1994.8), (39.5, 1996.2), (40.5, 1997.4),
      (41.5, 1998.8), (42.5, 2000.1), (43.5, 2001.3), (46.5, 2005), (50.5, 2010), (55, 2015), (58, 2020)]


def anio_nacimiento(identificacion):
    """DNI o CUIT/CUIL (con o sin guiones) -> año de nacimiento estimado (float) o None."""
    d = re.sub(r"\D", "", str(identificacion or ""))
    if len(d) == 11:  # CUIT/CUIL: el DNI son los 8 del medio
        d = d[2:10]
    if len(d) not in (7, 8):
        return None
    n = int(d)
    if n < 1_000_000 or n >= 90_000_000:
        return None
    m = n / 1e6
    if m <= _T[0][0]:
        return round(_T[0][1] - (_T[0][0] - m) * 1.5, 1)
    for (a, y1), (b, y2) in zip(_T, _T[1:]):
        if m <= b:
            return round(y1 + (m - a) * (y2 - y1) / (b - a), 1)
    return float(_T[-1][1])


def anio_nacimiento_orden(o):
    """Año de nacimiento estimado de la clienta de una orden de Tiendanube."""
    return anio_nacimiento(o.get("contact_identification") or (o.get("customer") or {}).get("identification"))
