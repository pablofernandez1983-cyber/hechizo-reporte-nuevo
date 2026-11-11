"""
tn_transaccion_historico.py — Reconstruye la historia del costo por transacción de Tiendanube
en la solapa «Tiendanube_transaccion» del Sheet de gastos (se corrió una vez, el 11/11/2026).

Tiendanube cobra un porcentaje de lo que se vende fuera de Pago Nube:
  - por MercadoPago lo descuenta del cobro (es la parte de FEE_AMOUNT del settlement que no es
    ni cuotas ni comisión de marketplace: 2% hasta abr-2023, 1% desde jun-2023);
  - por transferencia / efectivo lo factura al mes siguiente ("Costos por transacción").
Ninguno de los dos estaba en el P&L.

De ago-2026 en adelante el dato sale de las facturas (recurrentes.sync_tn_transaccion). Para
atrás, las facturas ya no se pueden bajar, así que:
  - MercadoPago  = lo realmente descontado según mp_settlement (dato real);
  - Transferencia y otros = ventas de ese mes por otros medios × el porcentaje que MercadoPago
    muestra ese mes (estimado; de jul a oct-2026 difiere menos de 2% de la factura).
Todo va en el mes SIGUIENTE al de las ventas, igual que la factura.

    python tn_transaccion_historico.py             # muestra lo que escribiría
    python tn_transaccion_historico.py --guardar   # crea la solapa si falta y escribe
"""

import os
import sys

HASTA_FACTURA = (2026, 7)   # último mes de factura que se reconstruye; desde 2026-08 hay facturas


def _env():
    aqui = os.path.dirname(os.path.abspath(__file__))
    for linea in open(os.path.join(aqui, ".env"), encoding="utf-8"):
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            os.environ.setdefault(k, v.strip().strip('"').strip("'"))


def _siguiente(k):
    return (k[0] + 1, 1) if k[1] == 12 else (k[0], k[1] + 1)


def calcular():
    import psycopg2
    conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=15)
    try:
        cur = conn.cursor()
        cur.execute("""
            WITH r AS (
                SELECT anio, mes, transaction_amount AS monto,
                       -(fee_amount - COALESCE(financing_fee_amount, 0) - COALESCE(mkp_fee_amount, 0)) AS cargo
                FROM mp_settlement)
            SELECT anio, mes, SUM(cargo),
                   SUM(cargo) FILTER (WHERE ABS(cargo) > 1 AND monto > 0)
                     / NULLIF(SUM(monto) FILTER (WHERE ABS(cargo) > 1 AND monto > 0), 0)
            FROM r GROUP BY 1, 2
        """)
        mp = {(a, m): (float(c or 0), float(t) if t is not None else None) for a, m, c, t in cur.fetchall()}
        cur.execute("""
            SELECT anio, mes, SUM(total)
            FROM ventas
            WHERE estado_pago IN ('paid', 'authorized') AND total > 0
              AND medio_pago NOT ILIKE '%%mercado pago%%' AND medio_pago NOT ILIKE '%%pago nube%%'
              AND medio_pago NOT ILIKE '%%getnet%%'
            GROUP BY 1, 2
        """)
        otros = {(a, m): float(t or 0) for a, m, t in cur.fetchall()}
    finally:
        conn.close()

    filas = []
    for k in sorted(set(mp) | set(otros)):
        factura = _siguiente(k)
        if factura > HASTA_FACTURA or factura[0] < 2022:
            continue
        cargo, tasa = mp.get(k, (0.0, None))
        fecha = f"{factura[0]}-{factura[1]:02d}-01"
        ventas = f"ventas {k[0]}-{k[1]:02d}"
        if cargo > 0.5:
            filas.append([fecha, round(cargo, 2), "MercadoPago (automático)",
                          f"reconstruido: descontado por MercadoPago, {ventas}"])
        if tasa is None:
            tasa = 0.02 if k < (2023, 5) else 0.01
        est = round(otros.get(k, 0.0) * tasa, 2)
        if est > 0.5:
            filas.append([fecha, est, "Transferencia y otros",
                          f"reconstruido: estimado {tasa * 100:.2f}% de {ventas} por transferencia/efectivo"])
    return filas


def main():
    _env()
    import reporte_nuevo as rn
    filas = calcular()
    por_anio = {}
    for f in filas:
        por_anio[f[0][:4]] = por_anio.get(f[0][:4], 0) + f[1]
    for f in filas[-8:]:
        print(f)
    print(f"{len(filas)} filas | total por año de factura:", {a: round(v) for a, v in por_anio.items()})
    if "--guardar" not in sys.argv:
        print("(modo prueba: no se escribió nada; usar --guardar)")
        return

    svc = rn.get_svc().spreadsheets()
    solapa = rn.SOLAPA_TN_TRANSACCION
    meta = svc.get(spreadsheetId=rn.SHEET_ID_GASTOS, fields="sheets.properties.title").execute()
    if solapa not in {s["properties"]["title"] for s in meta["sheets"]}:
        svc.batchUpdate(spreadsheetId=rn.SHEET_ID_GASTOS,
                        body={"requests": [{"addSheet": {"properties": {"title": solapa}}}]}).execute()
        print(f"solapa «{solapa}» creada")
    existentes = svc.values().get(spreadsheetId=rn.SHEET_ID_GASTOS, range=f"'{solapa}'!A:D").execute().get("values", [])
    if len(existentes) > 1:
        print(f"La solapa ya tiene {len(existentes) - 1} filas: no se escribe para no duplicar.")
        return
    svc.values().update(spreadsheetId=rn.SHEET_ID_GASTOS, range=f"'{solapa}'!A1", valueInputOption="RAW",
                        body={"values": [["Fecha", "Importe", "Tipo", "Origen"]] + filas}).execute()
    print(f"{len(filas)} filas escritas en «{solapa}»")


if __name__ == "__main__":
    main()
