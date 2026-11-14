"""
Tests de las funciones que mueven plata o mandan mails. No tocan la red ni la base.

    python -m unittest discover -s tests -v
"""

import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
for var in ("DATABASE_URL", "TIENDANUBE_ACCESS_TOKEN", "TIENDANUBE_STORE_ID"):
    os.environ.pop(var, None)   # que nada de lo importado pueda conectarse a producción

import reporte_nuevo as rn
import stock_notifications as sn


class SafeFloat(unittest.TestCase):
    def test_formatos(self):
        casos = {
            None: 0.0, "": 0.0, 15000: 15000.0, 12.5: 12.5,
            "1.234,56": 1234.56, "1,234.56": 1234.56, "$ 97.700,00 ": 97700.0,
            "1234.56": 1234.56, "12,5": 12.5, "1,234": 1234.0,
            "-1.500,25": -1500.25, "ARS 300": 300.0, "-": 0.0, "abc": 0.0,
        }
        for entrada, esperado in casos.items():
            self.assertAlmostEqual(rn.safe_float(entrada), esperado, msg=repr(entrada))


class MesKey(unittest.TestCase):
    def test_formatos(self):
        self.assertEqual(rn.mes_key("2026-10-31"), (2026, 10))
        self.assertEqual(rn.mes_key("31/10/2026"), (2026, 10))
        self.assertEqual(rn.mes_key("31-10-2026"), (2026, 10))
        self.assertEqual(rn.mes_key("2026-10-31T23:59:59-03:00"), (2026, 10))
        self.assertEqual(rn.mes_key(date(2025, 1, 5)), (2025, 1))
        self.assertEqual(rn.mes_key("2024 M03"), (2024, 3))
        self.assertEqual(rn.mes_key("oct-26"), (2026, 10))
        self.assertEqual(rn.mes_key("sept-2025"), (2025, 9))
        self.assertEqual(rn.mes_key("15-oct-25"), (2025, 10))
        self.assertEqual(rn.mes_key(46300), (2026, 10))          # fecha del Sheet como número

    def test_dia_mes_sin_anio(self):
        self.assertIsNone(rn.mes_key("15-oct"))
        self.assertEqual(rn.mes_key("15-oct", ano_ctx=2024), (2024, 10))

    def test_basura(self):
        self.assertIsNone(rn.mes_key("Fecha"))
        self.assertIsNone(rn.mes_key(""))


def _orden(fecha, unidades, precio, comparado, subtotal, cupon=None):
    return {
        "created_at": fecha,
        "subtotal": subtotal,
        "coupon": [{"code": cupon}] if cupon else [],
        "products": [{"quantity": unidades, "price": precio, "compare_at_price": comparado}],
    }


class Mayorista(unittest.TestCase):
    def test_cupon(self):
        self.assertTrue(rn.es_mayorista(_orden("2024-05-01T10:00:00", 1, 100, None, 100, "MAYORISTA25")))

    def test_tabla_de_precios_2026(self):
        self.assertTrue(rn.es_mayorista(_orden("2026-03-01T10:00:00", 20, 15000, 20000, 300000)))

    def test_no_llega_al_minimo(self):
        self.assertFalse(rn.es_mayorista(_orden("2026-03-01T10:00:00", 10, 15000, 20000, 300000)))   # pocas unidades
        self.assertFalse(rn.es_mayorista(_orden("2026-03-01T10:00:00", 20, 5000, 8000, 100000)))     # poco monto
        self.assertFalse(rn.es_mayorista(_orden("2026-03-01T10:00:00", 20, 15000, None, 300000)))    # sin rebaja

    def test_liquidaciones_viejas_no_cuentan(self):
        self.assertFalse(rn.es_mayorista(_orden("2024-03-01T10:00:00", 20, 15000, 20000, 300000)))


class Carrier(unittest.TestCase):
    def test_reglas(self):
        self.assertEqual(rn.carrier_de("360002123456789", "Andreani a domicilio"), "andreani")
        self.assertEqual(rn.carrier_de("19781234567", "Correo Argentino"), "correo")
        self.assertEqual(rn.carrier_de("", "Envíos Nube - Entrega rápida a domicilio"), "moto")
        self.assertEqual(rn.carrier_de("GAOC12345", "envio nube - entrega a domicilio"), "moto")
        self.assertEqual(rn.carrier_de("", "envío nube - correo argentino clásico a domicilio"), "otro")
        self.assertEqual(rn.carrier_de("", "envío nube - andreani a domicilio"), "otro")
        self.assertEqual(rn.carrier_de(None, "Retiro en showroom"), "otro")
        # con seguimiento del Correo manda el seguimiento, no el nombre del envío
        self.assertEqual(rn.carrier_de("19781234567", "Moto"), "correo")


class Multiplicadores(unittest.TestCase):
    def test_mes_cargado(self):
        self.assertEqual(rn.meta_mult((2024, 6)), 1.68)
        self.assertEqual(rn.gads_mult((2024, 6)), 1.43)

    def test_anio_nuevo_sigue_con_el_ultimo_valor(self):
        ultimo = max(rn.META_MULTIPLICADOR)
        self.assertEqual(rn.meta_mult((ultimo[0] + 1, 1)), rn.META_MULTIPLICADOR[ultimo])
        self.assertEqual(rn.gads_mult((ultimo[0] + 3, 7)), rn.GADS_MULTIPLICADOR[max(rn.GADS_MULTIPLICADOR)])

    def test_antes_de_la_tabla_usa_el_default(self):
        self.assertEqual(rn.meta_mult((2022, 5)), rn.META_MULTIPLICADOR_DEFAULT)


class Detalle(unittest.TestCase):
    def test_suma_lineas_repetidas_y_saltea_impagas(self):
        ordenes = [
            {"id": 1, "payment_status": "paid", "created_at": "2026-10-05T12:00:00-03:00",
             "products": [{"variant_id": 7, "product_id": 3, "name": "Pulsera", "quantity": 2, "price": "1000"},
                          {"variant_id": 7, "product_id": 3, "name": "Pulsera", "quantity": 1, "price": "1000"},
                          {"variant_id": 8, "product_id": 3, "name": "Pulsera", "quantity": 4, "price": "900"}]},
            {"id": 2, "payment_status": "pending", "created_at": "2026-10-05T12:00:00-03:00",
             "products": [{"variant_id": 9, "quantity": 1, "price": "10"}]},
            {"id": 3, "payment_status": "paid", "status": "cancelled", "created_at": "2026-10-05T12:00:00-03:00",
             "products": [{"variant_id": 9, "quantity": 1, "price": "10"}]},
        ]
        filas, ids = rn.filas_detalle(ordenes)
        self.assertEqual(ids, [1])
        self.assertEqual({(f[0], f[5]): f[9] for f in filas}, {(1, 7): 3, (1, 8): 4})


class AvisosDeStock(unittest.TestCase):
    def test_variante_con_stock(self):
        self.assertEqual(sn.variantes_a_notificar([10], {10: 3, 11: 0}), {10})

    def test_variante_en_cero_no_avisa_aunque_otra_tenga(self):
        self.assertEqual(sn.variantes_a_notificar([10], {10: 0, 11: 5}), set())

    def test_variante_que_ya_no_existe(self):
        self.assertEqual(sn.variantes_a_notificar([99], {10: 0, 11: 5}), {99})
        self.assertEqual(sn.variantes_a_notificar([99], {10: 0, 11: 0}), set())

    def test_producto_sin_datos(self):
        self.assertEqual(sn.variantes_a_notificar([10], {}), set())

    def test_mail(self):
        self.assertTrue(sn.RX_EMAIL.match("alguien@hotmail.com"))
        self.assertFalse(sn.RX_EMAIL.match("lau2105@hotmailcom"))
        self.assertFalse(sn.RX_EMAIL.match("sin arroba"))


class FirmaWebhook(unittest.TestCase):
    def setUp(self):
        from flask import Flask
        self.app = Flask(__name__)
        os.environ["TN_APP_CLIENT_SECRET"] = "secreto-de-prueba"

    def tearDown(self):
        os.environ.pop("TN_APP_CLIENT_SECRET", None)

    def _valida(self, cuerpo, firma):
        headers = {"X-Linkedstore-Hmac-Sha256": firma} if firma is not None else {}
        with self.app.test_request_context("/notify/webhook", method="POST", data=cuerpo, headers=headers):
            return sn._firma_webhook_valida()

    def test_firma(self):
        import hashlib, hmac
        cuerpo = b'{"store_id":1384618,"event":"product/updated","id":5}'
        buena = hmac.new(b"secreto-de-prueba", cuerpo, hashlib.sha256).hexdigest()
        self.assertTrue(self._valida(cuerpo, buena))
        self.assertFalse(self._valida(cuerpo + b" ", buena))     # cuerpo alterado
        self.assertFalse(self._valida(cuerpo, "0" * 64))         # firmado con otro secreto
        self.assertFalse(self._valida(cuerpo, None))             # sin firma


if __name__ == "__main__":
    unittest.main()


class CostoPorTransaccion(unittest.TestCase):
    def test_desde_facturas(self):
        import recurrentes as rec
        facturas = [
            {"mes": "2026-11", "numero": "A-1", "concepto": "Costos por transacción", "importe": 35638.23},
            {"mes": "2026-11", "numero": "A-2", "concepto": "Costos por transacción cobrados automáticamente", "importe": 58167.48},
            {"mes": "2026-11", "numero": "A-3", "concepto": "Costo de envío", "importe": 1655730.15},
            {"mes": "2026-10", "numero": "A-4", "concepto": "Plan - Impulso", "importe": 79999.0},
            {"mes": "2026-10", "numero": "A-5", "concepto": "Costos por transacción", "importe": 40336.33},
        ]
        r = rec.transaccion_desde_facturas(facturas)
        self.assertEqual(r[((2026, 11), rec.TIPO_OTROS)], (35638.23, ["A-1"]))
        self.assertEqual(r[((2026, 11), rec.TIPO_MP)], (58167.48, ["A-2"]))
        self.assertEqual(r[((2026, 10), rec.TIPO_OTROS)], (40336.33, ["A-5"]))
        self.assertEqual(len(r), 3)      # ni el plan ni el costo de envío

    def test_el_abono_no_incluye_el_costo_por_transaccion(self):
        import recurrentes as rec
        facturas = [
            {"mes": "2026-10", "concepto": "Plan - Impulso", "importe": 79999.0},
            {"mes": "2026-10", "concepto": "Nuvem Chat: cargos por cantidad de 261 conversaciones", "importe": 67860.0},
            {"mes": "2026-10", "concepto": "Costos por transacción", "importe": 40336.33},
        ]
        self.assertEqual(rec.abono_desde_facturas(facturas), {(2026, 10): 147859.0})
