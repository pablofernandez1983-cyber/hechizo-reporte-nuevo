"""
envios_seguimiento.py — Paquetes del Correo que esperan en sucursal (para que no vuelvan).

Todas las mañanas (ventas-service llama POST /envios/revisar a las 7) se consulta el
seguimiento público del Correo de los pedidos despachados que todavía no se entregaron:
  wsFacade.php action=ecommerce → historial (EN ESPERA EN SUCURSAL, ENTREGA EN SUCURSAL,
  DOMICILIO CERRADO/1 VISITA, RETORNAND0, PLAZO VENCIDO NO RECLAMO...).

Avisos (cada uno una sola vez por pedido, y solo si están activados en envios_config):
  - mail recordatorio: al 4º día en sucursal ("te quedan 3 días")
  - mail domicilio cerrado: el cartero fue y no había nadie
  - WhatsApp: el día anterior a la devolución, la app ofrece el botón (no se manda solo)
El Correo devuelve a los ~7 días corridos de llegar a la sucursal (relevado ago–oct 2026).

Andreani no tiene seguimiento público sin credenciales: queda afuera.
"""

import base64
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape

import requests
from flask import Blueprint, jsonify, request

from auth_clave import exigir_clave

DATABASE_URL = os.environ.get("DATABASE_URL", "")
TN_STORE_ID = os.environ.get("TIENDANUBE_STORE_ID", "")
TN_TOKEN = os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")
TZ_AR = timezone(timedelta(hours=-3))

envios_bp = Blueprint("envios", __name__, url_prefix="/envios")
exigir_clave(envios_bp)

CORREO_URL = "https://www.correoargentino.com.ar/sites/all/modules/custom/ca_forms/api/wsFacade.php"
RX_FILA = re.compile(r'Fecha:">(.*?)</td><td data-title="Planta:">(.*?)</td>'
                     r'<td data-title="Historia:">(.*?)</td><td data-title="Estado:">(.*?)</td>', re.S)

DIAS_PLAZO = 7          # el Correo devuelve a los ~7 días corridos de llegar a la sucursal
DIA_RECORDATORIO = 4    # mail "te quedan 3 días"
DIA_WHATSAPP = 6        # el día anterior a la devolución
DIAS_ATRAS = 30         # pedidos de los últimos 30 días
DIAS_SEMANA = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
         "septiembre", "octubre", "noviembre", "diciembre"]

_lock = threading.Lock()


# ─────────────────────────────── base ───────────────────────────────

def _q(sql, params=None, fetch=True):
    import psycopg2, psycopg2.extras
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or {})
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _config():
    return {r["clave"]: r["valor"] for r in _q("select clave, valor from envios_config")}


def _ahora():
    return datetime.now(TZ_AR)


def whatsapp_ar(tel):
    d = re.sub(r"\D", "", tel or "")
    if not d:
        return ""
    d = d.lstrip("0")
    if not d.startswith("54"):
        d = "54" + d
    if not d.startswith("549"):
        d = "549" + d[2:]
    return d


def _fecha_txt(d):
    return f"{DIAS_SEMANA[d.weekday()]} {d.day} de {MESES[d.month - 1]}"


# ─────────────────────────────── fuentes ───────────────────────────────

def _ordenes_tn():
    """Pedidos pagos de los últimos DIAS_ATRAS días con seguimiento del Correo."""
    desde = (_ahora() - timedelta(days=DIAS_ATRAS)).strftime("%Y-%m-%dT00:00:00-03:00")
    h = {"Authentication": f"bearer {TN_TOKEN}", "User-Agent": "HechizoBijou (hechizobijou@gmail.com)"}
    campos = ("id,number,shipping_option,shipping_tracking_number,shipping_status,shipping_pickup_type,"
              "shipping_pickup_details,contact_email,contact_name,contact_phone,customer,status")
    out, page = [], 1
    while page <= 5:
        r = requests.get(f"https://api.tiendanube.com/v1/{TN_STORE_ID}/orders", headers=h, timeout=40,
                         params={"per_page": 200, "page": page, "payment_status": "paid",
                                 "created_at_min": desde, "fields": campos})
        if r.status_code == 404:
            break
        r.raise_for_status()
        lote = r.json()
        out += lote
        if len(lote) < 200:
            break
        page += 1
    return [o for o in out
            if str(o.get("shipping_tracking_number") or "").startswith("1978") and o.get("status") != "cancelled"]


def _eventos_correo(tracking):
    """Historial del Correo, del más viejo al más nuevo: [(fecha, planta, historia, estado)]."""
    r = requests.post(CORREO_URL, data={"action": "ecommerce", "id": tracking},
                      headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    html = r.content.decode("utf-8-sig", "replace")
    ev = []
    for f, planta, hist, est in RX_FILA.findall(html):
        try:
            dt = datetime.strptime(f.strip(), "%d-%m-%Y %H:%M").replace(tzinfo=TZ_AR)
        except ValueError:
            continue
        ev.append((dt, planta.strip(), hist.strip(), est.strip()))
    ev.sort(key=lambda e: e[0])
    return ev


def _analizar(ev):
    """Estado del paquete a partir del historial del Correo."""
    estados = [e[3].upper() for e in ev]
    res = {"estado": "en_camino", "llego_sucursal_at": None, "domicilio_cerrado_at": None,
           "planta_sucursal": None, "ultimo_evento": None, "ultimo_evento_at": None}
    if ev:
        u = ev[-1]
        res["ultimo_evento"] = (u[3] or u[2]).capitalize()
        res["ultimo_evento_at"] = u[0]
    for dt, planta, _h, est in ev:
        e = est.upper()
        if e == "EN ESPERA EN SUCURSAL" and not res["llego_sucursal_at"]:
            res["llego_sucursal_at"], res["planta_sucursal"] = dt, planta
        if e.startswith("DOMICILIO CERRADO") and not res["domicilio_cerrado_at"]:
            res["domicilio_cerrado_at"] = dt
    if any(e in ("ENTREGADO", "ENTREGA EN SUCURSAL") for e in estados):
        res["estado"] = "retirado"
    elif any(e.startswith("RETORNAND") or e.startswith("PLAZO VENCIDO") for e in estados):
        res["estado"] = "devuelto"
    elif res["llego_sucursal_at"]:
        res["estado"] = "en_sucursal"
    elif res["domicilio_cerrado_at"]:
        res["estado"] = "domicilio_cerrado"
    return res


# ─────────────────────────────── mails ───────────────────────────────

def _mail(to_addr, asunto, cuerpo_html):
    from stock_notifications import _get_gmail_access_token
    gmail_user = os.environ.get("GMAIL_USER", "hechizobijou@gmail.com")
    html = f"""<!DOCTYPE html><html><body style="margin:0;padding:0;background:#fbf5f8;font-family:Arial,Helvetica,sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#fbf5f8;padding:32px 16px"><tr><td align="center">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;background:#ffffff;border-radius:18px;padding:28px 26px;color:#2a1225">
<tr><td>
<div style="font-family:Georgia,serif;font-size:24px;color:#8e3b78;margin-bottom:18px">Hechizo</div>
{cuerpo_html}
<p style="font-size:13px;color:#6b4f63;margin:26px 0 0;border-top:1px solid #f0e2ea;padding-top:16px;line-height:1.6">
Cualquier duda, respondé este mail. ¡Gracias por tu compra! 💜<br>
<a href="https://hechizo.com.ar" style="color:#8e3b78">hechizo.com.ar</a></p>
</td></tr></table></td></tr></table></body></html>"""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = asunto
    msg["From"] = f"Hechizo <{gmail_user}>"
    msg["To"] = to_addr
    msg.attach(MIMEText(html, "html", "utf-8"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    r = requests.post("https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
                      headers={"Authorization": f"Bearer {_get_gmail_access_token()}"},
                      json={"raw": raw}, timeout=20)
    if not r.ok:
        raise RuntimeError(f"Gmail {r.status_code}: {r.text[:200]}")


def _p(t):
    return f'<p style="font-size:15px;line-height:1.6;margin:0 0 14px">{t}</p>'


def _mail_recordatorio(f):
    limite = (f["llego_sucursal_at"] + timedelta(days=DIAS_PLAZO - 1)).date()
    nombre = escape((f["cliente"] or "").split(" ")[0] or "")
    suc = escape(f["sucursal"] or "la sucursal del Correo")
    dire = f' ({escape(f["sucursal_dir"])})' if f.get("sucursal_dir") else ""
    cuerpo = (_p(f"¡Hola{(' ' + nombre) if nombre else ''}!")
              + _p(f"Tu pedido <b>#{f['numero']}</b> te está esperando en <b>{suc}</b>{dire}.")
              + _p(f"Tenés tiempo de retirarlo hasta el <b>{_fecha_txt(limite)}</b>. "
                   "Después de esa fecha el Correo lo devuelve.")
              + _p("Para retirarlo llevá tu DNI. Si va otra persona, que lleve una autorización firmada y una copia de tu DNI.")
              + _p(f'<a href="https://www.correoargentino.com.ar/formularios/e-commerce?id={f["tracking"]}" '
                   'style="display:inline-block;background:#8e3b78;color:#fff;text-decoration:none;'
                   'padding:11px 18px;border-radius:12px;font-weight:bold">Ver el seguimiento</a>'))
    _mail(f["email"], f"Tu pedido de Hechizo te espera en la sucursal (hasta el {limite.day}/{limite.month})", cuerpo)


def _mail_domicilio(f):
    nombre = escape((f["cliente"] or "").split(" ")[0] or "")
    cuerpo = (_p(f"¡Hola{(' ' + nombre) if nombre else ''}!")
              + _p(f"El Correo pasó a entregarte tu pedido <b>#{f['numero']}</b> y no encontró a nadie en el domicilio.")
              + _p("Puede volver a intentarlo o dejarlo en la sucursal más cercana. "
                   "Te recomendamos mirar el seguimiento para no perdértelo:")
              + _p(f'<a href="https://www.correoargentino.com.ar/formularios/e-commerce?id={f["tracking"]}" '
                   'style="display:inline-block;background:#8e3b78;color:#fff;text-decoration:none;'
                   'padding:11px 18px;border-radius:12px;font-weight:bold">Ver el seguimiento</a>'))
    _mail(f["email"], f"El Correo no pudo entregarte tu pedido de Hechizo #{f['numero']}", cuerpo)


# ─────────────────────────────── revisión ───────────────────────────────

def revisar():
    """Actualiza el estado de los paquetes en viaje y manda los mails que correspondan."""
    if not _lock.acquire(blocking=False):
        return {"ok": False, "error": "ya se está revisando"}
    try:
        cfg = _config()
        mails_on = cfg.get("mails_activos") is True
        guardados = {r["order_id"]: r for r in _q("select * from envios_seguimiento")}
        ordenes = _ordenes_tn()
        resumen = {"consultados": 0, "en_sucursal": 0, "mails": 0, "errores": 0}
        for o in ordenes:
            prev = guardados.get(o["id"])
            if prev and prev["estado"] in ("retirado", "devuelto"):
                continue
            if not prev and o.get("shipping_status") in ("delivered", "unpacked", "unshipped", None):
                continue   # ni salió o ya se entregó: no hace falta consultar
            try:
                ev = _eventos_correo(o["shipping_tracking_number"])
            except Exception as e:
                print(f"[ENVIOS] #{o['number']} error Correo: {e}")
                resumen["errores"] += 1
                continue
            resumen["consultados"] += 1
            a = _analizar(ev)
            pick = o.get("shipping_pickup_details") or {}
            ad = pick.get("address") or {}
            es_suc = o.get("shipping_pickup_type") == "pickup" or "sucursal" in str(o.get("shipping_option", "")).lower()
            suc_nombre = " ".join((pick.get("name") or "").split(" - ")[-1].split()) or a["planta_sucursal"]
            suc_dir = " ".join(x for x in [ad.get("address"), ad.get("number")] if x)
            if ad.get("locality") or ad.get("city"):
                suc_dir += (", " if suc_dir else "") + (ad.get("locality") or ad.get("city"))
            cust = o.get("customer") or {}
            fila = {
                "order_id": o["id"], "numero": o["number"], "carrier": "correo",
                "tracking": o["shipping_tracking_number"],
                "cliente": o.get("contact_name") or cust.get("name"),
                "email": o.get("contact_email") or cust.get("email"),
                "whatsapp": whatsapp_ar(o.get("contact_phone") or cust.get("phone")),
                "tipo": "sucursal" if es_suc else "domicilio",
                "sucursal": suc_nombre if (es_suc and suc_nombre) else a["planta_sucursal"],
                "sucursal_dir": suc_dir if es_suc else "",
                "estado": a["estado"], "ultimo_evento": a["ultimo_evento"], "ultimo_evento_at": a["ultimo_evento_at"],
                "llego_sucursal_at": a["llego_sucursal_at"], "domicilio_cerrado_at": a["domicilio_cerrado_at"],
            }
            _q("""insert into envios_seguimiento (order_id, numero, carrier, tracking, cliente, email, whatsapp, tipo,
                    sucursal, sucursal_dir, estado, ultimo_evento, ultimo_evento_at, llego_sucursal_at, domicilio_cerrado_at, actualizado_at)
                  values (%(order_id)s, %(numero)s, %(carrier)s, %(tracking)s, %(cliente)s, %(email)s, %(whatsapp)s, %(tipo)s,
                    %(sucursal)s, %(sucursal_dir)s, %(estado)s, %(ultimo_evento)s, %(ultimo_evento_at)s, %(llego_sucursal_at)s,
                    %(domicilio_cerrado_at)s, now())
                  on conflict (order_id) do update set cliente = excluded.cliente, email = excluded.email,
                    whatsapp = excluded.whatsapp, tipo = excluded.tipo, sucursal = excluded.sucursal,
                    sucursal_dir = excluded.sucursal_dir, estado = excluded.estado, ultimo_evento = excluded.ultimo_evento,
                    ultimo_evento_at = excluded.ultimo_evento_at, llego_sucursal_at = excluded.llego_sucursal_at,
                    domicilio_cerrado_at = excluded.domicilio_cerrado_at, actualizado_at = now()""", fila, fetch=False)
            if a["estado"] == "en_sucursal":
                resumen["en_sucursal"] += 1
            if not (mails_on and fila["email"]):
                continue
            ya = prev or {}
            try:
                if (a["estado"] == "en_sucursal" and not ya.get("mail_recordatorio_at")
                        and (_ahora() - a["llego_sucursal_at"]).days >= DIA_RECORDATORIO - 1
                        and (_ahora() - a["llego_sucursal_at"]).days < DIAS_PLAZO):
                    _mail_recordatorio(fila)
                    _q("update envios_seguimiento set mail_recordatorio_at = now() where order_id = %(i)s", {"i": o["id"]}, fetch=False)
                    resumen["mails"] += 1
                elif (a["estado"] == "domicilio_cerrado" and not ya.get("mail_domicilio_at")
                        and (_ahora() - a["domicilio_cerrado_at"]).days <= 3):
                    _mail_domicilio(fila)
                    _q("update envios_seguimiento set mail_domicilio_at = now() where order_id = %(i)s", {"i": o["id"]}, fetch=False)
                    resumen["mails"] += 1
            except Exception as e:
                print(f"[ENVIOS] #{o['number']} error mail: {e}")
                resumen["errores"] += 1
            time.sleep(0.3)
        _q("""insert into envios_config (clave, valor) values ('ultima_revision', %(v)s)
              on conflict (clave) do update set valor = excluded.valor""",
           {"v": json.dumps(_ahora().isoformat())}, fetch=False)
        print(f"[ENVIOS] revisión: {resumen}")
        return {"ok": True, **resumen}
    finally:
        _lock.release()


# ─────────────────────────────── rutas ───────────────────────────────

@envios_bp.route("/revisar", methods=["POST"])
def ruta_revisar():
    # Corre en segundo plano: el Correo tarda ~1 s por paquete
    threading.Thread(target=revisar, daemon=True).start()
    return jsonify({"ok": True, "iniciado": True})


@envios_bp.route("")
def lista():
    cfg = _config()
    filas = _q("""select * from envios_seguimiento
                  where estado in ('en_sucursal', 'domicilio_cerrado')
                     or (estado = 'devuelto' and actualizado_at > now() - interval '15 days')
                  order by llego_sucursal_at nulls last, numero""")
    ahora = _ahora()
    out = []
    for f in filas:
        d = {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in f.items()}
        if f["llego_sucursal_at"]:
            dias = (ahora - f["llego_sucursal_at"]).days
            d["dias_en_sucursal"] = dias
            d["devolucion_estimada"] = (f["llego_sucursal_at"] + timedelta(days=DIAS_PLAZO)).date().isoformat()
            d["toca_whatsapp"] = f["estado"] == "en_sucursal" and dias >= DIA_WHATSAPP - 1 and not f["whatsapp_at"]
        out.append(d)
    return jsonify({"ok": True, "paquetes": out,
                    "mails_activos": cfg.get("mails_activos") is True,
                    "whatsapp_activo": cfg.get("whatsapp_activo") is True,
                    "ultima_revision": cfg.get("ultima_revision")})


@envios_bp.route("/config", methods=["PUT"])
def guardar_config():
    body = request.get_json(silent=True) or {}
    for k in ("mails_activos", "whatsapp_activo"):
        if k in body:
            _q("""insert into envios_config (clave, valor) values (%(k)s, %(v)s)
                  on conflict (clave) do update set valor = excluded.valor""",
               {"k": k, "v": json.dumps(bool(body[k]))}, fetch=False)
    return jsonify({"ok": True})


@envios_bp.route("/<int:order_id>/whatsapp", methods=["POST"])
def marcar_whatsapp(order_id):
    _q("update envios_seguimiento set whatsapp_at = now() where order_id = %(i)s", {"i": order_id}, fetch=False)
    return jsonify({"ok": True})
