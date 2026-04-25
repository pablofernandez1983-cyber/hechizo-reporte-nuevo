"""
stock_notifications.py — Blueprint para notificaciones de stock.

Endpoints:
  POST   /notify                      — registra solicitud (widget tienda)
  GET    /notify/list                 — lista notificaciones con filtros
  GET    /notify/products             — ranking de productos con más demanda
  GET    /notify/stats                — estadísticas generales
  POST   /notify/send/<variant_id>    — envía mails y marca como enviado
  DELETE /notify/<notif_id>           — elimina una notificación
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import Blueprint, jsonify, request
from flask_cors import cross_origin

DATABASE_URL   = os.environ.get("DATABASE_URL", "")
GMAIL_USER     = os.environ.get("GMAIL_USER", "")
GMAIL_APP_PASS = os.environ.get("GMAIL_APP_PASSWORD", "")

WIDGET_ORIGINS = [
    "https://hechizo.com.ar",
    "https://hechizobijou.com.ar",
    "https://hechizobijou.mitiendanube.com",
    "https://pruebasdehechizo.mitiendanube.com",
]

notify_bp = Blueprint("notify", __name__)

REQUIRED_FIELDS = {"email", "product_id", "variant_id", "product_name", "variant_name"}


def _get_conn():
    import psycopg2
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def _ensure_columns():
    """Agrega columnas faltantes de forma idempotente."""
    if not DATABASE_URL:
        return
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE stock_notifications
                ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ
            """)
            cur.execute("""
                ALTER TABLE stock_notifications
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()
            """)
        conn.commit()
        conn.close()
    except Exception:
        pass


_ensure_columns()


# ── POST /notify ───────────────────────────────────────────────────────────────

@notify_bp.route("/notify", methods=["POST", "OPTIONS"])
@cross_origin(origins=WIDGET_ORIGINS, methods=["POST", "OPTIONS"], allow_headers=["Content-Type"])
def notify():
    body = request.get_json(silent=True) or {}
    missing = REQUIRED_FIELDS - body.keys()
    if missing:
        return jsonify({"error": f"campos requeridos: {', '.join(sorted(missing))}"}), 400

    email        = body["email"].strip().lower()
    product_id   = int(body["product_id"])
    variant_id   = int(body["variant_id"])
    store_id     = int(body.get("store_id") or 1384618)
    product_name = str(body["product_name"])[:255]
    variant_name = str(body["variant_name"])[:255]

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stock_notifications
                  (email, product_id, variant_id, store_id, product_name, variant_name, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (email, variant_id)
                DO UPDATE SET
                  product_name = EXCLUDED.product_name,
                  variant_name = EXCLUDED.variant_name,
                  status       = 'pending',
                  sent_at      = NULL
            """, (email, product_id, variant_id, store_id, product_name, variant_name))
        conn.commit()
    finally:
        conn.close()

    return jsonify({"ok": True}), 200


# ── GET /notify/list ───────────────────────────────────────────────────────────

@notify_bp.route("/notify/list", methods=["GET"])
@cross_origin(origins="*")
def notify_list():
    email_q   = request.args.get("email", "").strip().lower()
    product_q = request.args.get("product", "").strip().lower()
    status_q  = request.args.get("status", "").strip()

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT id, email, product_name, variant_name, status,
                       created_at, sent_at
                FROM stock_notifications
                WHERE 1=1
            """
            params = []
            if email_q:
                sql += " AND LOWER(email) LIKE %s"
                params.append(f"%{email_q}%")
            if product_q:
                sql += " AND (LOWER(product_name) LIKE %s OR LOWER(variant_name) LIKE %s)"
                params.append(f"%{product_q}%")
                params.append(f"%{product_q}%")
            if status_q in ("pending", "sent"):
                sql += " AND status = %s"
                params.append(status_q)
            sql += " ORDER BY created_at DESC LIMIT 500"
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

    for r in rows:
        r["created_at"] = r["created_at"].isoformat() if r.get("created_at") else None
        r["sent_at"]    = r["sent_at"].isoformat()    if r.get("sent_at")    else None

    return jsonify({"ok": True, "items": rows})


# ── GET /notify/products ───────────────────────────────────────────────────────

@notify_bp.route("/notify/products", methods=["GET"])
@cross_origin(origins="*")
def notify_products():
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT variant_id, product_id, product_name, variant_name,
                       COUNT(*) AS cantidad
                FROM stock_notifications
                WHERE status = 'pending'
                GROUP BY variant_id, product_id, product_name, variant_name
                ORDER BY cantidad DESC
            """)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

    _enrich_with_stock(rows)
    for r in rows:
        r["cantidad"] = int(r["cantidad"])

    return jsonify({"ok": True, "items": rows})


def _enrich_with_stock(rows):
    import requests as _req
    TN_STORE_ID = os.environ.get("TIENDANUBE_STORE_ID", "")
    TN_TOKEN    = os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")
    if not TN_STORE_ID or not TN_TOKEN or not rows:
        for r in rows:
            r["current_stock"] = None
        return

    headers = {
        "Authentication": f"bearer {TN_TOKEN}",
        "User-Agent": "HechizoBijou-Stock/1.0 (hechizobijou@gmail.com)",
    }
    product_ids  = list({r["product_id"] for r in rows})
    variant_stock = {}

    for pid in product_ids:
        try:
            resp = _req.get(
                f"https://api.tiendanube.com/v1/{TN_STORE_ID}/products/{pid}",
                headers=headers, timeout=10,
            )
            if resp.ok:
                for var in resp.json().get("variants", []):
                    variant_stock[var["id"]] = var.get("stock")
        except Exception:
            pass

    for r in rows:
        r["current_stock"] = variant_stock.get(r["variant_id"])


# ── GET /notify/stats ──────────────────────────────────────────────────────────

@notify_bp.route("/notify/stats", methods=["GET"])
@cross_origin(origins="*")
def notify_stats():
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stock_notifications WHERE status = 'sent'")
            sent = int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM stock_notifications WHERE status = 'pending'")
            pending = int(cur.fetchone()[0])
            cur.execute("""
                SELECT COUNT(DISTINCT variant_id)
                FROM stock_notifications
                WHERE status = 'pending'
            """)
            productos_esperando = int(cur.fetchone()[0])
    finally:
        conn.close()

    return jsonify({
        "ok": True,
        "sent": sent,
        "pending": pending,
        "productos_esperando": productos_esperando,
    })


# ── POST /notify/send/<variant_id> ─────────────────────────────────────────────

@notify_bp.route("/notify/send/<int:variant_id>", methods=["POST"])
@cross_origin(origins="*")
def notify_send(variant_id):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, product_name, variant_name
                FROM stock_notifications
                WHERE variant_id = %s AND status = 'pending'
            """, (variant_id,))
            pendientes = cur.fetchall()
    finally:
        conn.close()

    if not pendientes:
        return jsonify({"ok": True, "enviados": 0, "msg": "Sin notificaciones pendientes"})

    errors   = []
    sent_ids = []
    for (notif_id, email, product_name, variant_name) in pendientes:
        try:
            _send_restock_email(email, product_name, variant_name)
            sent_ids.append(notif_id)
        except Exception as e:
            errors.append(f"{email}: {e}")

    if sent_ids:
        conn2 = _get_conn()
        try:
            with conn2.cursor() as cur:
                cur.execute("""
                    UPDATE stock_notifications
                    SET status = 'sent', sent_at = NOW()
                    WHERE id = ANY(%s)
                """, (sent_ids,))
            conn2.commit()
        finally:
            conn2.close()

    return jsonify({"ok": True, "enviados": len(sent_ids), "errores": errors})


# ── DELETE /notify/<notif_id> ──────────────────────────────────────────────────

@notify_bp.route("/notify/<int:notif_id>", methods=["DELETE"])
@cross_origin(origins="*")
def notify_delete(notif_id):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stock_notifications WHERE id = %s", (notif_id,))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True})


# ── Email ──────────────────────────────────────────────────────────────────────

def _send_restock_email(to_addr, product_name, variant_name):
    if not GMAIL_USER or not GMAIL_APP_PASS:
        raise RuntimeError("GMAIL_USER o GMAIL_APP_PASSWORD no configurados en Railway")

    nombre_completo = product_name
    if variant_name and variant_name != "-":
        nombre_completo += f" — {variant_name}"

    subject = f"¡{product_name} volvió a tener stock!"

    html = f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:32px 0">
    <tr><td align="center">
      <table width="520" cellpadding="0" cellspacing="0"
             style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08)">

        <tr>
          <td style="background:#1a1a1a;padding:20px 32px">
            <span style="color:#fff;font-size:20px;font-weight:bold;letter-spacing:.5px">
              Hechizo Bijou
            </span>
          </td>
        </tr>

        <tr>
          <td style="padding:32px">
            <p style="font-size:15px;color:#333;margin:0 0 16px">Hola,</p>
            <p style="font-size:15px;color:#333;margin:0 0 24px;line-height:1.6">
              Te escribimos porque pediste que te avisáramos cuando volviera a haber stock de
              <strong>{nombre_completo}</strong>. ¡Ya está disponible!
            </p>
            <div style="text-align:center;margin:28px 0">
              <a href="https://hechizobijou.com.ar"
                 style="display:inline-block;background:#1a1a1a;color:#fff;
                        padding:14px 36px;text-decoration:none;border-radius:4px;
                        font-size:14px;font-weight:bold;letter-spacing:.3px">
                Ver en la tienda
              </a>
            </div>
            <p style="font-size:13px;color:#999;margin:24px 0 0;line-height:1.6;border-top:1px solid #eee;padding-top:20px">
              Recibiste este mensaje porque lo solicitaste en
              <a href="https://hechizobijou.com.ar" style="color:#666">hechizobijou.com.ar</a>.
              <br>Si no lo solicitaste, podés ignorar este email.
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Hechizo Bijou <{GMAIL_USER}>"
    msg["To"]      = to_addr
    msg.attach(MIMEText(html, "html", "utf-8"))

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(GMAIL_USER, GMAIL_APP_PASS)
        smtp.sendmail(GMAIL_USER, to_addr, msg.as_bytes())
