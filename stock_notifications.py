"""
stock_notifications.py — Blueprint para notificaciones de stock.

Endpoints:
  POST   /notify                      — registra solicitud (widget tienda)
  GET    /notify/list                 — lista notificaciones con filtros
  GET    /notify/products             — ranking de productos con más demanda
  GET    /notify/stats                — estadísticas generales
  POST   /notify/send/<variant_id>    — envía mails y marca como enviado (manual)
  POST   /notify/check-stock          — chequea TN API y envía pendientes con stock > 0
  POST   /notify/webhook              — recibe webhooks product/updated de Tiendanube
  DELETE /notify/<notif_id>           — cancela una notificación (soft delete)
  POST   /notify/<notif_id>/restore   — restaura una notificación cancelada a pending
  DELETE /notify/variant/<variant_id> — cancela en bulk todas las pending de una variante
"""

import os
import hmac
import hashlib
import base64
import smtplib
import threading
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

TN_HEADERS = lambda: {
    "Authentication": f"bearer {os.environ.get('TIENDANUBE_ACCESS_TOKEN', '')}",
    "User-Agent": "HechizoBijou-Stock/1.0 (hechizobijou@gmail.com)",
}

notify_bp = Blueprint("notify", __name__)

REQUIRED_FIELDS = {"email", "product_id", "variant_id", "product_name", "variant_name"}


def _get_conn():
    import psycopg2
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def _ensure_columns():
    if not DATABASE_URL:
        return
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE stock_notifications ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE stock_notifications ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()")
            cur.execute("ALTER TABLE stock_notifications ADD COLUMN IF NOT EXISTS sku VARCHAR(100)")
        conn.commit()
        conn.close()
    except Exception:
        pass


threading.Thread(target=_ensure_columns, daemon=True).start()


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
    sku          = str(body.get("sku") or "")[:100] or None

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stock_notifications
                  (email, product_id, variant_id, store_id, product_name, variant_name, sku, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (email, variant_id)
                DO UPDATE SET
                  product_name = EXCLUDED.product_name,
                  variant_name = EXCLUDED.variant_name,
                  sku          = COALESCE(EXCLUDED.sku, stock_notifications.sku),
                  status       = 'pending',
                  sent_at      = NULL
            """, (email, product_id, variant_id, store_id, product_name, variant_name, sku))
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
                SELECT id, email, product_id, variant_id, product_name, variant_name, sku, status,
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
            if status_q in ("pending", "sent", "canceled"):
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

    product_ids   = list({r["product_id"] for r in rows})
    variant_stock = {}

    for pid in product_ids:
        try:
            resp = _req.get(
                f"https://api.tiendanube.com/v1/{TN_STORE_ID}/products/{pid}",
                headers=TN_HEADERS(), timeout=10,
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
            cur.execute("SELECT COUNT(DISTINCT variant_id) FROM stock_notifications WHERE status = 'pending'")
            productos_esperando = int(cur.fetchone()[0])
    finally:
        conn.close()

    return jsonify({
        "ok": True,
        "sent": sent,
        "pending": pending,
        "productos_esperando": productos_esperando,
    })


# ── POST /notify/send/<variant_id>  (manual) ──────────────────────────────────

@notify_bp.route("/notify/send/<int:variant_id>", methods=["POST", "OPTIONS"])
@cross_origin(origins="*", methods=["POST", "OPTIONS"], allow_headers=["Content-Type"])
def notify_send(variant_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200
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

    result = _send_and_mark(pendientes)
    return jsonify({"ok": True, **result})


# ── POST /notify/check-stock  (manual o cron) ─────────────────────────────────

@notify_bp.route("/notify/check-stock", methods=["POST", "OPTIONS"])
@cross_origin(origins="*", methods=["POST", "OPTIONS"], allow_headers=["Content-Type"])
def check_stock():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    import requests as _req

    TN_STORE_ID = os.environ.get("TIENDANUBE_STORE_ID", "")
    TN_TOKEN    = os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")
    if not TN_STORE_ID or not TN_TOKEN:
        return jsonify({"ok": False, "error": "Credenciales TN no configuradas"}), 500

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT variant_id, product_id
                FROM stock_notifications
                WHERE status = 'pending'
            """)
            pendientes = cur.fetchall()
    finally:
        conn.close()

    print(f"[CHECK-STOCK] pendientes={pendientes}", flush=True)

    if not pendientes:
        return jsonify({"ok": True, "enviados": 0, "msg": "Sin notificaciones pendientes"})

    product_ids = list({pid for _, pid in pendientes})

    # {product_id: {variant_id: stock}} — keyed by product for fallback when vid==pid
    tn_product_data = {}
    for pid in product_ids:
        try:
            resp = _req.get(
                f"https://api.tiendanube.com/v1/{TN_STORE_ID}/products/{pid}",
                headers=TN_HEADERS(), timeout=10,
            )
            print(f"[CHECK-STOCK] product_id={pid} status={resp.status_code}", flush=True)
            if resp.ok:
                tn_product_data[pid] = {
                    var["id"]: int(var.get("stock") or 0)
                    for var in resp.json().get("variants", [])
                    if var.get("stock") is not None
                }
        except Exception as e:
            print(f"[CHECK-STOCK] error pid={pid}: {e}", flush=True)

    print(f"[CHECK-STOCK] tn_product_data={tn_product_data}", flush=True)

    # Determinar qué variant_ids (de nuestra DB) deben recibir notificación
    to_notify = set()
    for pending_vid, pending_pid in pendientes:
        prod_variants = tn_product_data.get(pending_pid, {})
        if not prod_variants:
            continue
        if pending_vid == pending_pid:
            # El widget guardó product_id como variant_id (sin variantes detectadas)
            # Notificamos si cualquier variante del producto tiene stock > 0
            if any(s > 0 for s in prod_variants.values()):
                to_notify.add(pending_vid)
        else:
            if prod_variants.get(pending_vid, 0) > 0:
                to_notify.add(pending_vid)

    print(f"[CHECK-STOCK] to_notify={to_notify}", flush=True)

    if not to_notify:
        return jsonify({"ok": True, "enviados": 0, "msg": "Ninguna variante con stock > 0"})

    conn2 = _get_conn()
    try:
        with conn2.cursor() as cur:
            cur.execute("""
                SELECT id, email, product_name, variant_name
                FROM stock_notifications
                WHERE variant_id = ANY(%s) AND status = 'pending'
            """, (list(to_notify),))
            pendientes_mail = cur.fetchall()
    finally:
        conn2.close()

    result = _send_and_mark(pendientes_mail)
    print(f"[CHECK-STOCK] resultado={result}", flush=True)
    return jsonify({"ok": True, **result})


# ── POST /notify/webhook  (Tiendanube push) ───────────────────────────────────

@notify_bp.route("/notify/webhook", methods=["POST"])
def notify_webhook():
    """
    Recibe eventos product/updated de Tiendanube.
    TN envía: { "store_id": "...", "event": "product/updated", "id": <product_id> }
    """
    import requests as _req

    body = request.get_json(silent=True) or {}
    event      = body.get("event", "")
    product_id = body.get("id") or body.get("product_id")
    print(f"[WEBHOOK] event={event!r} product_id={product_id} body={body}", flush=True)

    if event != "product/updated":
        return jsonify({"ok": True, "msg": f"event ignored: {event}"}), 200

    if not product_id:
        print("[WEBHOOK] sin product_id, ignorando", flush=True)
        return jsonify({"ok": True}), 200

    product_id = int(product_id)

    # Chequear si hay pendientes para este producto antes de llamar a TN
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT variant_id
                FROM stock_notifications
                WHERE product_id = %s AND status = 'pending'
            """, (product_id,))
            variant_ids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    print(f"[WEBHOOK] product_id={product_id} variant_ids_pendientes={variant_ids}", flush=True)

    if not variant_ids:
        return jsonify({"ok": True, "msg": "no pending for this product"}), 200

    # Fetch stock actualizado desde TN
    TN_STORE_ID = os.environ.get("TIENDANUBE_STORE_ID", "")
    TN_TOKEN    = os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")
    if not TN_STORE_ID or not TN_TOKEN:
        return jsonify({"ok": False, "error": "credenciales TN no configuradas"}), 500

    try:
        resp = _req.get(
            f"https://api.tiendanube.com/v1/{TN_STORE_ID}/products/{product_id}",
            headers=TN_HEADERS(), timeout=10,
        )
        if not resp.ok:
            print(f"[WEBHOOK] TN API error {resp.status_code}", flush=True)
            return jsonify({"ok": False, "error": f"TN {resp.status_code}"}), 502
        product = resp.json()
    except Exception as e:
        print(f"[WEBHOOK] excepcion TN API: {e}", flush=True)
        return jsonify({"ok": False, "error": str(e)}), 500

    prod_variants = {
        var["id"]: int(var.get("stock") or 0)
        for var in product.get("variants", [])
        if var.get("stock") is not None
    }
    print(f"[WEBHOOK] prod_variants={prod_variants}", flush=True)

    # Fallback: si variant_id == product_id el widget no detectó la variante real
    to_notify = set()
    for vid in variant_ids:
        if vid == product_id:
            if any(s > 0 for s in prod_variants.values()):
                to_notify.add(vid)
        else:
            if prod_variants.get(vid, 0) > 0:
                to_notify.add(vid)

    print(f"[WEBHOOK] to_notify={to_notify}", flush=True)

    if not to_notify:
        return jsonify({"ok": True, "msg": "sin stock en variantes pendientes"}), 200

    conn2 = _get_conn()
    try:
        with conn2.cursor() as cur:
            cur.execute("""
                SELECT id, email, product_name, variant_name
                FROM stock_notifications
                WHERE variant_id = ANY(%s) AND status = 'pending'
            """, (list(to_notify),))
            pendientes_mail = cur.fetchall()
    finally:
        conn2.close()

    result = _send_and_mark(pendientes_mail)
    print(f"[WEBHOOK] resultado={result}", flush=True)
    return jsonify({"ok": True, **result}), 200


# ── DELETE /notify/<notif_id>  (soft cancel) ──────────────────────────────────

@notify_bp.route("/notify/<int:notif_id>", methods=["DELETE", "OPTIONS"])
@cross_origin(origins="*", methods=["DELETE", "OPTIONS"], allow_headers=["Content-Type"])
def notify_delete(notif_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stock_notifications SET status = 'canceled' WHERE id = %s",
                (notif_id,),
            )
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True})


# ── POST /notify/<notif_id>/restore ───────────────────────────────────────────

@notify_bp.route("/notify/<int:notif_id>/restore", methods=["POST", "OPTIONS"])
@cross_origin(origins="*", methods=["POST", "OPTIONS"], allow_headers=["Content-Type"])
def notify_restore(notif_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stock_notifications SET status = 'pending', sent_at = NULL WHERE id = %s AND status = 'canceled'",
                (notif_id,),
            )
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True})


# ── DELETE /notify/variant/<variant_id>  (bulk discard) ───────────────────────

@notify_bp.route("/notify/variant/<int:variant_id>", methods=["DELETE", "OPTIONS"])
@cross_origin(origins="*", methods=["DELETE", "OPTIONS"], allow_headers=["Content-Type"])
def notify_delete_variant(variant_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stock_notifications SET status = 'canceled' WHERE variant_id = %s AND status = 'pending'",
                (variant_id,),
            )
            canceled = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "eliminados": canceled})


# ── Helpers internos ───────────────────────────────────────────────────────────

def _dispatch_by_stock(variant_stock: dict) -> dict:
    """
    Dado {variant_id: stock_actual}, busca pendientes para las variantes
    con stock > 0 y manda los mails.
    """
    with_stock = [vid for vid, s in variant_stock.items() if s > 0]
    if not with_stock:
        return {"enviados": 0, "msg": "Ninguna variante con stock > 0"}

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, product_name, variant_name
                FROM stock_notifications
                WHERE variant_id = ANY(%s) AND status = 'pending'
            """, (with_stock,))
            pendientes = cur.fetchall()
    finally:
        conn.close()

    if not pendientes:
        return {"enviados": 0, "msg": "Sin pendientes para las variantes con stock"}

    return _send_and_mark(pendientes)


def _send_and_mark(pendientes: list) -> dict:
    """Envía mails y marca como enviados. Devuelve {"enviados": N, "errores": [...]}."""
    errors   = []
    sent_ids = []

    for (notif_id, email, product_name, variant_name) in pendientes:
        try:
            _send_restock_email(email, product_name, variant_name)
            sent_ids.append(notif_id)
        except Exception as e:
            errors.append(f"{email}: {e}")

    if sent_ids:
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE stock_notifications
                    SET status = 'sent', sent_at = NOW()
                    WHERE id = ANY(%s)
                """, (sent_ids,))
            conn.commit()
        finally:
            conn.close()

    return {"enviados": len(sent_ids), "errores": errors}


# ── Email ──────────────────────────────────────────────────────────────────────

def _send_restock_email(to_addr, product_name, variant_name):
    if not GMAIL_USER or not GMAIL_APP_PASS:
        raise RuntimeError("GMAIL_USER o GMAIL_APP_PASSWORD no configurados en Railway")

    nombre_completo = product_name
    if variant_name and variant_name not in ("-", ""):
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
            <span style="color:#fff;font-size:20px;font-weight:bold;letter-spacing:.5px">Hechizo Bijou</span>
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

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(GMAIL_USER, GMAIL_APP_PASS)
        smtp.sendmail(GMAIL_USER, to_addr, msg.as_bytes())
