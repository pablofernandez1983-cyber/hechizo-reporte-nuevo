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
import re
import threading
from flask import Blueprint, jsonify, request
from flask_cors import cross_origin

from auth_clave import exigir_clave

DATABASE_URL = os.environ.get("DATABASE_URL", "")

WIDGET_ORIGINS = [
    "https://hechizo.com.ar",
    "https://hechizobijou.com.ar",
    "https://hechizobijou.mitiendanube.com",
    "https://pruebasdehechizo.mitiendanube.com",
]

def _tn_token(store_id):
    """Devuelve el access token para el store_id dado.
    Busca primero en la tabla tiendanube_tokens (DB), luego en env var TIENDANUBE_ACCESS_TOKEN.
    """
    if store_id and DATABASE_URL:
        try:
            conn = _get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT access_token FROM tiendanube_tokens WHERE store_id = %s",
                    (int(store_id),),
                )
                row = cur.fetchone()
            conn.close()
            if row:
                return row[0]
        except Exception:
            pass
    return os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")

def _tn_headers(store_id=None):
    token = _tn_token(store_id) if store_id else os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")
    return {
        "Authentication": f"bearer {token}",
        "User-Agent": "HechizoBijou-Stock/1.0 (hechizobijou@gmail.com)",
    }

TN_HEADERS = lambda: _tn_headers()

notify_bp = Blueprint("notify", __name__)

# Rutas que quedan abiertas: el alta desde el widget de la tienda y el webhook de TN.
# Todo lo demás (listar mails, borrar, enviar) exige la misma clave que /web/*.
exigir_clave(notify_bp, publicas={"notify.notify", "notify.notify_webhook"})

REQUIRED_FIELDS = {"email", "product_id", "variant_id", "product_name", "variant_name"}
RX_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$")


def variantes_a_notificar(pendientes, prod_variants):
    """Variantes pendientes a las que hay que avisar: las que ahora tienen stock.
    Si la variante ya no existe en el producto (Tiendanube le cambió el ID, o el producto
    dejó de tener variantes), se avisa si cualquier variante del producto tiene stock.
    Una variante que existe y sigue en 0 NO se avisa aunque otra del producto tenga stock."""
    hay_stock = max(prod_variants.values(), default=0) > 0
    out = set()
    for vid in pendientes:
        if vid in prod_variants:
            if prod_variants[vid] > 0:
                out.add(vid)
        elif hay_stock:
            out.add(vid)
    return out


_URL_HOSTS = {h.split("://", 1)[1] for h in WIDGET_ORIGINS}
_URL_HOSTS |= {"www." + h for h in _URL_HOSTS}


def _url_tienda(url):
    """Devuelve la URL solo si es https y apunta a la tienda; si no, None.
    (El link termina en un mail que sale de la cuenta de Hechizo: nunca a otro sitio.)"""
    from urllib.parse import urlsplit
    try:
        u = urlsplit(str(url or "").strip())
    except ValueError:
        return None
    if u.scheme == "https" and u.hostname in _URL_HOSTS and not u.username and not u.port:
        return u.geturl()
    return None


def _get_conn():
    import psycopg2
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


_STORE_DOMAINS = {
    1384618: "hechizo.com.ar",
    7549940: "pruebasdehechizo.mitiendanube.com",
}

def _product_url(store_id, handle):
    from urllib.parse import quote
    domain = _STORE_DOMAINS.get(int(store_id or 0))
    if not domain or not handle:
        return None
    # TN sometimes returns handle as {"es": "slug"} localized object
    if isinstance(handle, dict):
        handle = handle.get("es") or next(iter(handle.values()), "")
    handle = quote(str(handle).strip(), safe="-_.")
    if not handle:
        return None
    return f"https://{domain}/productos/{handle}"


def _fill_product_url(product_id, url):
    """Rellena product_url NULL en registros pendientes de ese producto."""
    if not url:
        return
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE stock_notifications
                SET product_url = %s
                WHERE product_id = %s AND status = 'pending' AND product_url IS NULL
            """, (url, product_id))
        conn.commit()
        conn.close()
    except Exception:
        pass


def _ensure_columns():
    if not DATABASE_URL:
        return
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE stock_notifications ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ")
                cur.execute("ALTER TABLE stock_notifications ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()")
                cur.execute("ALTER TABLE stock_notifications ADD COLUMN IF NOT EXISTS sku VARCHAR(100)")
                cur.execute("ALTER TABLE stock_notifications ADD COLUMN IF NOT EXISTS product_url VARCHAR(500)")
            conn.commit()
        finally:
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

    email = str(body["email"]).strip().lower()
    if not RX_EMAIL.match(email):
        return jsonify({"error": "email inválido"}), 400
    try:
        product_id = int(body["product_id"])
        variant_id = int(body["variant_id"])
        store_id   = int(body.get("store_id") or 1384618)
    except (TypeError, ValueError):
        return jsonify({"error": "producto inválido"}), 400
    product_name = str(body["product_name"])[:255]
    variant_name = str(body["variant_name"])[:255]
    sku          = str(body.get("sku") or "")[:100] or None
    product_url  = _url_tienda(str(body.get("product_url") or "")[:500])

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stock_notifications
                  (email, product_id, variant_id, store_id, product_name, variant_name, sku, product_url, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (email, variant_id)
                DO UPDATE SET
                  product_name = EXCLUDED.product_name,
                  variant_name = EXCLUDED.variant_name,
                  sku          = COALESCE(EXCLUDED.sku, stock_notifications.sku),
                  product_url  = COALESCE(EXCLUDED.product_url, stock_notifications.product_url),
                  status       = 'pending',
                  sent_at      = NULL
            """, (email, product_id, variant_id, store_id, product_name, variant_name, sku, product_url))
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
                SELECT product_id, product_name,
                       COUNT(*) AS cantidad,
                       array_agg(DISTINCT variant_id) AS variant_ids
                FROM stock_notifications
                WHERE status = 'pending'
                GROUP BY product_id, product_name
                ORDER BY cantidad DESC
            """)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()

    _enrich_with_stock_product(rows)
    for r in rows:
        r["cantidad"]    = int(r["cantidad"])
        r["variant_ids"] = list(r["variant_ids"])

    return jsonify({"ok": True, "items": rows})


def _enrich_with_stock_product(rows):
    """Enriquece filas agrupadas por product_id con stock mínimo de sus variantes."""
    import requests as _req
    if not rows:
        return
    product_ids = list({r["product_id"] for r in rows})
    product_stock = {}  # product_id -> min stock across variants

    store_id = os.environ.get("TIENDANUBE_STORE_ID", "")
    headers  = _tn_headers(store_id) if store_id else None

    def _stock_de(pid):
        if not store_id:
            return None
        try:
            resp = _req.get(
                f"https://api.tiendanube.com/v1/{store_id}/products/{pid}",
                headers=headers, timeout=10,
            )
            if resp.ok:
                stocks = [v.get("stock") for v in resp.json().get("variants", [])]
                stocks = [s for s in stocks if s is not None]
                return min(stocks) if stocks else None
        except Exception:
            pass
        return None

    # En paralelo (antes era uno por uno y tardaba ~25 s con 18 productos)
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=6) as ex:
        for pid, stock in zip(product_ids, ex.map(_stock_de, product_ids)):
            product_stock[pid] = stock

    for r in rows:
        r["current_stock"] = product_stock.get(r["product_id"])


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
@cross_origin(origins="*", methods=["POST", "OPTIONS"], allow_headers=["Content-Type", "X-Web-Key"])
def notify_send(variant_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, product_name, variant_name, product_url
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
@cross_origin(origins="*", methods=["POST", "OPTIONS"], allow_headers=["Content-Type", "X-Web-Key"])
def check_stock():
    if request.method == "OPTIONS":
        return jsonify({}), 200
    import requests as _req

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT variant_id, product_id, store_id
                FROM stock_notifications
                WHERE status = 'pending'
            """)
            pendientes = cur.fetchall()
    finally:
        conn.close()

    print(f"[CHECK-STOCK] pendientes={pendientes}", flush=True)

    if not pendientes:
        return jsonify({"ok": True, "enviados": 0, "msg": "Sin notificaciones pendientes"})

    # Agrupar por store_id para usar el token correcto por tienda
    from collections import defaultdict as _dd
    by_store = _dd(list)
    for vid, pid, sid in pendientes:
        by_store[sid].append((vid, pid))

    not_found_pids = []
    tn_product_data = {}   # {product_id: {variant_id: stock}}

    for sid, items in by_store.items():
        product_ids = list({pid for _, pid in items})
        for pid in product_ids:
            try:
                resp = _req.get(
                    f"https://api.tiendanube.com/v1/{sid}/products/{pid}",
                    headers=_tn_headers(sid), timeout=10,
                )
                print(f"[CHECK-STOCK] store={sid} product_id={pid} status={resp.status_code}", flush=True)
                if resp.status_code == 404:
                    not_found_pids.append(pid)
                elif resp.ok:
                    pdata = resp.json()
                    variant_map = {}
                    for var in pdata.get("variants", []):
                        s = var.get("stock")
                        variant_map[var["id"]] = 999 if s is None else int(s)
                    tn_product_data[pid] = variant_map
                    _fill_product_url(pid, _product_url(sid, pdata.get("handle")))
                    print(f"[CHECK-STOCK] product_id={pid} variants={variant_map}", flush=True)
            except Exception as e:
                print(f"[CHECK-STOCK] error pid={pid}: {e}", flush=True)

    # pendientes sin store_id dimension para el loop siguiente
    pendientes = [(vid, pid) for vid, pid, _ in pendientes]

    # Limpiar productos que ya no existen en TN
    if not_found_pids:
        try:
            conn_clean = _get_conn()
            with conn_clean.cursor() as cur:
                cur.execute(
                    "UPDATE stock_notifications SET status = 'canceled' WHERE product_id = ANY(%s) AND status = 'pending'",
                    (not_found_pids,),
                )
                cleaned = cur.rowcount
            conn_clean.commit()
            conn_clean.close()
            print(f"[CHECK-STOCK] canceladas {cleaned} notificaciones de productos eliminados: {not_found_pids}", flush=True)
        except Exception as e:
            print(f"[CHECK-STOCK] error limpiando 404s: {e}", flush=True)

    # Determinar qué variant_ids deben recibir notificación
    to_notify  = set()
    stock_debug = {}
    for pending_vid, pending_pid in pendientes:
        prod_variants = tn_product_data.get(pending_pid, {})
        if not prod_variants:
            continue
        stock_debug[pending_vid] = prod_variants.get(pending_vid, "variante inexistente")
        to_notify |= variantes_a_notificar([pending_vid], prod_variants)

    print(f"[CHECK-STOCK] stock_debug={stock_debug} to_notify={to_notify}", flush=True)

    if not to_notify:
        return jsonify({
            "ok": True, "enviados": 0,
            "msg": "Ninguna variante con stock > 0",
            "stock_encontrado": stock_debug,
        })

    conn2 = _get_conn()
    try:
        with conn2.cursor() as cur:
            cur.execute("""
                SELECT id, email, product_name, variant_name, product_url
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

def _firma_webhook_valida():
    """True si el webhook viene firmado por Tiendanube con el secreto de la app de stock
    (header X-Linkedstore-Hmac-Sha256 = HMAC-SHA256 del cuerpo)."""
    import hashlib, hmac
    secreto = os.environ.get("TN_APP_CLIENT_SECRET", "")
    recibida = request.headers.get("X-Linkedstore-Hmac-Sha256", "")
    if not secreto or not recibida:
        return False
    esperada = hmac.new(secreto.encode(), request.get_data(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(recibida.encode(), esperada.encode())


@notify_bp.route("/notify/webhook", methods=["POST"])
def notify_webhook():
    """
    Recibe eventos product/updated de Tiendanube.
    TN envía: { "store_id": "...", "event": "product/updated", "id": <product_id> }
    """
    import requests as _req

    if not _firma_webhook_valida():
        # Se ignora sin procesar. Responde 200 para que Tiendanube no lo reintente durante
        # días (el webhook viejo que apunta al servicio del reporte llega con otra firma).
        print("[WEBHOOK] firma inválida: ignorado", flush=True)
        return jsonify({"ok": True, "msg": "ignorado"}), 200
    body = request.get_json(silent=True) or {}
    event      = body.get("event", "")
    product_id = body.get("id") or body.get("product_id")
    store_id   = int(body.get("store_id") or 0)
    print(f"[WEBHOOK] event={event!r} product_id={product_id} store_id={store_id} body={body}", flush=True)

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

    # Usar store_id del payload para llamar al API correcto
    TN_STORE_ID = store_id or os.environ.get("TIENDANUBE_STORE_ID", "")
    TN_TOKEN    = _tn_token(TN_STORE_ID)
    if not TN_STORE_ID or not TN_TOKEN:
        return jsonify({"ok": False, "error": "credenciales TN no configuradas"}), 500

    try:
        resp = _req.get(
            f"https://api.tiendanube.com/v1/{TN_STORE_ID}/products/{product_id}",
            headers=_tn_headers(TN_STORE_ID), timeout=10,
        )
        if not resp.ok:
            print(f"[WEBHOOK] TN API error {resp.status_code}", flush=True)
            return jsonify({"ok": False, "error": f"TN {resp.status_code}"}), 502
        product = resp.json()
    except Exception as e:
        print(f"[WEBHOOK] excepcion TN API: {e}", flush=True)
        return jsonify({"ok": False, "error": str(e)}), 500

    # Rellenar product_url en registros que no la tienen
    handle = product.get("handle")
    _fill_product_url(product_id, _product_url(store_id, handle))

    # stock=None en TN = sin gestión de stock = siempre disponible → 999
    prod_variants = {
        var["id"]: (999 if var.get("stock") is None else int(var.get("stock") or 0))
        for var in product.get("variants", [])
    }
    print(f"[WEBHOOK] prod_variants={prod_variants}", flush=True)

    to_notify = variantes_a_notificar(variant_ids, prod_variants)

    print(f"[WEBHOOK] to_notify={to_notify}", flush=True)

    if not to_notify:
        return jsonify({"ok": True, "msg": "sin stock en variantes pendientes"}), 200

    conn2 = _get_conn()
    try:
        with conn2.cursor() as cur:
            cur.execute("""
                SELECT id, email, product_name, variant_name, product_url
                FROM stock_notifications
                WHERE variant_id = ANY(%s) AND status = 'pending'
            """, (list(to_notify),))
            pendientes_mail = cur.fetchall()
    finally:
        conn2.close()

    result = _send_and_mark(pendientes_mail)
    print(f"[WEBHOOK] resultado={result}", flush=True)
    return jsonify({"ok": True, **result}), 200


# ── POST /notify/send-product/<product_id>  (manual por producto) ────────────

@notify_bp.route("/notify/send-product/<int:product_id>", methods=["POST", "OPTIONS"])
@cross_origin(origins="*", methods=["POST", "OPTIONS"], allow_headers=["Content-Type", "X-Web-Key"])
def notify_send_product(product_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, product_name, variant_name, product_url
                FROM stock_notifications
                WHERE product_id = %s AND status = 'pending'
            """, (product_id,))
            pendientes = cur.fetchall()
    finally:
        conn.close()
    if not pendientes:
        return jsonify({"ok": True, "enviados": 0, "msg": "Sin notificaciones pendientes"})
    result = _send_and_mark(pendientes)
    return jsonify({"ok": True, **result})


# ── DELETE /notify/product/<product_id>  (bulk discard por producto) ──────────

@notify_bp.route("/notify/product/<int:product_id>", methods=["DELETE", "OPTIONS"])
@cross_origin(origins="*", methods=["DELETE", "OPTIONS"], allow_headers=["Content-Type", "X-Web-Key"])
def notify_delete_product(product_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE stock_notifications SET status = 'canceled' WHERE product_id = %s AND status = 'pending'",
                (product_id,),
            )
            canceled = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "eliminados": canceled})


# ── DELETE /notify/<notif_id>  (soft cancel) ──────────────────────────────────

@notify_bp.route("/notify/<int:notif_id>", methods=["DELETE", "OPTIONS"])
@cross_origin(origins="*", methods=["DELETE", "OPTIONS"], allow_headers=["Content-Type", "X-Web-Key"])
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
@cross_origin(origins="*", methods=["POST", "OPTIONS"], allow_headers=["Content-Type", "X-Web-Key"])
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
@cross_origin(origins="*", methods=["DELETE", "OPTIONS"], allow_headers=["Content-Type", "X-Web-Key"])
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

def _send_and_mark(pendientes: list) -> dict:
    """Envía mails y marca como enviados. Devuelve {"enviados": N, "errores": [...]}."""
    errors   = []
    sent_ids = []

    for row in pendientes:
        notif_id, email, product_name, variant_name = row[0], row[1], row[2], row[3]
        product_url = row[4] if len(row) > 4 else None
        try:
            _send_restock_email(email, product_name, variant_name, product_url)
            sent_ids.append(notif_id)
        except Exception as e:
            print(f"[MAIL ERROR] {email}: {e}", flush=True)
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

def _send_restock_email(to_addr, product_name, variant_name, product_url=None):
    import gmail_envio
    from html import escape as _hesc
    product_name = " ".join(str(product_name or "").split())  # sin saltos de línea (va en el asunto)
    nombre_completo = product_name
    if variant_name and variant_name not in ("-", ""):
        nombre_completo += f" — {variant_name}"
    nombre_completo = _hesc(nombre_completo)  # el nombre viene del navegador: escapar antes de meterlo en el HTML

    link     = _url_tienda(product_url) or "https://hechizo.com.ar"
    link_safe = _hesc(link)
    subject  = f"¡{product_name} volvió a tener stock!"

    html = f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:32px 0">
    <tr><td align="center">
      <table width="520" cellpadding="0" cellspacing="0"
             style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08)">
        <tr>
          <td style="background:linear-gradient(135deg,#2d0a1a 0%,#1a0510 100%);padding:24px 32px;text-align:center">
            <span style="font-family:Georgia,'Times New Roman',serif;color:#f0a0b8;font-size:26px;font-weight:normal;letter-spacing:3px;text-transform:uppercase">Hechizo</span>
            <div style="width:40px;height:1px;background:linear-gradient(90deg,transparent,#e879a0,transparent);margin:8px auto 0"></div>
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
              <a href="{link_safe}"
                 style="display:inline-block;background:#1a1a1a;color:#fff;
                        padding:14px 36px;text-decoration:none;border-radius:4px;
                        font-size:14px;font-weight:bold;letter-spacing:.3px">
                Ver producto
              </a>
            </div>
            <p style="font-size:13px;color:#999;margin:24px 0 0;line-height:1.6;border-top:1px solid #eee;padding-top:20px">
              Recibiste este mensaje porque lo solicitaste en
              <a href="https://hechizo.com.ar" style="color:#e879a0">hechizo.com.ar</a>.
              <br>Si no lo solicitaste, podés ignorar este email.
            </p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    gmail_envio.enviar(to_addr, subject, html)


# ── GET /notify/test-gmail  (diagnóstico) ─────────────────────────────────────

@notify_bp.route("/notify/test-gmail", methods=["GET"])
@cross_origin(origins="*")
def notify_test_gmail():
    try:
        import gmail_envio
        token = gmail_envio.access_token()
        return jsonify({"ok": True, "msg": "Gmail OAuth OK", "token_len": len(token)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
