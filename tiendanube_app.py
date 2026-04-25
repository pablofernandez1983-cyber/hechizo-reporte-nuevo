"""
tiendanube_app.py — OAuth 2 + registro de script para la app de Tiendanube.

Flujo:
  1. GET /tiendanube/install  → redirige a Tiendanube para autorizar
  2. GET /tiendanube/callback → intercambia code por token, guarda en DB,
                                registra widget-stock.js via Scripts API
"""

import os
import requests as http
from flask import Blueprint, jsonify, redirect, request

TN_CLIENT_ID     = os.environ.get("TN_APP_CLIENT_ID", "")
TN_CLIENT_SECRET = os.environ.get("TN_APP_CLIENT_SECRET", "")
WIDGET_URL       = "https://hechizo-reporte-nuevo-production.up.railway.app/widget-stock.js"
USER_AGENT       = "HechizoBijou-Stock/1.0 (hechizobijou@gmail.com)"

DATABASE_URL = os.environ.get("DATABASE_URL", "")

tn_bp = Blueprint("tiendanube", __name__)


# ── helpers DB ────────────────────────────────────────────────────────────────

def _save_token(store_id, access_token):
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tiendanube_tokens (store_id, access_token)
                VALUES (%s, %s)
                ON CONFLICT (store_id) DO UPDATE SET access_token = EXCLUDED.access_token
            """, (store_id, access_token))
        conn.commit()
    finally:
        conn.close()


# ── endpoints ─────────────────────────────────────────────────────────────────

@tn_bp.route("/tiendanube/install")
def install():
    """Inicia el flujo OAuth redirigiendo a Tiendanube."""
    url = f"https://www.tiendanube.com/apps/{TN_CLIENT_ID}/authorize?response_type=code"
    return redirect(url)


@tn_bp.route("/tiendanube/list-webhooks/<int:store_id>")
def list_webhooks(store_id):
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT access_token FROM tiendanube_tokens WHERE store_id = %s", (store_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
    resp = http.get(
        f"https://api.tiendanube.com/v1/{store_id}/webhooks",
        headers={"Authentication": f"bearer {row[0]}", "User-Agent": USER_AGENT},
        timeout=10,
    )
    return jsonify(resp.json())


@tn_bp.route("/tiendanube/test-token/<int:store_id>")
def test_token(store_id):
    """Prueba el token guardado para un store_id — devuelve nombre de la tienda."""
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT access_token FROM tiendanube_tokens WHERE store_id = %s", (store_id,))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify({"ok": False, "error": f"No hay token guardado para store_id={store_id}"}), 404

    token = row[0]
    resp  = http.get(
        f"https://api.tiendanube.com/v1/{store_id}/store",
        headers={"Authentication": f"bearer {token}", "User-Agent": USER_AGENT},
        timeout=10,
    )
    return jsonify({
        "ok":        resp.ok,
        "http_status": resp.status_code,
        "store":     resp.json() if resp.ok else resp.text[:300],
    })


@tn_bp.route("/tiendanube/callback")
def callback():
    """Recibe el code, lo intercambia por token y registra el script."""
    code = request.args.get("code")
    if not code:
        return jsonify({"error": "code faltante"}), 400

    # 1. Intercambiar code por access_token
    resp = http.post(
        "https://www.tiendanube.com/apps/authorize/token",
        json={
            "client_id":     TN_CLIENT_ID,
            "client_secret": TN_CLIENT_SECRET,
            "grant_type":    "authorization_code",
            "code":          code,
        },
        timeout=15,
    )
    if not resp.ok:
        return jsonify({"error": "token exchange failed", "detail": resp.text[:300]}), 502

    data         = resp.json()
    access_token = data["access_token"]
    store_id     = data["user_id"]

    # 2. Guardar token en DB
    _save_token(store_id, access_token)

    return jsonify({"ok": True, "store_id": store_id})


@tn_bp.route("/tiendanube/setup-webhook")
@tn_bp.route("/tiendanube/setup-webhook/<int:store_id>")
def setup_webhook(store_id=None):
    """
    Registra el webhook product/updated en Tiendanube (idempotente).
    Elimina duplicados si los hay, crea uno solo si no existe.
    GET /tiendanube/setup-webhook          → usa store de env var
    GET /tiendanube/setup-webhook/7549940  → usa token de DB para ese store
    """
    if store_id:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT access_token FROM tiendanube_tokens WHERE store_id = %s", (store_id,))
                row = cur.fetchone()
        finally:
            conn.close()
        if not row:
            return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
        TN_STORE_ID = str(store_id)
        TN_TOKEN    = row[0]
    else:
        TN_STORE_ID = os.environ.get("TIENDANUBE_STORE_ID", "")
        TN_TOKEN    = os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")

    if not TN_STORE_ID or not TN_TOKEN:
        return jsonify({"error": "TIENDANUBE_STORE_ID o TIENDANUBE_ACCESS_TOKEN no configurados"}), 500

    webhook_url = "https://hechizo-reporte-nuevo-production.up.railway.app/notify/webhook"
    tn_headers  = {
        "Authentication": f"bearer {TN_TOKEN}",
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
    }
    base = f"https://api.tiendanube.com/v1/{TN_STORE_ID}/webhooks"

    # Listar webhooks existentes
    existing = http.get(base, headers=tn_headers, timeout=15).json()
    matches  = [w for w in (existing if isinstance(existing, list) else [])
                if w.get("url") == webhook_url and w.get("event") == "product/updated"]

    deleted = 0
    kept_id = None
    for i, w in enumerate(matches):
        if i == 0:
            kept_id = w["id"]   # conservar el primero
        else:
            http.delete(f"{base}/{w['id']}", headers=tn_headers, timeout=15)
            deleted += 1

    if kept_id:
        return jsonify({"ok": True, "msg": f"Webhook ya existía (id={kept_id})", "duplicados_eliminados": deleted})

    # No existía → crear
    resp = http.post(base, headers=tn_headers,
                     json={"event": "product/updated", "url": webhook_url}, timeout=15)
    return jsonify({
        "ok":     resp.ok,
        "msg":    "Webhook creado",
        "detail": resp.json() if resp.content else {},
        "duplicados_eliminados": deleted,
    })
