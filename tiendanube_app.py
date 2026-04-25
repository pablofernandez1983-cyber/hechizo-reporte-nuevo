"""
tiendanube_app.py — OAuth 2 + registro de scripts para la app de Tiendanube.

Flujo:
  1. GET /tiendanube/install      → redirige a Tiendanube para autorizar
  2. GET /tiendanube/callback     → intercambia code por token, guarda en DB,
                                    registra widget-stock.js y widget-ruleta.js
  3. GET /tiendanube/setup-all/<store_id>  → re-registra ambos widgets (idempotente)
"""

import os
import requests as http
from flask import Blueprint, jsonify, redirect, request

TN_CLIENT_ID     = os.environ.get("TN_APP_CLIENT_ID", "")
TN_CLIENT_SECRET = os.environ.get("TN_APP_CLIENT_SECRET", "")

BASE_URL          = "https://hechizo-reporte-nuevo-production.up.railway.app"
WIDGET_STOCK_URL  = f"{BASE_URL}/widget-stock.js"
WIDGET_RULETA_URL = f"{BASE_URL}/widget-ruleta.js"

USER_AGENT   = "HechizoBijou/1.0 (hechizobijou@gmail.com)"
DATABASE_URL = os.environ.get("DATABASE_URL", "")

tn_bp = Blueprint("tiendanube", __name__)


# ── helpers DB ────────────────────────────────────────────────────────────────

def _get_token(store_id):
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT access_token FROM tiendanube_tokens WHERE store_id = %s", (store_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    return row[0] if row else None


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


# ── helpers API TN ────────────────────────────────────────────────────────────

def _tn_headers(token):
    return {
        "Authentication": f"bearer {token}",
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
    }


def _register_script(store_id, token, widget_url):
    """Registra un script tag en el store (idempotente). Devuelve dict con resultado."""
    base     = f"https://api.tiendanube.com/v1/{store_id}/scripts"
    headers  = _tn_headers(token)
    existing = http.get(base, headers=headers, timeout=10).json()
    matches  = [s for s in (existing if isinstance(existing, list) else [])
                if s.get("src") == widget_url]

    if matches:
        return {"ok": True, "msg": f"Script ya registrado (id={matches[0]['id']})", "created": False}

    resp = http.post(base, headers=headers, json={
        "src":   widget_url,
        "event": "onload",
        "where": "store",
    }, timeout=10)
    return {"ok": resp.ok, "msg": "Script registrado", "created": True,
            "detail": resp.json() if resp.content else {}}


# ── endpoints ─────────────────────────────────────────────────────────────────

@tn_bp.route("/tiendanube/install")
def install():
    url = f"https://www.tiendanube.com/apps/{TN_CLIENT_ID}/authorize?response_type=code"
    return redirect(url)


@tn_bp.route("/tiendanube/callback")
def callback():
    """Recibe el code, intercambia por token y registra ambos scripts automáticamente."""
    code = request.args.get("code")
    if not code:
        return jsonify({"error": "code faltante"}), 400

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

    _save_token(store_id, access_token)

    stock  = _register_script(store_id, access_token, WIDGET_STOCK_URL)
    ruleta = _register_script(store_id, access_token, WIDGET_RULETA_URL)

    return jsonify({"ok": True, "store_id": store_id, "stock": stock, "ruleta": ruleta})


@tn_bp.route("/tiendanube/setup-all/<int:store_id>")
def setup_all(store_id):
    """Re-registra ambos widgets en el store (idempotente). Útil para forzar re-instalación."""
    token = _get_token(store_id)
    if not token:
        return jsonify({"error": f"No hay token para store_id={store_id}"}), 404

    stock  = _register_script(store_id, token, WIDGET_STOCK_URL)
    ruleta = _register_script(store_id, token, WIDGET_RULETA_URL)
    return jsonify({"ok": True, "stock": stock, "ruleta": ruleta})


@tn_bp.route("/tiendanube/setup-script/<int:store_id>")
def setup_script(store_id):
    token = _get_token(store_id)
    if not token:
        return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
    return jsonify(_register_script(store_id, token, WIDGET_STOCK_URL))


@tn_bp.route("/tiendanube/setup-script-ruleta/<int:store_id>")
def setup_script_ruleta(store_id):
    token = _get_token(store_id)
    if not token:
        return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
    return jsonify(_register_script(store_id, token, WIDGET_RULETA_URL))


@tn_bp.route("/tiendanube/list-scripts/<int:store_id>")
def list_scripts(store_id):
    token = _get_token(store_id)
    if not token:
        return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
    resp = http.get(
        f"https://api.tiendanube.com/v1/{store_id}/scripts",
        headers=_tn_headers(token),
        timeout=10,
    )
    return jsonify(resp.json())


@tn_bp.route("/tiendanube/list-webhooks/<int:store_id>")
def list_webhooks(store_id):
    token = _get_token(store_id)
    if not token:
        return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
    resp = http.get(
        f"https://api.tiendanube.com/v1/{store_id}/webhooks",
        headers=_tn_headers(token),
        timeout=10,
    )
    return jsonify(resp.json())


@tn_bp.route("/tiendanube/test-token/<int:store_id>")
def test_token(store_id):
    token = _get_token(store_id)
    if not token:
        return jsonify({"ok": False, "error": f"No hay token guardado para store_id={store_id}"}), 404
    resp = http.get(
        f"https://api.tiendanube.com/v1/{store_id}/store",
        headers=_tn_headers(token),
        timeout=10,
    )
    return jsonify({
        "ok":          resp.ok,
        "http_status": resp.status_code,
        "store":       resp.json() if resp.ok else resp.text[:300],
    })


@tn_bp.route("/tiendanube/setup-webhook")
@tn_bp.route("/tiendanube/setup-webhook/<int:store_id>")
def setup_webhook(store_id=None):
    """
    Registra el webhook product/updated en Tiendanube (idempotente).
    GET /tiendanube/setup-webhook/<store_id>  → usa token de DB
    GET /tiendanube/setup-webhook             → usa env vars (legacy)
    """
    if store_id:
        TN_TOKEN    = _get_token(store_id)
        TN_STORE_ID = str(store_id)
        if not TN_TOKEN:
            return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
    else:
        TN_STORE_ID = os.environ.get("TIENDANUBE_STORE_ID", "")
        TN_TOKEN    = os.environ.get("TIENDANUBE_ACCESS_TOKEN", "")

    if not TN_STORE_ID or not TN_TOKEN:
        return jsonify({"error": "TIENDANUBE_STORE_ID o TIENDANUBE_ACCESS_TOKEN no configurados"}), 500

    webhook_url = f"{BASE_URL}/notify/webhook"
    headers     = _tn_headers(TN_TOKEN)
    base        = f"https://api.tiendanube.com/v1/{TN_STORE_ID}/webhooks"

    existing = http.get(base, headers=headers, timeout=15).json()
    matches  = [w for w in (existing if isinstance(existing, list) else [])
                if w.get("url") == webhook_url and w.get("event") == "product/updated"]

    deleted = 0
    kept_id = None
    for i, w in enumerate(matches):
        if i == 0:
            kept_id = w["id"]
        else:
            http.delete(f"{base}/{w['id']}", headers=headers, timeout=15)
            deleted += 1

    if kept_id:
        return jsonify({"ok": True, "msg": f"Webhook ya existía (id={kept_id})", "duplicados_eliminados": deleted})

    resp = http.post(base, headers=headers,
                     json={"event": "product/updated", "url": webhook_url}, timeout=15)
    return jsonify({
        "ok":                  resp.ok,
        "msg":                 "Webhook creado",
        "detail":              resp.json() if resp.content else {},
        "duplicados_eliminados": deleted,
    })
