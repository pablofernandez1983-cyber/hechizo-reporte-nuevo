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

    # 3. Registrar widget-stock.js via Scripts API
    headers = {
        "Authentication": f"bearer {access_token}",
        "User-Agent":     USER_AGENT,
        "Content-Type":   "application/json",
    }
    script_resp = http.post(
        f"https://api.tiendanube.com/2025-03/{store_id}/scripts",
        json={
            "script_id":    6218,
            "query_params": f'{{"store_id": {store_id}}}',
        },
        headers=headers,
        timeout=15,
    )

    if script_resp.ok:
        return jsonify({"ok": True, "store_id": store_id, "script": script_resp.json()})
    else:
        # Token guardado igual — el script se puede reintentar
        return jsonify({
            "ok": False,
            "store_id": store_id,
            "warning": "token guardado pero script no registrado",
            "detail": script_resp.text[:300],
        }), 207
