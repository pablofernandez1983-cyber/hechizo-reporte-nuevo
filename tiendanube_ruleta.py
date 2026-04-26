"""
tiendanube_ruleta.py — OAuth 2 + registro de script para la app Ruleta (independiente).

App separada de la app de Stock. Tiene su propio client_id, su propia tabla
de tokens y su propio ciclo de instalación/desinstalación en Tiendanube.

Flujo:
  1. GET /ruleta/install    → redirige a Tiendanube para autorizar
  2. GET /ruleta/callback   → intercambia code por token, guarda en DB,
                              registra widget-ruleta.js via Scripts API
"""

import os
import requests as http
from flask import Blueprint, jsonify, redirect, request

TN_CLIENT_ID     = os.environ.get("TN_RULETA_CLIENT_ID", "")
TN_CLIENT_SECRET = os.environ.get("TN_RULETA_CLIENT_SECRET", "")

SCRIPT_ID = 6238  # ID del script registrado en el portal de Partners

USER_AGENT   = "HechizoBijou-Ruleta/1.0 (hechizobijou@gmail.com)"
DATABASE_URL = os.environ.get("DATABASE_URL", "")

ruleta_bp = Blueprint("tiendanube_ruleta", __name__)


# ── helpers DB ────────────────────────────────────────────────────────────────

def _get_token(store_id):
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT access_token FROM tiendanube_ruleta_tokens WHERE store_id = %s", (store_id,))
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
                INSERT INTO tiendanube_ruleta_tokens (store_id, access_token)
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


def _register_script(store_id, token):
    """Activa el script de la ruleta en el store (idempotente)."""
    base     = f"https://api.tiendanube.com/v1/{store_id}/scripts"
    headers  = _tn_headers(token)
    existing = http.get(base, headers=headers, timeout=10).json()
    matches  = [s for s in (existing if isinstance(existing, list) else [])
                if s.get("script_id") == SCRIPT_ID]

    if matches:
        return {"ok": True, "msg": f"Script ya activo (id={matches[0]['id']})", "created": False}

    resp = http.post(base, headers=headers, json={
        "script_id": SCRIPT_ID,
        "where":     "store",
        "event":     "onload",
    }, timeout=10)
    return {"ok": resp.ok, "msg": "Script activado", "created": True,
            "detail": resp.json() if resp.content else {}}


def _remove_script(store_id, token):
    """Desactiva el script de la ruleta en el store."""
    base     = f"https://api.tiendanube.com/v1/{store_id}/scripts"
    headers  = _tn_headers(token)
    existing = http.get(base, headers=headers, timeout=10).json()
    matches  = [s for s in (existing if isinstance(existing, list) else [])
                if s.get("script_id") == SCRIPT_ID]

    deleted = []
    for s in matches:
        r = http.delete(f"{base}/{s['id']}", headers=headers, timeout=10)
        deleted.append({"id": s["id"], "ok": r.ok})
    return {"ok": True, "deleted": deleted, "count": len(deleted)}


# ── endpoints ─────────────────────────────────────────────────────────────────

@ruleta_bp.route("/ruleta/install")
def install():
    url = f"https://www.tiendanube.com/apps/{TN_CLIENT_ID}/authorize?response_type=code"
    return redirect(url)


@ruleta_bp.route("/ruleta/callback")
def callback():
    """Recibe el code, intercambia por token y registra el script de la ruleta."""
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

    # El script se activa automáticamente en Tiendanube al completar el OAuth
    return jsonify({"ok": True, "store_id": store_id})


@ruleta_bp.route("/ruleta/setup-script/<int:store_id>")
def setup_script(store_id):
    """Re-registra el script de la ruleta (idempotente)."""
    token = _get_token(store_id)
    if not token:
        return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
    return jsonify(_register_script(store_id, token))


@ruleta_bp.route("/ruleta/remove-script/<int:store_id>")
def remove_script(store_id):
    """Elimina el script de la ruleta del store."""
    token = _get_token(store_id)
    if not token:
        return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
    return jsonify(_remove_script(store_id, token))


@ruleta_bp.route("/ruleta/list-scripts/<int:store_id>")
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


@ruleta_bp.route("/ruleta/test-token/<int:store_id>")
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
