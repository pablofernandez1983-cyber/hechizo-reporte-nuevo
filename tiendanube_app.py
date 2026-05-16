"""
tiendanube_app.py — OAuth 2 + registro de scripts para la app de Tiendanube.

Flujo:
  1. GET /tiendanube/install      → redirige a Tiendanube para autorizar
  2. GET /tiendanube/callback     → intercambia code por token, guarda en DB,
                                    asocia el script de Partner Portal
  3. GET /tiendanube/setup-script/<store_id>  → re-asocia el script (idempotente)
"""

import os
import requests as http
from flask import Blueprint, jsonify, redirect, request

TN_CLIENT_ID     = os.environ.get("TN_APP_CLIENT_ID", "")
TN_CLIENT_SECRET = os.environ.get("TN_APP_CLIENT_SECRET", "")

BASE_URL         = os.environ.get("BASE_URL", "https://hechizo-reporte-nuevo-production.up.railway.app")
DEFAULT_TN_SCRIPT_ID = 6218

USER_AGENT   = "HechizoBijou-Stock/1.0 (hechizobijou@gmail.com)"
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


def _response_detail(resp):
    if not resp.content:
        return {}
    try:
        return resp.json()
    except ValueError:
        return {"raw": resp.text[:300]}


def _tn_script_id():
    raw_script_id = os.environ.get("TN_SCRIPT_ID", "").strip()
    if not raw_script_id:
        return DEFAULT_TN_SCRIPT_ID
    try:
        return int(raw_script_id)
    except ValueError as exc:
        raise ValueError("TN_SCRIPT_ID debe ser un numero entero") from exc


def _script_matches(script, script_id):
    expected = str(script_id)
    return str(script.get("script_id") or script.get("id")) == expected


def _is_auto_installed_error(detail):
    if not isinstance(detail, dict):
        return False
    message = str(detail.get("message", "")).lower()
    return "auto installed" in message and "store association" in message


def _register_script(store_id, token, script_id=None):
    """Asocia al store el script creado en Partner Portal. Devuelve dict con resultado."""
    script_id = script_id or _tn_script_id()
    base     = f"https://api.tiendanube.com/v1/{store_id}/scripts"
    headers  = _tn_headers(token)
    existing = http.get(base, headers=headers, timeout=10).json()
    matches  = [s for s in (existing if isinstance(existing, list) else [])
                if _script_matches(s, script_id)]

    if matches:
        return {
            "ok": True,
            "msg": f"Script ya asociado (id={matches[0].get('id', script_id)})",
            "created": False,
            "script_id": script_id,
        }

    payload = {
        "script_id": script_id,
        "query_params": "{}",
    }
    resp = http.post(base, headers=headers, json=payload, timeout=10)
    detail = _response_detail(resp)
    if resp.status_code == 422 and _is_auto_installed_error(detail):
        return {
            "ok": True,
            "msg": "Script auto instalado por Tiendanube; no requiere asociacion manual",
            "created": False,
            "script_id": script_id,
            "http_status": resp.status_code,
            "detail": detail,
        }

    return {
        "ok": resp.ok,
        "msg": "Script asociado",
        "created": resp.ok,
        "script_id": script_id,
        "http_status": resp.status_code,
        "detail": detail,
    }


# ── endpoints ─────────────────────────────────────────────────────────────────

@tn_bp.route("/tiendanube/install")
def install():
    url = f"https://www.tiendanube.com/apps/{TN_CLIENT_ID}/authorize?response_type=code"
    return redirect(url)


@tn_bp.route("/tiendanube/callback")
def callback():
    """Recibe el code, intercambia por token y asocia el script automáticamente."""
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
    try:
        result = _register_script(store_id, access_token)
    except ValueError as exc:
        return jsonify({"ok": False, "store_id": store_id, "error": str(exc)}), 500

    return jsonify({"ok": result.get("ok", False), "store_id": store_id, "script": result})


@tn_bp.route("/tiendanube/setup-script/<int:store_id>")
def setup_script(store_id):
    token = _get_token(store_id)
    if not token:
        return jsonify({"error": f"No hay token para store_id={store_id}"}), 404
    try:
        return jsonify(_register_script(store_id, token))
    except ValueError as exc:
        return jsonify({"ok": False, "store_id": store_id, "error": str(exc)}), 500


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
