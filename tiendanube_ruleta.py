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
import threading
import requests as http
from flask import Blueprint, jsonify, make_response, redirect, request

TN_CLIENT_ID     = os.environ.get("TN_RULETA_CLIENT_ID", "")
TN_CLIENT_SECRET = os.environ.get("TN_RULETA_CLIENT_SECRET", "")

SCRIPT_ID = 7428  # ID del script registrado en el portal de Partners

USER_AGENT   = "HechizoBijou-Ruleta/1.0 (hechizobijou@gmail.com)"
DATABASE_URL = os.environ.get("DATABASE_URL", "")
STORE_URL    = os.environ.get("STORE_URL", "https://hechizo.com.ar")

MAIL_PREMIOS = {
    "HECHIZO10":      ("10% OFF",              "10% de descuento sobre el total de tu compra"),
    "HECHIZO15":      ("15% OFF",              "15% de descuento sobre el total de tu compra"),
    "HECHIZO4000":    ("$4.000 de descuento",  "$4.000 de descuento sobre el total de tu compra"),
    "HECHIZOENV":     ("Envío Gratis",         "Envío completamente gratis, sin costo adicional"),
    "HECHIZOPULSERA": ("Pulsera de Argentina", "Una Pulsera de Argentina incluida en tu pedido, sin cargo"),
}

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


def _get_gmail_access_token():
    import requests as _req
    client_id     = os.environ.get("GMAIL_CLIENT_ID", "")
    client_secret = os.environ.get("GMAIL_CLIENT_SECRET", "")
    refresh_token = os.environ.get("GMAIL_REFRESH_TOKEN", "")
    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError("GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET / GMAIL_REFRESH_TOKEN no configurados")
    resp = _req.post("https://oauth2.googleapis.com/token", data={
        "client_id":     client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type":    "refresh_token",
    }, timeout=10)
    if not resp.ok:
        raise RuntimeError(f"Error obteniendo access token: {resp.text[:200]}")
    return resp.json()["access_token"]


def _enviar_mail_cupon(email, premio):
    """Envía email con el cupón ganado. Diseñado para correr en un thread."""
    import requests as _req
    import base64 as _b64
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    if not premio or premio == "nada":
        return

    titulo, desc = MAIL_PREMIOS.get(premio, (premio, f"Tu premio: {premio}"))
    subject = f"Ganaste {titulo} en Hechizo Bijou"

    html = f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f8fc;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f8fc;padding:20px 0;">
<tr><td align="center">
<table width="100%" style="max-width:520px;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08);">

  <!-- Header -->
  <tr>
    <td style="background:#1A3A5C;padding:28px 24px;text-align:center;">
      <div style="color:#C9A227;font-size:20px;letter-spacing:6px;">&#11088; &#11088; &#11088;</div>
      <div style="color:#ffffff;font-size:22px;font-weight:bold;letter-spacing:3px;margin-top:8px;">HECHIZO BIJOU</div>
      <div style="color:#75AADB;font-size:12px;margin-top:4px;letter-spacing:2px;">BIJOUTERIE &amp; ACCESORIOS</div>
    </td>
  </tr>

  <!-- Body -->
  <tr>
    <td style="padding:32px 28px;text-align:center;">
      <div style="font-size:40px;margin-bottom:8px;">&#127942;</div>
      <h1 style="color:#1A3A5C;font-size:22px;margin:0 0 6px;">&#161;Ganaste {titulo}!</h1>
      <p style="color:#666;font-size:14px;margin:0 0 24px;">{desc}</p>

      <!-- Coupon code -->
      <div style="background:#f0f7ff;border:2px dashed #75AADB;border-radius:10px;padding:20px 24px;margin:0 auto 24px;">
        <div style="color:#999;font-size:11px;letter-spacing:2px;text-transform:uppercase;margin-bottom:8px;">Tu c&#243;digo de cup&#243;n</div>
        <div style="font-size:30px;font-weight:bold;color:#1A3A5C;letter-spacing:4px;font-family:Courier,monospace;">{premio}</div>
      </div>

      <!-- Instructions -->
      <div style="text-align:left;background:#fafafa;border-radius:8px;padding:20px 24px;margin-bottom:24px;">
        <div style="color:#1A3A5C;font-weight:bold;font-size:14px;margin-bottom:14px;">C&#243;mo usar tu cup&#243;n:</div>
        <table cellpadding="0" cellspacing="0" width="100%">
          <tr>
            <td style="width:30px;vertical-align:top;padding:5px 0;">
              <span style="background:#75AADB;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-block;text-align:center;font-size:12px;line-height:22px;font-weight:bold;">1</span>
            </td>
            <td style="padding:5px 0 5px 8px;font-size:13px;color:#444;">Visit&#225; nuestra tienda y eleg&#237; tus productos favoritos</td>
          </tr>
          <tr>
            <td style="width:30px;vertical-align:top;padding:5px 0;">
              <span style="background:#75AADB;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-block;text-align:center;font-size:12px;line-height:22px;font-weight:bold;">2</span>
            </td>
            <td style="padding:5px 0 5px 8px;font-size:13px;color:#444;">Agreg&#225; los productos al carrito</td>
          </tr>
          <tr>
            <td style="width:30px;vertical-align:top;padding:5px 0;">
              <span style="background:#75AADB;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-block;text-align:center;font-size:12px;line-height:22px;font-weight:bold;">3</span>
            </td>
            <td style="padding:5px 0 5px 8px;font-size:13px;color:#444;"><strong>Antes de finalizar el pago</strong>, busc&#225; el campo <em>&#34;&#191;Ten&#233;s un cup&#243;n de descuento?&#34;</em> e ingres&#225; el c&#243;digo <strong style="color:#1A3A5C;">{premio}</strong></td>
          </tr>
          <tr>
            <td style="width:30px;vertical-align:top;padding:5px 0;">
              <span style="background:#C9A227;color:#fff;border-radius:50%;width:22px;height:22px;display:inline-block;text-align:center;font-size:12px;line-height:22px;font-weight:bold;">4</span>
            </td>
            <td style="padding:5px 0 5px 8px;font-size:13px;color:#444;">&#161;El descuento se aplica autom&#225;ticamente! Complet&#225; tu compra y listo</td>
          </tr>
        </table>
      </div>

      <!-- CTA -->
      <a href="{STORE_URL}" style="display:inline-block;background:#1A3A5C;color:#ffffff;text-decoration:none;padding:14px 36px;border-radius:8px;font-size:14px;font-weight:bold;letter-spacing:1px;margin-bottom:24px;">
        IR A LA TIENDA
      </a>

      <!-- Terms -->
      <p style="color:#aaa;font-size:11px;margin:0;line-height:1.8;">
        * V&#225;lido por 7 d&#237;as &nbsp;&#183;&nbsp; Para compras mayores a $50.000 &nbsp;&#183;&nbsp; Un solo uso por cliente
      </p>
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="background:#1A3A5C;padding:16px 24px;text-align:center;">
      <div style="color:#75AADB;font-size:12px;line-height:1.8;">
        &#169; 2025 Hechizo Bijou &nbsp;&#183;&nbsp; Bijouterie &amp; Accesorios<br>
        hechizobijou@gmail.com
      </div>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""

    gmail_user = os.environ.get("GMAIL_USER", "hechizobijou@gmail.com")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Hechizo Bijou <{gmail_user}>"
    msg["To"]      = email
    msg["Bcc"]     = "pablofernandez1983@gmail.com"
    msg.attach(MIMEText(html, "html", "utf-8"))

    raw = _b64.urlsafe_b64encode(msg.as_bytes()).decode()
    try:
        access_token = _get_gmail_access_token()
        resp = _req.post(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json={"raw": raw},
            timeout=15,
        )
        if resp.ok:
            print(f"[ruleta] Mail enviado a {email} ({premio})", flush=True)
        else:
            print(f"[ruleta] Gmail API error {resp.status_code}: {resp.text[:200]}", flush=True)
    except Exception as exc:
        print(f"[ruleta] Error enviando mail a {email}: {exc}", flush=True)


def _save_email(email, store_id, premio):
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ruleta_emails (email, store_id, premio)
                VALUES (%s, %s, %s)
            """, (email.lower().strip(), store_id, premio))
        conn.commit()
    finally:
        conn.close()


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
    base     = f"https://api.tiendanube.com/2025-03/{store_id}/scripts"
    headers  = _tn_headers(token)
    existing = http.get(base, headers=headers, timeout=10).json()
    items    = existing.get("result", existing) if isinstance(existing, dict) else existing
    matches  = [s for s in (items if isinstance(items, list) else [])
                if s.get("id") == SCRIPT_ID]

    if matches:
        return {"ok": True, "msg": f"Script ya activo (id={matches[0]['id']})", "created": False}

    resp = http.post(base, headers=headers, json={
        "script_id": SCRIPT_ID,
        "where":     "store",
        "event":     "onfirstinteraction",
    }, timeout=10)
    return {"ok": resp.ok, "msg": "Script activado", "created": True,
            "detail": resp.json() if resp.content else {}}


def _remove_script(store_id, token):
    """Desactiva el script de la ruleta en el store."""
    base     = f"https://api.tiendanube.com/2025-03/{store_id}/scripts"
    headers  = _tn_headers(token)
    existing = http.get(base, headers=headers, timeout=10).json()
    items    = existing.get("result", existing) if isinstance(existing, dict) else existing
    matches  = [s for s in (items if isinstance(items, list) else [])
                if s.get("id") == SCRIPT_ID]

    deleted = []
    for s in matches:
        r = http.delete(f"{base}/{s['id']}", headers=headers, timeout=10)
        deleted.append({"id": s["id"], "ok": r.ok})
    return {"ok": True, "deleted": deleted, "count": len(deleted)}


# ── endpoints ─────────────────────────────────────────────────────────────────

@ruleta_bp.route("/ruleta/suscribir", methods=["POST", "OPTIONS"])
def suscribir():
    """Guarda el email capturado por el widget de la ruleta."""
    r = make_response()
    r.headers["Access-Control-Allow-Origin"] = "*"
    r.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    r.headers["Access-Control-Allow-Headers"] = "Content-Type"

    if request.method == "OPTIONS":
        return r, 204

    data     = request.get_json(silent=True) or {}
    email    = (data.get("email") or "").strip()
    store_id = data.get("store_id")
    premio   = data.get("premio") or "nada"

    if not email or "@" not in email:
        r.set_data(jsonify({"error": "email inválido"}).get_data())
        r.content_type = "application/json"
        return r, 400

    try:
        _save_email(email, store_id, premio)
        threading.Thread(target=_enviar_mail_cupon, args=(email, premio), daemon=True).start()
        r.set_data(jsonify({"ok": True}).get_data())
        r.content_type = "application/json"
        return r, 200
    except Exception as e:
        r.set_data(jsonify({"error": str(e)[:200]}).get_data())
        r.content_type = "application/json"
        return r, 500


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
        f"https://api.tiendanube.com/2025-03/{store_id}/scripts",
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
        f"https://api.tiendanube.com/2025-03/{store_id}/store",
        headers=_tn_headers(token),
        timeout=10,
    )
    return jsonify({
        "ok":          resp.ok,
        "http_status": resp.status_code,
        "store":       resp.json() if resp.ok else resp.text[:300],
    })
