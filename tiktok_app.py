"""
tiktok_app.py — Integración con la API de TikTok (Login Kit + Content Posting API) para @hechizo.com.ar.

Rutas públicas (las exige TikTok for Developers):
  /tiktok/terms, /tiktok/privacy  → páginas legales de la app "Hechizo Studio"
  /tiktok/install                 → redirige a autorizar la app con la cuenta de TikTok
  /tiktok/callback                → recibe el code, lo cambia por tokens y los guarda en Supabase (tiktok_tokens)

Los scripts de publicación (C:\\claude\\Hechizo\\tiktok) leen el refresh_token de tiktok_tokens.
"""

import os
import secrets
import time
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta

import requests as http
from flask import Blueprint, redirect, request

tiktok_bp = Blueprint("tiktok", __name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
CLIENT_KEY = os.environ.get("TIKTOK_CLIENT_KEY", "")
CLIENT_SECRET = os.environ.get("TIKTOK_CLIENT_SECRET", "")
REDIRECT_URI = "https://hechizo-reporte-nuevo-production.up.railway.app/tiktok/callback"
SCOPES = "user.info.basic,video.upload,video.publish"

_states = {}  # state -> timestamp (anti-CSRF del flujo OAuth; vive lo que vive el proceso)

_PAGE = """<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>{title}</title>
<style>body{{font-family:system-ui,sans-serif;max-width:720px;margin:40px auto;padding:0 16px;line-height:1.55;color:#222}}
h1{{font-size:1.5rem}}h2{{font-size:1.1rem;margin-top:1.6em}}</style></head><body>{body}</body></html>"""

_TERMS = """<h1>Términos de uso — Hechizo Studio</h1>
<p>Última actualización: 12 de octubre de 2026.</p>
<p>Hechizo Studio es una herramienta interna de <b>Hechizo Bijou</b> (hechizobijou.com.ar), tienda online argentina de bijouterie
y accesorios, que se usa únicamente para publicar contenido propio de la marca en su cuenta oficial de TikTok (@hechizo.com.ar).</p>
<h2>Uso</h2>
<p>La herramienta sube a TikTok videos creados por Hechizo Bijou, con su descripción, a la cuenta que autorizó la conexión.
Solo se publica contenido propio y del que Hechizo Bijou tiene los derechos. No se publica en cuentas de terceros.</p>
<h2>Cuenta y autorización</h2>
<p>El acceso se da mediante el inicio de sesión oficial de TikTok y puede revocarse en cualquier momento desde la configuración
de TikTok (Seguridad → Aplicaciones con permisos). El uso de TikTok se rige además por los Términos de Servicio de TikTok.</p>
<h2>Responsabilidad</h2>
<p>La herramienta se ofrece tal cual, para uso exclusivo de Hechizo Bijou. Contacto: hechizobijou@gmail.com.</p>"""

_PRIVACY = """<h1>Política de privacidad — Hechizo Studio</h1>
<p>Última actualización: 12 de octubre de 2026.</p>
<p>Hechizo Studio es una herramienta interna de <b>Hechizo Bijou</b> (hechizobijou.com.ar) para publicar contenido propio en su
cuenta oficial de TikTok (@hechizo.com.ar).</p>
<h2>Qué datos usamos</h2>
<p>Al autorizar la conexión, TikTok nos entrega un identificador de la cuenta (open_id), datos básicos del perfil
(nombre y foto) y tokens de acceso. Los usamos solo para publicar los videos de Hechizo Bijou en esa cuenta y consultar el
estado de las publicaciones.</p>
<h2>Cómo los guardamos</h2>
<p>Los tokens se guardan en una base de datos privada de Hechizo Bijou y no se comparten, venden ni ceden a terceros.
No recopilamos datos de otros usuarios de TikTok ni de seguidores.</p>
<h2>Eliminación</h2>
<p>Podés revocar el acceso en cualquier momento desde TikTok (Seguridad → Aplicaciones con permisos); al hacerlo los tokens
dejan de funcionar y los borramos. Para pedir la eliminación escribí a hechizobijou@gmail.com.</p>"""


@tiktok_bp.route("/tiktok/terms")
def terms():
    return _PAGE.format(title="Términos — Hechizo Studio", body=_TERMS)


@tiktok_bp.route("/tiktok/privacy")
def privacy():
    return _PAGE.format(title="Privacidad — Hechizo Studio", body=_PRIVACY)


@tiktok_bp.route("/tiktok/install")
def install():
    if not CLIENT_KEY:
        return _PAGE.format(title="Hechizo Studio", body="<p>Falta configurar TIKTOK_CLIENT_KEY.</p>"), 500
    now = time.time()
    for s, t in list(_states.items()):
        if now - t > 900:
            _states.pop(s, None)
    state = secrets.token_urlsafe(24)
    _states[state] = now
    url = ("https://www.tiktok.com/v2/auth/authorize/?" + urlencode({
        "client_key": CLIENT_KEY, "scope": SCOPES, "response_type": "code",
        "redirect_uri": REDIRECT_URI, "state": state}))
    return redirect(url)


def _save_tokens(d):
    import psycopg2
    now = datetime.now(timezone.utc)
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tiktok_tokens (open_id, access_token, refresh_token, scope, expires_at, refresh_expires_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (open_id) DO UPDATE SET access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token, scope = EXCLUDED.scope, expires_at = EXCLUDED.expires_at,
                    refresh_expires_at = EXCLUDED.refresh_expires_at, updated_at = EXCLUDED.updated_at
            """, (d["open_id"], d["access_token"], d["refresh_token"], d.get("scope", ""),
                  now + timedelta(seconds=int(d.get("expires_in", 0))),
                  now + timedelta(seconds=int(d.get("refresh_expires_in", 0))), now))
        conn.commit()
    finally:
        conn.close()


@tiktok_bp.route("/tiktok/callback")
def callback():
    err = request.args.get("error")
    if err:
        return _PAGE.format(title="Hechizo Studio", body=f"<p>TikTok devolvió un error: {err}</p>"), 400
    state = request.args.get("state", "")
    if _states.pop(state, None) is None:
        return _PAGE.format(title="Hechizo Studio", body="<p>Sesión vencida. Volvé a entrar a /tiktok/install.</p>"), 400
    resp = http.post("https://open.tiktokapis.com/v2/oauth/token/", data={
        "client_key": CLIENT_KEY, "client_secret": CLIENT_SECRET, "code": request.args.get("code", ""),
        "grant_type": "authorization_code", "redirect_uri": REDIRECT_URI,
    }, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=15)
    d = resp.json() if resp.ok else {}
    if not d.get("refresh_token"):
        return _PAGE.format(title="Hechizo Studio", body=f"<p>No se pudo completar la conexión ({resp.status_code}).</p>"), 502
    _save_tokens(d)
    return _PAGE.format(title="Hechizo Studio", body=f"<h1>Listo ✨</h1><p>TikTok quedó conectado (permisos: {d.get('scope')}). Ya podés cerrar esta pestaña.</p>")


# Archivo de verificación de "URL properties" (prefijo /tiktok/) de TikTok for Developers.
@tiktok_bp.route("/tiktok/tiktokLDDN7vbjSVJiXzmY1gvMwNmwWNpwRL9D.txt")
def verificacion_url():
    return "tiktok-developers-site-verification=LDDN7vbjSVJiXzmY1gvMwNmwWNpwRL9D", 200, {"Content-Type": "text/plain"}


@tiktok_bp.route("/tiktok/")
def inicio():
    return _PAGE.format(title="Hechizo Studio", body="""<h1>Hechizo Studio</h1>
<p>Herramienta interna de <a href="https://hechizobijou.com.ar">Hechizo Bijou</a> para publicar videos propios de la marca
en su cuenta oficial de TikTok, <a href="https://www.tiktok.com/@hechizo.com.ar">@hechizo.com.ar</a>.</p>
<p><a href="/tiktok/terms">Términos de uso</a> · <a href="/tiktok/privacy">Política de privacidad</a></p>""")
