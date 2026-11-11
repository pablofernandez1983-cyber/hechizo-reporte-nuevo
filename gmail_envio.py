"""
gmail_envio.py — Envío de mails desde la cuenta de Hechizo por la API de Gmail.

Lo usan los avisos de stock, la ruleta y el seguimiento de envíos (antes cada uno tenía
su copia). El access token se reutiliza hasta que está por vencer.
"""

import base64
import os
import threading
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

_token = {"valor": None, "vence": 0}
_lock = threading.Lock()


def access_token():
    with _lock:
        if _token["valor"] and time.time() < _token["vence"]:
            return _token["valor"]
        client_id     = os.environ.get("GMAIL_CLIENT_ID", "")
        client_secret = os.environ.get("GMAIL_CLIENT_SECRET", "")
        refresh_token = os.environ.get("GMAIL_REFRESH_TOKEN", "")
        if not all([client_id, client_secret, refresh_token]):
            raise RuntimeError("GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET / GMAIL_REFRESH_TOKEN no configurados")
        resp = requests.post("https://oauth2.googleapis.com/token", data={
            "client_id":     client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type":    "refresh_token",
        }, timeout=10)
        if not resp.ok:
            raise RuntimeError(f"Error obteniendo access token: {resp.text[:200]}")
        d = resp.json()
        _token["valor"] = d["access_token"]
        _token["vence"] = time.time() + int(d.get("expires_in", 3600)) - 300
        return _token["valor"]


def usuario():
    return os.environ.get("GMAIL_USER", "hechizobijou@gmail.com")


def enviar(to_addr, asunto, html, bcc=None):
    """Manda un mail HTML. Lanza RuntimeError si Gmail lo rechaza."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = asunto
    msg["From"]    = f"Hechizo <{usuario()}>"
    msg["To"]      = to_addr
    if bcc:
        msg["Bcc"] = bcc
    msg.attach(MIMEText(html, "html", "utf-8"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    resp = requests.post(
        "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
        headers={"Authorization": f"Bearer {access_token()}", "Content-Type": "application/json"},
        json={"raw": raw},
        timeout=20,
    )
    if not resp.ok:
        raise RuntimeError(f"Gmail API error {resp.status_code}: {resp.text[:200]}")
