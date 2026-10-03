"""
auth_clave.py — Exige la clave WEB_PASSWORD (la misma de /web/*) en las rutas de un blueprint.

La clave va en el header X-Web-Key, o en ?clave=... para poder abrir la ruta desde
la barra del navegador (ej. /ruleta/setup-script/1384618?clave=...).
"""

import hmac
import os
import time

from flask import jsonify, request


def exigir_clave(bp, publicas=()):
    """Registra un before_request en `bp`. `publicas` = endpoints que quedan abiertos."""
    publicas = set(publicas)

    @bp.before_request
    def _auth():
        if request.method == "OPTIONS" or request.endpoint in publicas:
            return None
        password = os.environ.get("WEB_PASSWORD", "")
        if not password:
            return jsonify({"ok": False, "error": "WEB_PASSWORD no configurada"}), 503
        key = request.headers.get("X-Web-Key") or request.args.get("clave", "")
        if not hmac.compare_digest(key.encode(), password.encode()):
            time.sleep(0.5)
            return jsonify({"ok": False, "error": "Clave incorrecta"}), 401
        return None
