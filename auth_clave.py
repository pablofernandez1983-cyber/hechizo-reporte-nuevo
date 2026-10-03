"""
auth_clave.py — Exige la clave WEB_PASSWORD (la misma de /web/*).

La clave va solo en el header X-Web-Key (nunca en la URL: quedaría en logs e historial).
"""

import hmac
import os
import time

from flask import jsonify, request


def chequear_clave():
    """None si la clave es válida; si no, la respuesta de error a devolver."""
    password = os.environ.get("WEB_PASSWORD", "")
    if not password:
        return jsonify({"ok": False, "error": "WEB_PASSWORD no configurada"}), 503
    key = request.headers.get("X-Web-Key", "")
    if not hmac.compare_digest(key.encode(), password.encode()):
        time.sleep(0.5)
        return jsonify({"ok": False, "error": "Clave incorrecta"}), 401
    return None


def exigir_clave(bp, publicas=()):
    """Registra un before_request en `bp`. `publicas` = endpoints que quedan abiertos."""
    publicas = set(publicas)

    @bp.before_request
    def _auth():
        if request.method == "OPTIONS" or request.endpoint in publicas:
            return None
        return chequear_clave()
