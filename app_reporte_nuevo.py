"""
app_reporte_nuevo.py — Flask wrapper para Railway
Expone reporte_nuevo.py como endpoint HTTP.
"""

import os, threading
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
from reporte_nuevo import main as ejecutar_reporte

app = Flask(__name__)
CORS(app)

TZ_AR = timezone(timedelta(hours=-3))

_estado = {
    "corriendo": False,
    "ultimo_inicio": None,
    "ultimo_fin": None,
    "resultado": None,
    "error": None,
}


def _run():
    _estado["corriendo"] = True
    _estado["ultimo_inicio"] = datetime.now(TZ_AR).isoformat()
    _estado["error"] = None
    try:
        ejecutar_reporte()
        _estado["resultado"] = "OK"
    except Exception as e:
        _estado["resultado"] = "ERROR"
        _estado["error"] = str(e)
    finally:
        _estado["corriendo"] = False
        _estado["ultimo_fin"] = datetime.now(TZ_AR).isoformat()


@app.route("/ejecutar", methods=["POST"])
def ejecutar():
    if _estado["corriendo"]:
        return jsonify({"ok": False, "msg": "Ya está corriendo un reporte"}), 409
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "msg": "Reporte iniciado"})


@app.route("/estado")
def ver_estado():
    return jsonify({
        "ok": True,
        "corriendo": _estado["corriendo"],
        "ultimo_inicio": _estado["ultimo_inicio"],
        "ultimo_fin": _estado["ultimo_fin"],
        "resultado": _estado["resultado"],
        "error": _estado["error"],
    })


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "msg": "hechizo-reporte-nuevo running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
