"""
app_reporte_nuevo.py — Flask wrapper para Railway
Expone reporte_nuevo.py como endpoint HTTP.
"""

import os, sys, threading, io
from collections import deque
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
from reporte_nuevo import main as ejecutar_reporte

DATABASE_URL = os.environ.get("DATABASE_URL", "")

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

_log_buffer = deque(maxlen=200)  # últimas 200 líneas


class _LogCapture(io.TextIOBase):
    """Stream que escribe en stdout Y en el buffer de logs."""
    def __init__(self, original):
        self._original = original

    def write(self, text):
        self._original.write(text)
        self._original.flush()
        line = text.rstrip("\n")
        if line:
            _log_buffer.append(line)
        return len(text)

    def flush(self):
        self._original.flush()


def _run():
    _estado["corriendo"] = True
    _estado["ultimo_inicio"] = datetime.now(TZ_AR).isoformat()
    _estado["error"] = None
    _log_buffer.clear()

    original_stdout = sys.stdout
    sys.stdout = _LogCapture(original_stdout)
    try:
        ejecutar_reporte()
        _estado["resultado"] = "OK"
    except Exception as e:
        _estado["resultado"] = "ERROR"
        _estado["error"] = str(e)
    finally:
        sys.stdout = original_stdout
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


@app.route("/logs")
def ver_logs():
    desde = request.args.get("desde", 0, type=int)
    lineas = list(_log_buffer)
    nuevas = lineas[desde:]
    return jsonify({
        "ok": True,
        "total": len(lineas),
        "lineas": nuevas,
    })


@app.route("/historico")
def historico():
    """Devuelve ventas agrupadas por día para los últimos 45 días desde Supabase."""
    if not DATABASE_URL:
        return jsonify({"ok": False, "error": "DATABASE_URL no configurada"}), 500
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        cur = conn.cursor()
        cur.execute("""
            SELECT fecha,
                   ROUND(SUM(subtotal - descuento)::numeric, 0) AS total,
                   COUNT(orden_id) AS cantidad
            FROM ventas
            WHERE fecha >= CURRENT_DATE - INTERVAL '45 days'
              AND estado_pago IN ('paid', 'authorized')
            GROUP BY fecha
            ORDER BY fecha
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        dias = [
            {"fecha": str(r[0]), "total": int(r[1]), "cantidad": int(r[2])}
            for r in rows
        ]
        return jsonify({"ok": True, "dias": dias})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "msg": "hechizo-reporte-nuevo running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
