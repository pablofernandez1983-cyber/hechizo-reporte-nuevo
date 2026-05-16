"""
Servicio liviano para notificaciones de stock en Railway.

Expone solo los endpoints necesarios para el script de Tiendanube y OAuth:
  - /notify
  - /notify/webhook
  - /notify/*
  - /tiendanube/*
"""

from flask import Flask, jsonify
from flask_cors import CORS

from stock_notifications import notify_bp
from tiendanube_app import tn_bp


app = Flask(__name__)
CORS(app)

app.register_blueprint(notify_bp)
app.register_blueprint(tn_bp)


@app.route("/")
@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "stock-notifications"})


if __name__ == "__main__":
    app.run(host="0.0.0.0")
