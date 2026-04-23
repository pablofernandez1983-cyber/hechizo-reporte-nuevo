"""
stock_notifications.py — Blueprint para notificaciones de stock.
POST /notify upserta en stock_notifications (conflict: email+variant_id).
Usa DATABASE_URL con psycopg2, igual que el resto de la app.
"""

import os
from flask import Blueprint, jsonify, request
from flask_cors import cross_origin

DATABASE_URL = os.environ.get("DATABASE_URL", "")

ALLOWED_ORIGINS = [
    "https://hechizo.com.ar",
    "https://hechizobijou.mitiendanube.com",
]

notify_bp = Blueprint("notify", __name__)

REQUIRED_FIELDS = {"email", "product_id", "variant_id", "product_name", "variant_name"}


@notify_bp.route("/notify", methods=["POST", "OPTIONS"])
@cross_origin(origins=ALLOWED_ORIGINS, methods=["POST", "OPTIONS"], allow_headers=["Content-Type"])
def notify():
    body = request.get_json(silent=True) or {}
    missing = REQUIRED_FIELDS - body.keys()
    if missing:
        return jsonify({"error": f"campos requeridos: {', '.join(sorted(missing))}"}), 400

    email        = body["email"].strip().lower()
    product_id   = int(body["product_id"])
    variant_id   = int(body["variant_id"])
    store_id     = int(body.get("store_id") or 1384618)
    product_name = str(body["product_name"])[:255]
    variant_name = str(body["variant_name"])[:255]

    import psycopg2
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stock_notifications
                  (email, product_id, variant_id, store_id, product_name, variant_name, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT (email, variant_id)
                DO UPDATE SET
                  product_name = EXCLUDED.product_name,
                  variant_name = EXCLUDED.variant_name,
                  status       = 'pending'
            """, (email, product_id, variant_id, store_id, product_name, variant_name))
        conn.commit()
    finally:
        conn.close()

    return jsonify({"ok": True}), 200
