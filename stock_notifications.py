"""
stock_notifications.py — Blueprint para notificaciones de stock.
POST /notify upserta en Supabase stock_notifications (conflict: email+variant_id).
"""

import os
from datetime import datetime, timezone
from flask import Blueprint, jsonify, request
from flask_cors import cross_origin
import requests as http

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

ALLOWED_ORIGINS = [
    "https://hechizo.com.ar",
    "https://hechizobijou.mitiendanube.com",
]

notify_bp = Blueprint("notify", __name__)

REQUIRED_FIELDS = {"email", "product_id", "variant_id", "store_id", "product_name", "variant_name"}


@notify_bp.route("/notify", methods=["POST", "OPTIONS"])
@cross_origin(origins=ALLOWED_ORIGINS, methods=["POST", "OPTIONS"], allow_headers=["Content-Type"])
def notify():
    body = request.get_json(silent=True) or {}
    missing = REQUIRED_FIELDS - body.keys()
    if missing:
        return jsonify({"error": f"campos requeridos: {', '.join(sorted(missing))}"}), 400

    row = {
        "email":        body["email"].strip().lower(),
        "product_id":   int(body["product_id"]),
        "variant_id":   int(body["variant_id"]),
        "store_id":     int(body["store_id"]),
        "product_name": str(body["product_name"])[:255],
        "variant_name": str(body["variant_name"])[:255],
        "status":       "pending",
        "created_at":   datetime.now(timezone.utc).isoformat(),
    }

    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates,return=representation",
    }

    resp = http.post(
        f"{SUPABASE_URL}/rest/v1/stock_notifications",
        json=row,
        headers=headers,
        timeout=10,
    )

    if not resp.ok:
        return jsonify({"error": "supabase error", "detail": resp.text[:300]}), 502

    return jsonify({"ok": True}), 200
