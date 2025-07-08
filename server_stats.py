"""
Levanta un servidor HTTP que devuelve, en JSON, los stats guardados en MongoDB
para el usuario cuyo `email` se pasa como query-string.

GET /api/stats?email=test@gmail.com

Requisitos:
    pip install flask pymongo python-dotenv
    # (.env con credenciales o usa config.py)
"""

import os, json
from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson import json_util
from dotenv import load_dotenv
import config  # contiene MONGODB_USER, MONGODB_PASSWORD, ...

load_dotenv()  # opcional, si prefieres variables de entorno

# --- MongoDB -------------------------------------------------------------
URI = (
    f"mongodb+srv://{config.MONGODB_USER}:{config.MONGODB_PASSWORD}"
    f"@{config.MONGODB_CLUSTER}/?retryWrites=true&w=majority"
)
COL = MongoClient(URI)[config.MONGODB_DB_NAME]["social_accounts"]

# --- Flask ---------------------------------------------------------------
app = Flask(__name__)

@app.route("/api/stats")
def stats():
    email = request.args.get("email")
    if not email:
        return jsonify({"error": "email parameter is required"}), 400

    doc = COL.find_one({"email": email, "verified": True})
    if not doc:
        return jsonify({"error": "user not found"}), 404

    # Convertir ObjectId y fechas BSON → JSON serializable
    return app.response_class(
        json.dumps(doc, default=json_util.default),
        mimetype="application/json"
    )

@app.route("/")
def index():
    return "Social Stats API — use /api/stats?email=<user>", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
