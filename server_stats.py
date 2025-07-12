#!/usr/bin/env python3
# server_stats.py — versión completa actualizada 11-jul-2025 (usa config.py)

"""
Servidor HTTP que devuelve en JSON las estadísticas de redes sociales
almacenadas en MongoDB para el usuario cuyo `email` se pasa como query-string.

Endpoints:
  GET /             → página de bienvenida
  GET /api/stats    → stats JSON, requiere ?email=tu_email

Requisitos:
    pip install flask pymongo certifi
"""

from flask import Flask, request, jsonify, abort
from pymongo import MongoClient
import certifi
from config import (
    MONGODB_USER,
    MONGODB_PASSWORD,
    MONGODB_CLUSTER,
    MONGODB_DB_NAME
)

# Verificar que config.py tenga todas las credenciales
if not all([MONGODB_USER, MONGODB_PASSWORD, MONGODB_CLUSTER, MONGODB_DB_NAME]):
    raise RuntimeError("Revisa config.py: faltan credenciales de MongoDB")

# Construir URI de conexión
MONGO_URI = (
    f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}"
    f"@{MONGODB_CLUSTER}/?retryWrites=true&w=majority"
)

# Cliente Mongo con verificación TLS usando el bundle de certifi
client = MongoClient(
    MONGO_URI,
    tls=True,
    tlsCAFile=certifi.where()
)
COL = client[MONGODB_DB_NAME]["social_accounts"]

app = Flask(__name__)

@app.route("/")
def index():
    return (
        "<h1>Social Stats API</h1>"
        "<p>Usa <code>/api/stats?email=tu_email</code> para obtener datos JSON.</p>"
    ), 200

@app.route("/api/stats", methods=["GET"])
def stats():
    email = request.args.get("email")
    if not email:
        abort(400, description="Falta el parámetro 'email'")
    # Solo cuentas verificadas
    doc = COL.find_one({"email": email, "verified": True})
    if not doc:
        abort(404, description="Cuenta no encontrada o no verificada")

    # Preparar solo los campos públicos
    resultado = {
        "email":        doc["email"],
        "tiktok":       doc.get("tiktok_stats",    {}),
        "instagram":    doc.get("instagram_stats", {}),
        "youtube":      doc.get("youtube_stats",   {}),
        "last_updated": doc.get("last_updated")
    }
    return jsonify(resultado)

if __name__ == "__main__":
    # En producción reemplaza el servidor dev con Gunicorn/Waitress
    app.run(host="0.0.0.0", port=80, debug=False)
