# register_user.py

import re
from pymongo import MongoClient
from datetime import datetime
from config import MONGODB_USER, MONGODB_PASSWORD, MONGODB_CLUSTER, MONGODB_DB_NAME

# Configurar MongoDB
MONGO_URI = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER}/?retryWrites=true&w=majority&appName=EngiAcademyCluster"
client = MongoClient(MONGO_URI)
db = client[MONGODB_DB_NAME]
collection = db["social_accounts"]

# Validación de email
def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

# Pedir datos
while True:
    email = input("Email del usuario: ").strip()
    if is_valid_email(email):
        break
    print("❌ Email no válido. Inténtalo de nuevo.")

youtube_id = input("Canal de YouTube (ej: @miusuario): ").strip()
instagram_id = input("Usuario de Instagram (ej: miusuario): ").strip()
tiktok_id = input("Usuario de TikTok (ej: miusuario): ").strip()

# Verificar que al menos una red esté
if not (youtube_id or instagram_id or tiktok_id):
    print("❌ Debes introducir al menos una red social.")
    exit(1)

# Documento base
update_data = {
    "youtube_id": youtube_id if youtube_id else None,
    "instagram_id": instagram_id if instagram_id else None,
    "tiktok_id": tiktok_id if tiktok_id else None,
    "verified": True,
    "last_updated": datetime.utcnow(),
}

# Verificar si el email ya existe
existing = collection.find_one({"email": email})

if existing:
    collection.update_one(
        {"email": email},
        {"$set": update_data}
    )
    print("🔁 Usuario actualizado.")
else:
    new_doc = {
        "email": email,
        "created_at": datetime.utcnow(),
        **update_data,
    }
    collection.insert_one(new_doc)
    print("✅ Usuario nuevo creado.")
