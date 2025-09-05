import os
from typing import Optional
from flask import Flask, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError

app = Flask(__name__)

# --- Configuración de conexión ---
# Opción A: cadena completa en DATABASE_URL (recomendado)
DATABASE_URL = os.getenv("DATABASE_URL")
config_source = "env:database_url" if DATABASE_URL else "env:pieces"

# Opción B: piezas sueltas (si no usas DATABASE_URL)
if not DATABASE_URL:
    DB_HOST = os.getenv("DB_HOST", "bigdata-sql.mysql.database.azure.com")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_USER = os.getenv("DB_USER", "bigdata")
    DB_PASS = os.getenv("DB_PASS", "uklIAAPN3A8u6s4cQVXK")
    DB_NAME = os.getenv("DB_NAME", "perfilesdb")
    DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# Importante: Azure MySQL requiere TLS → ssl vacío negocia TLS por defecto
engine = create_engine(
    DATABASE_URL,  
    pool_pre_ping=True,
    pool_recycle=300,
    connect_args={"ssl": {}}
)

def mask_url(url_str: Optional[str]) -> str:
    if not url_str:
        return "<missing>"
    try:
        u = make_url(url_str)
        if u.password:
            u = u.set(password="****")
        return str(u)
    except Exception:
        return "<redacted>"
    

@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "status": "ok",
            "config_source": config_source,   # 'env:database_url' o 'env:pieces'
            "dsn_masked": mask_url(DATABASE_URL)    
        }
    except SQLAlchemyError as e:
        return {
            "status": "db_error",
            "config_source": config_source,
            "dsn_masked": mask_url(DATABASE_URL),
            "detail": str(e.__cause__ or e)
        }, 500
    

@app.get("/profile/<username>/")
def get_profile(username: str):
    sql = text("""
        SELECT id, username, full_name, profile_photo_url
        FROM profiles
        WHERE username = :u
        LIMIT 1
    """)
    try:
        with engine.connect() as conn:
            row = conn.execute(sql, {"u": username}).mappings().first()
        if not row:
            return jsonify({"error": "user_not_found"}), 404
        return jsonify({
            "id": row["id"],
            "username": row["username"],
            "name": row["full_name"],
            "profile_photo_url": row["profile_photo_url"]
        })
    except SQLAlchemyError as e:
        return jsonify({"error": str(e.__cause__ or e)}), 500

# (opcional) listado rápido
@app.get("/profiles/")
def list_profiles():
    sql = text("SELECT username, full_name, profile_photo_url FROM profiles ORDER BY id DESC LIMIT 50")
    with engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(sql).mappings().all()]
    return jsonify(rows)

if __name__ == "__main__":
    # Para pruebas locales
    app.run(host="0.0.0.0", port=8080, debug=True)
