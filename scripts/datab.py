#!/usr/bin/env python3
import os
import logging
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


# ===============================
# 🔐 CONFIGURACIÓN INTELIGENTE DB
# ===============================

def _get_db_config():
    """
    1️⃣ Intenta leer desde st.secrets (Streamlit Cloud)
    2️⃣ Si falla, usa variables de entorno (.env local)
    """

    # Intento 1: Streamlit secrets
    try:
        import streamlit as st

        host = st.secrets.get("DB_HOST", "")

        # Si estamos en cloud normalmente NO será localhost
        if host and host != "localhost":
            logger.info("🔐 Usando credenciales desde st.secrets")

            return {
                "host":     host,
                "port":     st.secrets.get("DB_PORT", "5432"),
                "user":     st.secrets.get("DB_USER", "postgres"),
                "password": st.secrets.get("DB_PASSWORD", ""),
                "dbname":   st.secrets.get("DB_NAME", "postgres"),
            }

    except Exception:
        pass  # st no disponible en ejecución local CLI

    # Intento 2: Variables de entorno (.env)
    logger.info("💻 Usando credenciales desde .env")

    return {
        "host":     os.getenv("DB_HOST", "localhost"),
        "port":     os.getenv("DB_PORT", "5432"),
        "user":     os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
        "dbname":   os.getenv("DB_NAME", "weatherstack_etl"),
    }


# ===============================
# 🔧 CREACIÓN DEL ENGINE
# ===============================

config = _get_db_config()

DATABASE_URL = (
    f"postgresql://{config['user']}:{config['password']}"
    f"@{config['host']}:{config['port']}/{config['dbname']}"
)

engine = create_engine(DATABASE_URL, echo=False)

# Base para modelos ORM
Base = declarative_base()

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Metadata
metadata = MetaData()


# ===============================
# 📦 UTILIDADES
# ===============================

def get_db():
    """Obtiene una sesión de base de datos"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection():
    """Prueba la conexión a la base de datos"""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            logger.info("✅ Conexión a PostgreSQL exitosa")
            return True
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {str(e)}")
        return False


def create_all_tables():
    """Crea todas las tablas definidas en los modelos"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas creadas exitosamente")
    except Exception as e:
        logger.error(f"❌ Error creando tablas: {str(e)}")