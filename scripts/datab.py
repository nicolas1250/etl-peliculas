#!/usr/bin/env python3
import os
import logging
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ===============================
# 🔐 CONFIGURACIÓN SEGURA DB
# ===============================

def _get_db_config():
    """
    Prioridad:
    1. Streamlit secrets (Cloud)
    2. Variables de entorno (.env local)
    """

    # =========================
    # 🌐 STREAMLIT CLOUD
    # =========================
    try:
        import streamlit as st

        if hasattr(st, "secrets"):

            host = st.secrets.get("DB_HOST")

            if host:  # solo si existe realmente
                logger.info("🔐 Usando st.secrets (producción)")

                return {
                    "host": st.secrets.get("DB_HOST"),
                    "port": st.secrets.get("DB_PORT", "5432"),
                    "user": st.secrets.get("DB_USER"),
                    "password": st.secrets.get("DB_PASSWORD"),
                    "dbname": st.secrets.get("DB_NAME"),
                }

    except Exception:
        pass  # Streamlit no está disponible (modo CLI)

    # =========================
    # 💻 LOCAL (.env)
    # =========================
    logger.info("💻 Usando variables de entorno (.env)")

    return {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT", "5432"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "dbname": os.getenv("DB_NAME", "etl_peliculas"),
    }


# ===============================
# 🔧 ENGINE SQLALCHEMY
# ===============================

config = _get_db_config()

DATABASE_URL = (
    f"postgresql://{config['user']}:{config['password']}"
    f"@{config['host']}:{config['port']}/{config['dbname']}"
)

engine = create_engine(DATABASE_URL, echo=False)

# Base ORM
Base = declarative_base()

# Session
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Metadata
metadata = MetaData()


# ===============================
# 📦 UTILIDADES
# ===============================

def get_db():
    """Obtiene sesión de base de datos"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_connection():
    """Prueba conexión a PostgreSQL"""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            logger.info("✅ Conexión exitosa a PostgreSQL")
            return True
    except Exception as e:
        logger.error(f"❌ Error de conexión: {e}")
        return False


def create_all_tables():
    """Crea tablas desde modelos ORM"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Tablas creadas correctamente")
    except Exception as e:
        logger.error(f"❌ Error creando tablas: {e}")