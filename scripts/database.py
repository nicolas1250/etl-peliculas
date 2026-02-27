from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from scripts.models import Base


DATABASE_URL = "sqlite:///peliculas.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)
# Crea todas las tablas definidas en los modelos
Base.metadata.create_all(bind=engine)