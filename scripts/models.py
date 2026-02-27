from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class Pelicula(Base):
    __tablename__ = "peliculas"

    id = Column(Integer, primary_key=True, index=True)
    titulo = Column(String, index=True)

class RegistroPeliculas(Base):
    __tablename__ = "registro_peliculas"

    id = Column(Integer, primary_key=True, index=True)
    pelicula_id = Column(Integer, ForeignKey("peliculas.id"))
    anio = Column(Integer)
    genero = Column(String)
    director = Column(String)
    imdb_rating = Column(Float)
    duracion = Column(Integer)
    recaudacion = Column(Float)
    fecha_extraccion = Column(DateTime, default=datetime.utcnow)