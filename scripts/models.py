#!/usr/bin/env python3
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy import ForeignKey, Index, Text
from sqlalchemy.orm import relationship
from scripts.datab import Base


class Pelicula(Base):
    """Modelo principal de películas"""
    __tablename__ = "peliculas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    titulo = Column(String(255), nullable=False, index=True)
    titulo_original = Column(String(255), nullable=True)
    anio = Column(Integer, nullable=False, index=True)
    genero = Column(String(150), nullable=True, index=True)
    director = Column(String(150), nullable=True, index=True)
    pais = Column(String(100), nullable=True)
    idioma = Column(String(100), nullable=True)
    activa = Column(Boolean, default=True)
    fecha_creacion = Column(DateTime, default=datetime.utcnow)

    # Relación con detalles extraídos del ETL
    registros = relationship(
        "RegistroPeliculas",
        back_populates="pelicula",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Pelicula(titulo='{self.titulo}', anio={self.anio})>"


class RegistroPeliculas(Base):
    """Datos extraídos en cada ejecución del ETL"""
    __tablename__ = "registro_peliculas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pelicula_id = Column(Integer, ForeignKey('peliculas.id'), nullable=False, index=True)

    imdb_rating = Column(Float, nullable=True)
    duracion_minutos = Column(Integer, nullable=True)
    presupuesto = Column(Float, nullable=True)
    recaudacion = Column(Float, nullable=True)
    descripcion = Column(Text, nullable=True)

    fecha_extraccion = Column(DateTime, default=datetime.utcnow, index=True)
    fecha_creacion = Column(DateTime, default=datetime.utcnow)

    # Relación con película
    pelicula = relationship("Pelicula", back_populates="registros")

    # Índice compuesto optimizado para consultas históricas
    __table_args__ = (
        Index('idx_pelicula_fecha', 'pelicula_id', 'fecha_extraccion'),
    )

    def __repr__(self):
        return f"<RegistroPeliculas(pelicula_id={self.pelicula_id}, rating={self.imdb_rating})>"


class MetricasETL(Base):
    """Registro de métricas de cada ejecución del ETL"""
    __tablename__ = "metricas_etl"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha_ejecucion = Column(DateTime, default=datetime.utcnow, index=True)

    registros_extraidos = Column(Integer, nullable=False)
    registros_transformados = Column(Integer, nullable=True)
    registros_guardados = Column(Integer, nullable=False)
    registros_fallidos = Column(Integer, default=0)

    tiempo_ejecucion_segundos = Column(Float, nullable=False)

    estado = Column(String(50), nullable=False)  # SUCCESS, PARTIAL, FAILED
    mensaje = Column(String(500), nullable=True)

    def __repr__(self):
        return f"<MetricasETL(estado='{self.estado}', registros={self.registros_guardados})>"