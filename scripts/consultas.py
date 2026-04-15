#!/usr/bin/env python3
import sys
sys.path.insert(0, '.')
from scripts.datab import SessionLocal
from scripts.models import Pelicula, RegistroPeliculas, MetricasETL
from sqlalchemy import func
import pandas as pd

db = SessionLocal()


# 🎬 1️⃣ Rating promedio por película
def rating_promedio_por_pelicula():
    """Calcula el rating promedio por película"""
    registros = db.query(
        Pelicula.titulo,
        func.avg(RegistroPeliculas.imdb_rating).label('rating_promedio')
    ).join(RegistroPeliculas).group_by(Pelicula.titulo).all()

    df = pd.DataFrame(registros, columns=['Película', 'Rating Promedio'])
    print("\n🎬 RATING PROMEDIO POR PELÍCULA:")
    print(df.to_string(index=False))


# 🏆 2️⃣ Película con mejor rating registrado
def pelicula_mejor_rating():
    """Obtiene la película con mayor rating"""
    registro = db.query(
        Pelicula.titulo,
        RegistroPeliculas.imdb_rating,
        RegistroPeliculas.fecha_extraccion
    ).join(Pelicula).order_by(
        RegistroPeliculas.imdb_rating.desc()
    ).first()

    if registro:
        print(f"\n🏆 MEJOR RATING: {registro.titulo} con {registro.imdb_rating}")


# ⏱ 3️⃣ Película más larga registrada
def pelicula_mas_larga():
    """Obtiene la película con mayor duración"""
    registro = db.query(
        Pelicula.titulo,
        RegistroPeliculas.duracion_minutos
    ).join(Pelicula).order_by(
        RegistroPeliculas.duracion_minutos.desc()
    ).first()

    if registro:
        print(f"\n⏱ PELÍCULA MÁS LARGA: {registro.titulo} con {registro.duracion_minutos} minutos")


# 📅 4️⃣ Total de registros por año
def total_peliculas_por_anio():
    """Cuenta películas agrupadas por año"""
    registros = db.query(
        Pelicula.anio,
        func.count(Pelicula.id)
    ).group_by(
        Pelicula.anio
    ).order_by(
        Pelicula.anio
    ).all()

    df = pd.DataFrame(registros, columns=['Año', 'Cantidad Registros'])
    print("\n📅 TOTAL DE REGISTROS POR AÑO:")
    print(df.to_string(index=False))


# 📈 5️⃣ Últimas ejecuciones del ETL
def metricas_etl():
    """Muestra métricas de ejecuciones"""
    metricas = db.query(MetricasETL).order_by(
        MetricasETL.fecha_ejecucion.desc()
    ).limit(5).all()

    print("\n📈 ÚLTIMAS 5 EJECUCIONES DEL ETL:")
    for m in metricas:
        print(f"  - {m.fecha_ejecucion}: {m.estado} "
              f"({m.registros_guardados} registros en "
              f"{m.tiempo_ejecucion_segundos:.2f}s)")


# 🔥 MAIN
if __name__ == "__main__":
    try:
        print("\n" + "="*50)
        print("ANÁLISIS DE DATOS - PELÍCULAS (POSTGRESQL)")
        print("="*50)

        rating_promedio_por_pelicula()
        pelicula_mejor_rating()
        pelicula_mas_larga()
        total_peliculas_por_anio()
        metricas_etl()

        print("\n" + "="*50 + "\n")

    finally:
        db.close()