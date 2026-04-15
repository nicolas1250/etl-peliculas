#!/usr/bin/env python3
"""
demo_movies_data.py - Genera 1000 registros simulados de películas.
Simula múltiples extracciones históricas (ratings variables en el tiempo).
"""

import json
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

random = np.random.default_rng(seed=42)

PELICULAS_BASE = [
    {"titulo": "The Shawshank Redemption", "anio": 1994, "genero": "Drama", "director": "Frank Darabont", "pais": "USA"},
    {"titulo": "The Godfather", "anio": 1972, "genero": "Crime", "director": "Francis Ford Coppola", "pais": "USA"},
    {"titulo": "The Dark Knight", "anio": 2008, "genero": "Action", "director": "Christopher Nolan", "pais": "USA"},
    {"titulo": "Pulp Fiction", "anio": 1994, "genero": "Crime", "director": "Quentin Tarantino", "pais": "USA"},
    {"titulo": "Forrest Gump", "anio": 1994, "genero": "Drama", "director": "Robert Zemeckis", "pais": "USA"},
    {"titulo": "Inception", "anio": 2010, "genero": "Sci-Fi", "director": "Christopher Nolan", "pais": "USA"},
    {"titulo": "Interstellar", "anio": 2014, "genero": "Sci-Fi", "director": "Christopher Nolan", "pais": "USA"},
    {"titulo": "Gladiator", "anio": 2000, "genero": "Action", "director": "Ridley Scott", "pais": "USA"},
    {"titulo": "Titanic", "anio": 1997, "genero": "Romance", "director": "James Cameron", "pais": "USA"},
    {"titulo": "The Matrix", "anio": 1999, "genero": "Sci-Fi", "director": "Wachowski Sisters", "pais": "USA"},
]

def generar_registros(n_total: int = 1000) -> list:
    registros = []
    registros_por_pelicula = n_total // len(PELICULAS_BASE)

    inicio = datetime.now() - timedelta(days=registros_por_pelicula)

    for pelicula in PELICULAS_BASE:
        for i in range(registros_por_pelicula):

            fecha = inicio + timedelta(days=i)

            rating = round(
                np.clip(random.normal(8.5, 0.3), 7.0, 9.5),
                1
            )

            duracion = int(np.clip(
                random.normal(140, 15),
                90, 210
            ))

            recaudacion = float(
                np.clip(random.normal(500_000_000, 200_000_000),
                        50_000_000,
                        2_500_000_000)
            )

            registros.append({
                "titulo": pelicula["titulo"],
                "anio": pelicula["anio"],
                "genero": pelicula["genero"],
                "director": pelicula["director"],
                "pais": pelicula["pais"],
                "idioma": "English",
                "imdb_rating": rating,
                "duracion_minutos": duracion,
                "recaudacion": recaudacion,
                "fecha_extraccion": fecha.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp": fecha.isoformat()
            })

    while len(registros) < n_total:
        registros.append(registros[-1].copy())

    return registros[:n_total]


def generar_datos_demo(n: int = 1000):

    os.makedirs('data', exist_ok=True)
    datos = generar_registros(n)

    # JSON crudo
    with open('data/movies_raw.json', 'w', encoding='utf-8') as f:
        json.dump(datos, f, indent=2, ensure_ascii=False)
    print(f"✅ data/movies_raw.json — {len(datos)} registros")

    # CSV transformado
    df = pd.DataFrame(datos)
    df.to_csv('data/movies.csv', index=False)
    print(f"✅ data/movies.csv      — {len(df)} filas x {len(df.columns)} columnas")

    print("\n📊 Resumen por película:")

    resumen = df.groupby('titulo').agg(
        registros=('imdb_rating', 'count'),
        rating_promedio=('imdb_rating', 'mean'),
        rating_min=('imdb_rating', 'min'),
        rating_max=('imdb_rating', 'max'),
        duracion_media=('duracion_minutos', 'mean'),
        recaudacion_promedio=('recaudacion', 'mean')
    ).round(2)

    print(resumen.to_string())

    print(f"\n⚠️  Datos de DEMO (simulados). Total: {len(df)} registros.")


if __name__ == "__main__":
    generar_datos_demo(1000)