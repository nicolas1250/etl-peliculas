#!/usr/bin/env python3
"""
transformador_peliculas.py - Fase Transform del pipeline ETL de películas.
Limpia, normaliza y enriquece los datos extraídos (movies.csv).
"""

import pandas as pd
import os
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PeliculasTransformador:

    def __init__(self, input_csv='data/movies.csv'):
        self.input_csv = input_csv
        self.df = None

    # ================= CARGA ================= #

    def cargar_datos(self):
        """Carga los datos desde el CSV generado por el extractor"""
        if not os.path.exists(self.input_csv):
            raise FileNotFoundError(
                f"Archivo {self.input_csv} no encontrado. "
                "Ejecuta primero el extractor."
            )

        self.df = pd.read_csv(self.input_csv)
        logger.info(f"📂 Datos cargados: {len(self.df)} registros desde {self.input_csv}")
        return self

    # ================= LIMPIEZA ================= #

    def limpiar_datos(self):
        """Elimina duplicados exactos y maneja valores nulos"""
        filas_antes = len(self.df)

        self.df.drop_duplicates(inplace=True)

        self.df.fillna({
            'genero': 'Desconocido',
            'director': 'Desconocido',
            'pais': 'Desconocido',
            'idioma': 'Desconocido',
            'imdb_rating': 0.0,
            'duracion_minutos': 0,
            'recaudacion': 0.0
        }, inplace=True)

        filas_despues = len(self.df)

        logger.info(
            f"🧹 Limpieza: {filas_antes - filas_despues} duplicados eliminados, "
            f"{filas_despues} registros restantes"
        )

        return self

    # ================= NORMALIZACIÓN ================= #

    def normalizar_tipos(self):
        """Convierte columnas a tipos correctos"""

        columnas_numericas = [
            'anio',
            'imdb_rating',
            'duracion_minutos',
            'recaudacion'
        ]

        for col in columnas_numericas:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

        if 'fecha_extraccion' in self.df.columns:
            self.df['fecha_extraccion'] = pd.to_datetime(
                self.df['fecha_extraccion'], errors='coerce'
            )

        logger.info("🔧 Tipos de datos normalizados")
        return self

    # ================= ENRIQUECIMIENTO ================= #

    def enriquecer_datos(self):
        """Agrega columnas calculadas para análisis"""

        # Clasificación por rating
        def clasificar_rating(r):
            if r >= 9:
                return 'Obra Maestra'
            elif r >= 8:
                return 'Excelente'
            elif r >= 7:
                return 'Buena'
            elif r > 0:
                return 'Regular'
            else:
                return 'Sin rating'

        self.df['categoria_rating'] = self.df['imdb_rating'].apply(clasificar_rating)

        # Clasificación por duración
        def clasificar_duracion(d):
            if d < 90:
                return 'Corta'
            elif d < 150:
                return 'Media'
            else:
                return 'Larga'

        self.df['categoria_duracion'] = self.df['duracion_minutos'].apply(clasificar_duracion)

        # Rentabilidad simple (recaudación por minuto)
        self.df['recaudacion_por_minuto'] = (
            self.df['recaudacion'] / self.df['duracion_minutos']
        ).replace([float('inf')], 0).round(2)

        # Año de antigüedad
        self.df['antiguedad'] = datetime.now().year - self.df['anio']

        # Fecha de procesamiento
        self.df['fecha_procesamiento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        logger.info("✨ Datos enriquecidos con columnas calculadas")

        return self

    # ================= GUARDAR ================= #

    def guardar_datos(self, output_csv='data/movies_transformado.csv'):
        """Guarda el DataFrame transformado"""

        os.makedirs('data', exist_ok=True)

        self.df.to_csv(output_csv, index=False)
        logger.info(f"💾 Datos transformados guardados en {output_csv}")

        output_xlsx = output_csv.replace('.csv', '.xlsx')
        self.df.to_excel(output_xlsx, index=False, sheet_name='Peliculas')
        logger.info(f"💾 Datos exportados a Excel en {output_xlsx}")

        return self.df

    # ================= RESUMEN ================= #

    def mostrar_resumen(self):
        """Muestra estadísticas descriptivas"""

        print("\n" + "=" * 60)
        print("ESTADÍSTICAS DEL DATASET TRANSFORMADO - PELÍCULAS")
        print("=" * 60)

        columnas = ['imdb_rating', 'duracion_minutos', 'recaudacion']
        print(self.df[columnas].describe().round(2).to_string())

        print("\n🎬 Categorías de Rating:")
        print(self.df['categoria_rating'].value_counts().to_string())

        print("\n⏱ Categorías de Duración:")
        print(self.df['categoria_duracion'].value_counts().to_string())

        print("=" * 60)


if __name__ == "__main__":
    try:
        transformador = PeliculasTransformador()

        df = (
            transformador
            .cargar_datos()
            .limpiar_datos()
            .normalizar_tipos()
            .enriquecer_datos()
            .guardar_datos()
        )

        transformador.mostrar_resumen()

    except FileNotFoundError as e:
        logger.error(str(e))
    except Exception as e:
        logger.error(f"❌ Error fatal en transformación: {str(e)}")
        raise