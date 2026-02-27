#!/usr/bin/env python3
import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MovieExtractor:
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.base_url = os.getenv('BASE_URL')
        self.peliculas = os.getenv('PELICULAS').split(',')

        if not self.api_key:
            raise ValueError("API_KEY no configurada en .env")

    def extraer_pelicula(self, pelicula):
        """Extrae datos de una película específica"""
        try:
            params = {
                'apikey': self.api_key,
                't': pelicula.strip()
            }

            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get("Response") == "False":
                logger.error(f"❌ Error en API para {pelicula}: {data.get('Error')}")
                return None

            logger.info(f"✅ Datos extraídos para {pelicula}")
            return data

        except Exception as e:
            logger.error(f"❌ Error extrayendo datos para {pelicula}: {str(e)}")
            return None

    def procesar_respuesta(self, response_data):
        """Procesa la respuesta JSON a formato estructurado"""
        try:
            return {
                'titulo': response_data.get('Title'),
                'anio': response_data.get('Year'),
                'genero': response_data.get('Genre'),
                'director': response_data.get('Director'),
                'actores': response_data.get('Actors'),
                'duracion': response_data.get('Runtime'),
                'calificacion_imdb': response_data.get('imdbRating'),
                'votos_imdb': response_data.get('imdbVotes'),
                'idioma': response_data.get('Language'),
                'pais': response_data.get('Country'),
                'fecha_extraccion': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error procesando respuesta: {str(e)}")
            return None

    def ejecutar_extraccion(self):
        """Ejecuta la extracción para todas las películas"""
        datos_extraidos = []

        logger.info(f"Iniciando extracción para {len(self.peliculas)} películas...")

        for pelicula in self.peliculas:
            response = self.extraer_pelicula(pelicula)
            if response:
                datos_procesados = self.procesar_respuesta(response)
                if datos_procesados:
                    datos_extraidos.append(datos_procesados)

        return datos_extraidos


if __name__ == "__main__":
    try:
        extractor = MovieExtractor()
        datos = extractor.ejecutar_extraccion()

        # Guardar como JSON
        with open('data/peliculas_raw.json', 'w') as f:
            json.dump(datos, f, indent=2)

        logger.info("📁 Datos guardados en data/peliculas_raw.json")

        # Guardar como CSV
        df = pd.DataFrame(datos)
        df.to_csv('data/peliculas.csv', index=False)

        logger.info("📁 Datos guardados en data/peliculas.csv")

        print("\n" + "="*50)
        print("RESUMEN DE EXTRACCIÓN")
        print("="*50)
        print(df.to_string())
        print("="*50)

    except Exception as e:
        logger.error(f"Error en extracción: {str(e)}")
