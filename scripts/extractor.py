#!/usr/bin/env python3

import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging
from scripts.datab import SessionLocal
from scripts.models import Pelicula, RegistroPeliculas
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

    def procesar_respuesta(self, data):
        try:
            duracion = data.get("Runtime")
            duracion = int(duracion.replace(" min", "")) if duracion and "min" in duracion else 0

            rating = data.get("imdbRating")
            rating = float(rating) if rating and rating != "N/A" else 0.0

            box = data.get("BoxOffice")
            if box and box != "N/A":
                box = float(box.replace("$", "").replace(",", ""))
            else:
                box = 0.0

            return {
                "titulo": data.get("Title"),
                "anio": data.get("Year"),
                "genero": data.get("Genre"),
                "director": data.get("Director"),
                "duracion": duracion,
                "rating": rating,
                "recaudacion": box,
                'fecha_extraccion': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error procesando datos: {e}")
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
        
    def guardar_en_bd(self, datos):
        """Guarda los datos procesados en la base de datos"""
        db = SessionLocal()

        try:
            for pelicula_data in datos:

                # Buscar si la película ya existe
                pelicula = db.query(Pelicula).filter_by(
                    titulo=pelicula_data['titulo']
                ).first()

                # Si no existe, crearla
                if not pelicula:
                    pelicula = Pelicula(
                        titulo=pelicula_data['titulo']
                    )
                    db.add(pelicula)
                    db.commit()
                    db.refresh(pelicula)

                # Crear registro histórico
                registro = RegistroPeliculas(
                    pelicula_id=pelicula.id,
                    anio=pelicula_data['anio'],
                    genero=pelicula_data['genero'],
                    director=pelicula_data['director'],
                    duracion=pelicula_data['duracion'],
                    imdb_rating=pelicula_data['rating'],
                    fecha_extraccion=datetime.now()
                )

                db.add(registro)

            db.commit()
            print("✅ Datos guardados en la base de datos")

        except Exception as e:
            db.rollback()
            print("❌ Error guardando en BD:", e)

        finally:
            db.close()


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
