#!/usr/bin/env python3
"""
extractor_db_movies.py — Fase Load del ETL de películas.
Lee movies_transformado.csv y realiza inserción masiva en PostgreSQL.
"""

import sys
sys.path.insert(0, '.')

import os
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
import logging
from sqlalchemy.exc import IntegrityError

from scripts.datab import SessionLocal
from scripts.models import Pelicula, RegistroPeliculas, MetricasETL

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MovieETLDB:

    def __init__(self):
        self.db = SessionLocal()
        self.tiempo_inicio = time.time()
        self.registros_extraidos = 0
        self.registros_guardados = 0
        self.registros_fallidos = 0

    # ----------------------------------------------------
    # Obtener o crear película
    # ----------------------------------------------------
    def _obtener_pelicula(self, fila: pd.Series) -> Pelicula:
        titulo = str(fila.get("titulo")).strip()

        pelicula = self.db.query(Pelicula).filter(
            Pelicula.titulo == titulo
        ).first()

        if not pelicula:
            pelicula = Pelicula(
                titulo=titulo,
                anio=int(fila.get("anio")) if pd.notna(fila.get("anio")) else None,
                genero=str(fila.get("genero", "Desconocido")),
                director=str(fila.get("director", "Desconocido")),
                pais=str(fila.get("pais", "Desconocido")),
                idioma=str(fila.get("idioma", "Desconocido")),
                activa=True
            )
            self.db.add(pelicula)
            self.db.commit()
            self.db.refresh(pelicula)
            logger.info(f"🎬 Película registrada: {titulo}")

        return pelicula

    # ----------------------------------------------------
    # Guardar métricas
    # ----------------------------------------------------
    def _guardar_metricas(self, estado: str):
        try:
            tiempo = round(time.time() - self.tiempo_inicio, 2)

            metricas = MetricasETL(
                registros_extraidos=self.registros_extraidos,
                registros_guardados=self.registros_guardados,
                registros_fallidos=self.registros_fallidos,
                tiempo_ejecucion_segundos=tiempo,
                estado=estado,
                mensaje=f"{self.registros_guardados} registros guardados "
                        f"de {self.registros_extraidos} en {tiempo}s"
            )

            self.db.add(metricas)
            self.db.commit()

            logger.info(f"📈 Métricas guardadas — estado: {estado}")

        except Exception as e:
            logger.error(f"❌ Error guardando métricas: {e}")

    # ----------------------------------------------------
    # Ejecutar Load completo con BULK INSERT
    # ----------------------------------------------------
    def ejecutar(self) -> bool:

        csv_path = 'data/movies_transformado.csv'

        if not os.path.exists(csv_path):
            logger.error(f"❌ No se encontró {csv_path}. Ejecuta primero el transformador.")
            return False

        logger.info(f"📂 Cargando datos desde {csv_path}")
        df = pd.read_csv(csv_path)

        self.registros_extraidos = len(df)
        logger.info(f"📊 {self.registros_extraidos} registros a procesar")

        # 1️⃣ Registrar películas únicas primero
        peliculas_unicas = df["titulo"].dropna().unique()
        pelicula_map = {}

        for titulo in peliculas_unicas:
            fila = df[df["titulo"] == titulo].iloc[0]
            pelicula_map[titulo] = self._obtener_pelicula(fila)

        logger.info(f"🎬 {len(pelicula_map)} películas registradas")

        # 2️⃣ Construir registros históricos en memoria
        registros_bulk = []

        for _, fila in df.iterrows():
            try:
                titulo = str(fila.get("titulo")).strip()
                pelicula = pelicula_map.get(titulo)

                if not pelicula:
                    self.registros_fallidos += 1
                    continue

                registros_bulk.append(
                    RegistroPeliculas(
                        pelicula_id=pelicula.id,
                        imdb_rating=float(fila.get("imdb_rating", 0)),
                        duracion_minutos=int(fila.get("duracion_minutos", 0)),
                        recaudacion=float(fila.get("recaudacion", 0)),
                        fecha_extraccion=pd.to_datetime(
                            fila.get("fecha_extraccion", datetime.now())
                        )
                    )
                )

            except Exception as e:
                logger.warning(f"⚠️ Fila omitida: {e}")
                self.registros_fallidos += 1

        # 3️⃣ Bulk insert
        try:
            self.db.bulk_save_objects(registros_bulk)
            self.db.commit()

            self.registros_guardados = len(registros_bulk)

            logger.info(f"✅ Bulk insert completado: {self.registros_guardados} registros")

        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Error en bulk insert: {e}")
            return False

        estado = "SUCCESS" if self.registros_fallidos == 0 else "PARTIAL"
        self._guardar_metricas(estado)

        logger.info(
            f"✅ ETL completado — Guardados: {self.registros_guardados} | "
            f"Fallidos: {self.registros_fallidos}"
        )

        return True

    # ----------------------------------------------------
    # Mostrar resumen
    # ----------------------------------------------------
    def mostrar_resumen(self):
        try:
            total_peliculas = self.db.query(Pelicula).count()
            total_registros = self.db.query(RegistroPeliculas).count()

            print("\n📊 RESUMEN EN BASE DE DATOS")
            print(f"   Películas registradas : {total_peliculas}")
            print(f"   Registros históricos  : {total_registros}")

        except Exception as e:
            logger.error(f"❌ Error mostrando resumen: {e}")
        finally:
            self.db.close()


if __name__ == "__main__":
    etl = MovieETLDB()
    exito = etl.ejecutar()
    etl.mostrar_resumen()
    sys.exit(0 if exito else 1)