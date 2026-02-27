#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from sqlalchemy import func
import sys

sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import Pelicula, RegistroPeliculas

st.set_page_config(
    page_title="Dashboard Películas",
    page_icon="🎬",
    layout="wide"
)

st.title("🎥 Dashboard ETL - Películas")
st.markdown("---")

db = SessionLocal()

tab1, tab2 = st.tabs(["📊 Vista General", "📈 Histórico"])

# ---------- TAB 1: Vista General ----------
with tab1:
    st.subheader("Datos Actuales")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_peliculas = db.query(func.count(Pelicula.id)).scalar()
        st.metric("🎬 Total de Películas", total_peliculas)
    
    with col2:
        total_registros = db.query(func.count(RegistroPeliculas.id)).scalar()
        st.metric("📊 Registros ETL", total_registros)
    
    with col3:
        ultima_fecha = db.query(func.max(RegistroPeliculas.fecha_extraccion)).scalar()
        if ultima_fecha:
            st.metric("⏰ Última Actualización", ultima_fecha.strftime("%Y-%m-%d %H:%M"))
        else:
            st.metric("⏰ Última Actualización", "No hay registros")
    
    st.markdown("---")
    
    # Últimas películas
    peliculas_actuales = db.query(
        Pelicula.titulo,
        RegistroPeliculas.genero,
        RegistroPeliculas.anio,
        RegistroPeliculas.duracion,
        RegistroPeliculas.imdb_rating
    ).join(RegistroPeliculas).order_by(RegistroPeliculas.fecha_extraccion.desc()).limit(20).all()
    
    df_actual = pd.DataFrame(peliculas_actuales, columns=[
        'Título', 'Género', 'Año', 'Duración', 'Calificación'
    ])
    
    if not df_actual.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(df_actual, x='Título', y='Calificación',
                         title='Calificación de Últimas Películas',
                         color='Calificación', color_continuous_scale='Viridis')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.pie(df_actual, values='Duración', names='Título',
                         title='Distribución de Duración')
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        st.dataframe(df_actual, use_container_width=True)
    else:
        st.info("No hay películas registradas aún")

# ---------- TAB 2: Histórico ----------
with tab2:
    st.subheader("Análisis Histórico")
    
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Desde:", value=datetime.now() - timedelta(days=30))
    with col2:
        fecha_fin = st.date_input("Hasta:", value=datetime.now())
    
    registros_historicos = db.query(
        RegistroPeliculas,
        Pelicula.titulo
    ).join(Pelicula).filter(
        RegistroPeliculas.fecha_extraccion >= fecha_inicio,
        RegistroPeliculas.fecha_extraccion <= fecha_fin
    ).all()
    
    if registros_historicos:
        data = []
        for registro, titulo in registros_historicos:
            data.append({
                'Fecha': registro.fecha_extraccion,
                'Película': titulo,
                'Año': registro.anio,
                'Género': registro.genero,
                'Director': registro.director,
                'Duración': registro.duracion,
                'IMDB Rating': registro.imdb_rating,
                'Recaudación': registro.recaudacion
            })
        
        df_historico = pd.DataFrame(data)
        st.dataframe(df_historico, use_container_width=True)
        
        fig = px.line(df_historico, x='Fecha', y='IMDB Rating',
                      color='Película', title='Calificación en el Tiempo',
                      markers=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No hay registros en ese rango de fechas")

db.close()