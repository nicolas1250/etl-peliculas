#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import sys
sys.path.insert(0, '.')

from scripts.datab import SessionLocal
from scripts.models import Pelicula, RegistroPeliculas
from scripts.extractor import MovieExtractor

# ===================== CONFIGURACIÓN =====================
st.set_page_config(
    page_title="Dashboard de Películas ETL",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🎥 Dashboard de Películas - ETL IMDB")
st.markdown("---")

db = SessionLocal()

try:
    registros = db.query(RegistroPeliculas, Pelicula.titulo).join(
        Pelicula
    ).order_by(RegistroPeliculas.fecha_extraccion.desc()).all()

    data = []
    for registro, titulo in registros:
        data.append({
            'Película': titulo,
            'Género': registro.genero,
            'Año': registro.anio,
            'Duración': registro.duracion,
            'IMDB Rating': registro.imdb_rating,
            'Recaudación': registro.recaudacion,
            'Director': registro.director,
            'Fecha': registro.fecha_extraccion
        })

    df = pd.DataFrame(data)

    # 🔥 PROTECCIÓN SI NO HAY DATOS
    if df.empty:
        st.warning("⚠️ No hay datos en la base de datos. Ejecuta el ETL primero.")
        st.stop()

    # ===================== LIMPIEZA NUMÉRICA (FIX ERROR SCATTER) =====================

    numeric_cols = ['Duración', 'IMDB Rating', 'Recaudación']

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Normalizar recaudación para tamaño del scatter (evita burbujas gigantes)
    df['Recaudación_norm'] = df['Recaudación'] / 1_000_000

    # Convertir fecha correctamente
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')

    # ===================== SIDEBAR =====================
    st.sidebar.title("🔧 Filtros")

    peliculas_filtro = st.sidebar.multiselect(
        "Selecciona Películas:",
        options=df['Película'].unique(),
        default=df['Película'].unique()
    )

    genero_filtro = st.sidebar.multiselect(
        "Selecciona Géneros:",
        options=df['Género'].unique(),
        default=df['Género'].unique()
    )

    df_filtrado = df[
        (df['Película'].isin(peliculas_filtro)) &
        (df['Género'].isin(genero_filtro))
    ]

    # ===================== MÉTRICAS =====================
    st.subheader("📈 Métricas Principales")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "⭐ Rating Promedio",
            f"{df_filtrado['IMDB Rating'].mean():.2f}"
        )

    with col2:
        st.metric(
            "⏱️ Duración Promedio",
            f"{df_filtrado['Duración'].mean():.1f} min"
        )

    with col3:
        st.metric(
            "💰 Recaudación Total",
            f"${df_filtrado['Recaudación'].sum():,.0f}"
        )

    with col4:
        st.metric(
            "📊 Total Registros",
            len(df_filtrado)
        )

    st.markdown("---")

    # ===================== VISUALIZACIONES =====================
    st.subheader("📉 Visualizaciones")

    col1, col2 = st.columns(2)

    # Rating por Película
    with col1:
        fig_rating = px.bar(
            df_filtrado.sort_values('IMDB Rating', ascending=False),
            x='Película',
            y='IMDB Rating',
            title="Calificación IMDB por Película",
            color='IMDB Rating',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig_rating, use_container_width=True)

    # Recaudación por Película
    with col2:
        fig_recaudacion = px.bar(
            df_filtrado.sort_values('Recaudación', ascending=False),
            x='Película',
            y='Recaudación',
            title="Recaudación por Película",
            color='Recaudación',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_recaudacion, use_container_width=True)

    # ===================== SCATTER FIXED =====================
    col1, col2 = st.columns(2)

    with col1:
        fig_scatter = px.scatter(
            df_filtrado,
            x='Duración',
            y='IMDB Rating',
            size='Recaudación_norm',  # 🔥 ya no falla
            color='Género',
            title="Duración vs Rating (Tamaño = Recaudación)",
            hover_data=['Película', 'Director']
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    # Evolución temporal
    with col2:
        fig_line = px.line(
            df_filtrado.sort_values('Fecha'),
            x='Fecha',
            y='IMDB Rating',
            color='Película',
            title="Evolución del Rating en el Tiempo",
            markers=True
        )
        st.plotly_chart(fig_line, use_container_width=True)

    st.markdown("---")

    # ===================== TABLA =====================
    st.subheader("📋 Datos Detallados")

    st.dataframe(
        df_filtrado.sort_values('Fecha', ascending=False),
        use_container_width=True,
        height=400
    )

finally:
    db.close()