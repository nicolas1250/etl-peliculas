#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from sqlalchemy import func, and_
import sys
sys.path.insert(0, '.')

from scripts.database import SessionLocal
from scripts.models import Pelicula, RegistroPeliculas

st.set_page_config(
    page_title="Dashboard Películas",
    page_icon="🎬",
    layout="wide"
)

# CSS personalizado
st.markdown("""
    <style>
    .metric-box {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🎛️ Dashboard Interactivo - Películas")
db = SessionLocal()

# Sidebar con controles
st.sidebar.markdown("### 🔧 Controles")

# Selector de películas
peliculas_disponibles = [p.titulo for p in db.query(Pelicula).all()]
peliculas_seleccionadas = st.sidebar.multiselect(
    "🎬 Películas a Mostrar",
    options=peliculas_disponibles,
    default=peliculas_disponibles[:5]
)

# Rango de fechas de extracción
col1, col2 = st.sidebar.columns(2)
with col1:
    fecha_inicio = st.sidebar.date_input(
        "📅 Desde:",
        value=datetime.now() - timedelta(days=30)
    )
with col2:
    fecha_fin = st.sidebar.date_input(
        "📅 Hasta:",
        value=datetime.now()
    )

# Filtros de calificación IMDB
col1, col2 = st.sidebar.columns(2)
with col1:
    rating_min = st.sidebar.slider("⭐ Calificación Min:", 0.0, 10.0, 0.0, 0.1)
with col2:
    rating_max = st.sidebar.slider("⭐ Calificación Max:", 0.0, 10.0, 10.0, 0.1)

# Obtén datos filtrados
registros_filtrados = db.query(
    RegistroPeliculas,
    Pelicula.titulo
).join(Pelicula).filter(
    and_(
        Pelicula.titulo.in_(peliculas_seleccionadas),
        RegistroPeliculas.fecha_extraccion >= fecha_inicio,
        RegistroPeliculas.fecha_extraccion <= fecha_fin,
        RegistroPeliculas.imdb_rating >= rating_min,
        RegistroPeliculas.imdb_rating <= rating_max
    )
).all()

# Construye DataFrame
data = []
for registro, titulo in registros_filtrados:
    data.append({
        'Título': titulo,
        'Año': registro.anio,
        'Género': registro.genero,
        'Director': registro.director,
        'IMDB Rating': registro.imdb_rating,
        'Duración': registro.duracion,
        'Recaudación': registro.recaudacion,
        'Fecha Extracción': registro.fecha_extraccion
    })

df = pd.DataFrame(data) if data else pd.DataFrame()

if not df.empty:
    # KPIs
    st.markdown("### 📊 Indicadores Clave")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("⭐ Rating Max", f"{df['IMDB Rating'].max():.1f}")
    with col2:
        st.metric("⭐ Rating Min", f"{df['IMDB Rating'].min():.1f}")
    with col3:
        st.metric("⭐ Rating Prom", f"{df['IMDB Rating'].mean():.1f}")
    with col4:
        st.metric("⏱️ Duración Prom", f"{df['Duración'].mean():.0f} min")
    with col5:
        st.metric("💰 Recaudación Total", f"${df['Recaudación'].sum():,.0f}")
    
    st.markdown("---")
    
    # Gráficas interactivas
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Comparativa de Ratings")
        fig = px.box(
            df,
            x='Título',
            y='IMDB Rating',
            color='Título',
            title='Distribución de Calificaciones IMDB por Película'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### Duración de Películas")
        fig = px.bar(
            df,
            x='Título',
            y='Duración',
            color='Duración',
            color_continuous_scale='Viridis',
            title='Duración de Películas'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Línea temporal de rating
    st.markdown("#### 📈 Evolución Temporal")
    df['Fecha Extracción'] = pd.to_datetime(df['Fecha Extracción'])
    rating_tiempo = df.groupby(['Fecha Extracción', 'Título'])['IMDB Rating'].mean().reset_index()
    
    fig = px.line(
        rating_tiempo,
        x='Fecha Extracción',
        y='IMDB Rating',
        color='Título',
        title='Calificación IMDB en el Tiempo',
        markers=True
    )
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Tabla interactiva
    st.markdown("#### 📋 Datos Detallados")
    col1, col2 = st.columns(2)
    
    with col1:
        mostrar_todos = st.checkbox("Mostrar todos los registros", value=False)
    
    with col2:
        columnas_mostrar = st.multiselect(
            "Columnas a mostrar:",
            df.columns.tolist(),
            default=['Título', 'Año', 'Género', 'IMDB Rating', 'Duración', 'Recaudación', 'Fecha Extracción']
        )
    
    if mostrar_todos:
        st.dataframe(df[columnas_mostrar], use_container_width=True, height=600)
    else:
        st.dataframe(df[columnas_mostrar].head(20), use_container_width=True)
    
    # Descargar datos
    st.markdown("---")
    csv = df.to_csv(index=False)
    st.download_button(
        label="⬇️ Descargar datos como CSV",
        data=csv,
        file_name=f"peliculas_datos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
    
else:
    st.warning("⚠️ No hay datos que coincidan con los filtros seleccionados")

db.close()
