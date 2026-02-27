#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar datos
df = pd.read_csv('data/peliculas.csv')

# Limpiar y convertir columnas necesarias
df['calificacion_imdb'] = pd.to_numeric(df['calificacion_imdb'], errors='coerce')
df['anio'] = pd.to_numeric(df['anio'], errors='coerce')
df['duracion'] = df['duracion'].str.replace(' min', '', regex=False)
df['duracion'] = pd.to_numeric(df['duracion'], errors='coerce')

# Crear figura con múltiples gráficas
fig, axes = plt.subplots(2, 2, figsize=(15, 10))
fig.suptitle('Análisis de Películas', fontsize=16, fontweight='bold')

# -------------------------
# Gráfica 1: Calificación IMDb
# -------------------------
ax1 = axes[0, 0]
ax1.bar(df['titulo'], df['calificacion_imdb'], color='#6c5ce7')
ax1.set_title('Calificación IMDb')
ax1.set_ylabel('Rating')
ax1.tick_params(axis='x', rotation=45)
ax1.grid(axis='y', alpha=0.3)

# -------------------------
# Gráfica 2: Año de Lanzamiento
# -------------------------
ax2 = axes[0, 1]
ax2.bar(df['titulo'], df['anio'], color='#00b894')
ax2.set_title('Año de Lanzamiento')
ax2.set_ylabel('Año')
ax2.tick_params(axis='x', rotation=45)
ax2.grid(axis='y', alpha=0.3)

# -------------------------
# Gráfica 3: Duración
# -------------------------
ax3 = axes[1, 0]
ax3.scatter(df['titulo'], df['duracion'], s=200, color='#fdcb6e')
ax3.set_title('Duración (minutos)')
ax3.set_ylabel('Minutos')
ax3.tick_params(axis='x', rotation=45)
ax3.grid(alpha=0.3)

# -------------------------
# Gráfica 4: Rating vs Duración
# -------------------------
ax4 = axes[1, 1]
ax4.scatter(df['duracion'], df['calificacion_imdb'], s=200, color='#e17055')
ax4.set_title('Duración vs Calificación')
ax4.set_xlabel('Duración (min)')
ax4.set_ylabel('Rating IMDb')
ax4.grid(alpha=0.3)

plt.tight_layout()
plt.savefig('data/peliculas_analysis.png', dpi=300, bbox_inches='tight')
logger.info("✅ Gráficas guardadas en data/peliculas_analysis.png")

plt.show()
