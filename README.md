# ETL OMDb - Extracción de Datos de Películas

Proyecto de Minería de Datos que implementa un pipeline ETL completo para 
extraer, transformar y cargar datos de películas usando la OMDb API.

##  Objetivo

Aprender las 4 fases de un proceso ETL profesional:
1. **Extract** - Obtener datos de APIs externas
2. **Transform** - Procesar y normalizar datos
3. **Load** - Almacenar en múltiples formatos
4. **Visualize** - Analizar y presentar resultados

##  Quick Start

### Requisitos
- Python 3.11+
- pip
- Git

### Instalación

```bash
# Clonar repositorio
git clone https://github.com/tu_usuario/etl-omdb.git
cd etl-omdb
```
# Crear entorno virtual
python3 -m venv venv
source venv/bin/activate  # En Windows: .\venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt

# Configurar API key
echo "OMDB_API_KEY=tu_api_key_aqui" > .env
# Ejecutar el Pipeline
python scripts/extractor_peliculas.py
#  Salida del Pipeline
El script genera:

data/peliculas.csv - Datos de películas en formato CSV

data/peliculas_raw.json - Datos en formato JSON

data/peliculas_analysis.png - Gráficas de análisis

logs/etl.log - Registro de ejecución
#  Estructura del Proyecto
```
etl-peliculas/
├── scripts/
│   ├── extractor_peliculas.py  # Extrae datos de la API OMDb
│   ├── transformador.py        # Procesa los datos
│   └── visualizador.py         # Genera gráficas
├── data/                       # Salida (CSV, JSON, PNG)
├── logs/                       # Registros de ejecución
├── .env                        # Variables de entorno (no commitear)
├── requirements.txt            # Dependencias Python
└── README.md                   # Este archivo
```
#  Obtener API Key
Ve a OMDb API

Registrate y verifica tu email

Copia tu API Key

Pega en .env como:

```
API_KEY=tu_api_key_aqui
BASE_URL=https://www.omdbapi.com/
PELICULAS=Inception,Interstellar,The Matrix,Gladiator,Titanic

```
#  Conceptos Aprendidos
ETL Pipeline: Ciclo de vida completo de datos

APIs REST: Consumir servicios web externos

Python Avanzado: Logging, manejo de errores, variables de entorno

Versionamiento: Git y GitHub para colaboración

Análisis de Datos: Pandas, Matplotlib, Visualización

Buenas Prácticas: Docstring, type hints, testing
# Tecnologías
Python 3.12.3

requests (HTTP client)

pandas (Data processing)

matplotlib (Visualization)

python-dotenv (Environment variables)

Git/GitHub (Version control)
# 👨‍💻 Autor
Yeferson Esmid Heredia Perdomo  
Nicolas Obregon Rojas
# 🤝 Contribuciones
Si deseas mejorar este proyecto:

Haz fork del repositorio

Crea una rama para tu mejora

Commit tus cambios

Push a la rama

Abre un Pull Request

## Última actualización: Febrero 2026
## Estado: En desarrollo ✅
