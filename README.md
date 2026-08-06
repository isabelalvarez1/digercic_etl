# ETL DIGERCIC

Proyecto ETL para extraer datos desde Oracle, transformarlos y cargarlos en PostgreSQL.

## Estado del Proyecto

| Modulo | Estado | Descripcion |
|--------|--------|-------------|
| `config/settings.py` | Completo | Variables de entorno desde `.env` |
| `config/logging_config.py` | Completo | Logger a archivo y consola |
| `database/oracle_pool.py` | Completo | Pool de conexiones Oracle |
| `database/postgres_pool.py` | Vacio | Pendiente implementar |
| `extract/oracle_extractor.py` | Vacio | Pendiente implementar |
| `transform/digercic_transform.py` | Vacio | Pendiente implementar |
| `load/postgres_loader.py` | Vacio | Pendiente implementar |
| `pipelines/digercic_pipeline.py` | Vacio | Pendiente implementar |

## Estructura

```
digercic_etl/
├── app/
│   ├── __init__.py
│   ├── main.py                  # Punto de entrada
│   ├── config/
│   │   ├── __init__.py
│   │   ├── logging_config.py    # Config de logging
│   │   └── settings.py          # Variables de entorno
│   ├── database/
│   │   ├── __init__.py
│   │   ├── oracle_pool.py       # Pool Oracle
│   │   └── postgres_pool.py     # Pool PostgreSQL (vacio)
│   ├── extract/
│   │   ├── __init__.py
│   │   └── oracle_extractor.py  # Extractor Oracle (vacio)
│   ├── transform/
│   │   ├── __init__.py
│   │   └── digercic_transform.py # Transform (vacio)
│   ├── load/
│   │   ├── __init__.py
│   │   └── postgres_loader.py   # Loader PostgreSQL (vacio)
│   ├── pipelines/
│   │   ├── __init__.py
│   │   └── digercic_pipeline.py # Pipeline (vacio)
│   ├── utils/
│   │   └── __init__.py
│   └── logs/
├── tests/
├── .env                 # NO se sube al repositorio
├── .env.example         # Plantilla de variables
├── .gitignore
├── README.md
└── requirements.txt
```

## Instalacion

```bash
# Clonar repositorio
git clone https://github.com/tu-usuario/digercic_etl.git
cd digercic_etl

# Crear entorno virtual
python -m venv .venv
.venv\Scripts\activate      # Windows
# source .venv/bin/activate  # Linux/Mac

# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
copy .env.example .env
# Editar .env con tus credenciales
```

## Ejecucion

```bash
cd app
python main.py
```

## Variables de Entorno

Copiar `.env.example` a `.env` y completar:

| Variable | Descripcion | Default |
|----------|-------------|---------|
| ORACLE_HOST | Host de Oracle | - |
| ORACLE_PORT | Puerto Oracle | 1521 |
| ORACLE_SERVICE | Service name de Oracle | - |
| ORACLE_USER | Usuario de Oracle | - |
| ORACLE_PASSWORD | Password de Oracle | - |
| POSTGRES_HOST | Host de PostgreSQL | - |
| POSTGRES_PORT | Puerto PostgreSQL | 5432 |
| POSTGRES_DATABASE | Base de datos PostgreSQL | - |
| POSTGRES_USER | Usuario de PostgreSQL | - |
| POSTGRES_PASSWORD | Password de PostgreSQL | - |

## Dependencias

| Paquete | Uso |
|---------|-----|
| `python-dotenv` | Variables de entorno |
| `oracledb` | Conexion a Oracle |
| `psycopg` | Conexion a PostgreSQL |
| `polars` | Manipulacion de datos |
| `pyarrow` | Soporte Apache Arrow |
| `pydantic` | Validacion de datos |
| `structlog` | Logging estructurado |

## Git

El archivo `.env` esta excluido del repositorio via `.gitignore` para proteger las credenciales.

Para subir cambios:

```bash
git add .
git commit -m "mensaje descriptivo"
git push origin master
```
