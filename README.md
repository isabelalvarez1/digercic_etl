# ETL DIGERCIC - Arquitectura Multi-Fuente

Pipeline ETL para extraer datos de multiples fuentes (Oracle, PostgreSQL, SQL Server, archivos) y cargarlos en un Data Warehouse.

## Arquitectura

```
                    ┌─────────────────────────────────────────────┐
                    │         PIPELINE MANAGER (Orquestador)      │
                    │     Coordina Extract → Transform → Load     │
                    └───────────────────────┬─────────────────────┘
                                            │
        ┌───────────────┬───────────────┬───┴───┬───────────────┐
        ▼               ▼               ▼       ▼               ▼
   ┌─────────┐    ┌──────────┐    ┌─────────┐ ┌─────────┐  ┌─────────┐
   │ Oracle  │    │PostgreSQL│    │SQL Server│ │  CSV    │  │ Excel   │
   └────┬────┘    └────┬─────┘    └────┬────┘ └────┬────┘  └────┬────┘
        │              │              │           │             │
        └──────────────┴──────┬───────┴───────────┴─────────────┘
                              ▼
                    ┌─────────────────┐
                    │   DATA LAKE     │
                    │  (PostgreSQL)   │
                    └────────┬────────┘
                             ▼
                    ┌─────────────────┐
                    │ DATA WAREHOUSE  │
                    │   (Futuro)      │
                    └─────────────────┘
```

## Estructura del Proyecto

```
digercic_etl/
├── app/
│   ├── core/                          # Nucleo de la arquitectura
│   │   ├── __init__.py
│   │   ├── factory.py                 # Factory Pattern
│   │   ├── pipeline_manager.py        # Orquestador ETL
│   │   ├── extractors/                # Extractores por fuente
│   │   │   ├── __init__.py
│   │   │   ├── base_extractor.py      # Clase abstracta
│   │   │   ├── oracle_extractor.py    # Oracle
│   │   │   ├── postgres_extractor.py  # PostgreSQL
│   │   │   ├── sqlserver_extractor.py # SQL Server
│   │   │   └── file_extractor.py      # CSV, Excel, TXT, JSON
│   │   ├── loaders/                   # Loaders por destino
│   │   │   ├── __init__.py
│   │   │   ├── base_loader.py         # Clase abstracta
│   │   │   ├── postgres_loader.py     # PostgreSQL
│   │   │   ├── sqlserver_loader.py    # SQL Server
│   │   │   └── file_loader.py         # CSV, Excel, JSON
│   │   ├── transformers/              # Transformaciones
│   │   │   └── __init__.py
│   │   └── connections/               # Pool de conexiones
│   │       └── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── config_loader.py           # Loader de YAML
│   │   ├── logging_config.py          # Config de logging
│   │   └── settings.py                # Variables de entorno
│   ├── main.py                        # Punto de entrada
│   └── logs/
├── config/
│   └── pipeline.yaml                  # Configuracion del pipeline
├── .env                               # Credenciales (NO se sube)
├── .env.example                       # Plantilla
├── .gitignore
├── README.md
└── requirements.txt
```

## Fuentes de Datos Soportadas

| Fuente | Tipo | Extractor | Loader |
|--------|------|-----------|--------|
| Oracle | Base de datos | `OracleExtractor` | - |
| PostgreSQL | Base de datos | `PostgresExtractor` | `PostgresLoader` |
| SQL Server | Base de datos | `SqlServerExtractor` | `SqlServerLoader` |
| CSV | Archivo | `FileExtractor` | `FileLoader` |
| Excel | Archivo | `FileExtractor` | - |
| TXT | Archivo | `FileExtractor` | - |
| JSON | Archivo | `FileExtractor` | `FileLoader` |

## Configuracion YAML

```yaml
extractions:
  - name: oracle_data
    source: oracle
    config:
      host: ${ORACLE_HOST}
      port: ${ORACLE_PORT}
      service: ${ORACLE_SERVICE}
      user: ${ORACLE_USER}
      password: ${ORACLE_PASSWORD}
    query: "SELECT * FROM tabla"
    params: {}

loads:
  - name: load_to_pg
    source: oracle_data
    target: postgresql
    config:
      host: ${POSTGRES_HOST}
      database: ${POSTGRES_DATABASE}
    table: "destino"
    mode: upsert
```

## Instalacion

```bash
# Clonar repositorio
git clone https://github.com/tu-usuario/digercic_etl.git
cd digercic_etl

# Crear entorno virtual
python -m venv .venv
.venv\Scripts\activate

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

```env
# Oracle
ORACLE_HOST=10.91.254.20
ORACLE_PORT=1521
ORACLE_SERVICE=DBINTERO
ORACLE_USER=MIN_DESARROLLO_HUMANO
ORACLE_PASSWORD=xxxxxx

# PostgreSQL
POSTGRES_HOST=192.168.95.24
POSTGRES_PORT=5432
POSTGRES_DATABASE=datalake
POSTGRES_USER=postgres
POSTGRES_PASSWORD=xxxxxx

# SQL Server (opcional)
SQLSERVER_HOST=
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=
SQLSERVER_USER=
SQLSERVER_PASSWORD=
```

## Dependencias

| Paquete | Uso |
|---------|-----|
| `oracledb` | Conexion Oracle |
| `psycopg` | Conexion PostgreSQL |
| `pyodbc` | Conexion SQL Server |
| `polars` | Manipulacion de datos |
| `pyarrow` | Soporte Apache Arrow |
| `pyyaml` | Configuracion |
| `python-dotenv` | Variables de entorno |
| `structlog` | Logging estructurado |

## Patron Factory

La arquitectura usa el patron Factory para crear extractores y loaders dinamicamente:

```python
from core.factory import ExtractorFactory, LoaderFactory

# Crear extractor
extractor = ExtractorFactory.create("oracle", config)
data = extractor.execute(query)

# Crear loader
loader = LoaderFactory.create("postgresql", config)
loader.execute(data, "tabla_destino")
```

## Escalabilidad

Para agregar una nueva fuente de datos:

1. Crear extractor en `core/extractors/`
2. Heredar de `BaseExtractor`
3. Implementar `connect()`, `extract()`, `disconnect()`
4. Registrar en `ExtractorFactory`

```python
class MongoExtractor(BaseExtractor):
    def connect(self): ...
    def extract(self, query, params): ...
    def disconnect(self): ...

# Registrar
ExtractorFactory.register("mongodb", MongoExtractor)
```
