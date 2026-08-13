# ETL DIGERCIC - Arquitectura Multi-Fuente

Pipeline ETL para extraer datos de multiples fuentes (Oracle, PostgreSQL, SQL Server, archivos) y cargarlos en un Data Warehouse, usando **Polars** para procesamiento de alto rendimiento.

## Arquitectura

```
┌─────────────────────────────────────────────────┐
│            PIPELINE MANAGER (Orquestador)        │
│      Coordina Extract → Transform → Load         │
│        Soporta modo paralelo y secuencial        │
└──────────────────────┬──────────────────────────┘
                       │
    ┌──────────┬───────┴───────┬──────────┐
    ▼          ▼               ▼          ▼
┌────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐
│ Oracle │ │PostgreSQL│ │SQL Server│ │ Archivo│
└───┬────┘ └────┬─────┘ └────┬─────┘ └───┬────┘
    │           │            │           │
    └───────────┴─────┬──────┴───────────┘
                      ▼
            ┌──────────────────┐
            │  PostgreSQL      │
            │ (local_stage)    │
            └──────────────────┘
```

## Estructura del Proyecto

```
digercic_etl/
├── app/
│   ├── __init__.py
│   ├── main.py                        # Punto de entrada
│   ├── core/                          # Nucleo de la arquitectura
│   │   ├── __init__.py
│   │   ├── factory.py                 # Factory Pattern (ExtractorFactory/LoaderFactory)
│   │   ├── pipeline_manager.py        # Orquestador ETL con resolucion de variables
│   │   ├── extractors/                # Extractores por fuente
│   │   │   ├── __init__.py
│   │   │   ├── base_extractor.py      # Clase abstracta con patron template
│   │   │   ├── oracle_extractor.py    # Oracle (oracledb, batch OFFSET/FETCH)
│   │   │   ├── postgres_extractor.py  # PostgreSQL (psycopg)
│   │   │   ├── sqlserver_extractor.py # SQL Server (pyodbc)
│   │   │   └── file_extractor.py      # CSV, Excel, TXT, JSON (Polars)
│   │   └── loaders/                   # Loaders por destino
│   │       ├── __init__.py
│   │       ├── base_loader.py         # Clase abstracta
│   │       ├── postgres_loader.py     # PostgreSQL (Polars batch, column_mapping)
│   │       ├── sqlserver_loader.py    # SQL Server (pyodbc)
│   │       └── file_loader.py         # CSV, Excel, JSON (Polars)
│   ├── config/
│   │   ├── __init__.py
│   │   ├── config_loader.py           # Loader de YAML con get() dot-notation
│   │   ├── logging_config.py          # Config de logging
│   │   └── settings.py                # Variables de entorno desde .env
│   └── logs/
│       └── digercic_etl.log
├── config/
│   └── pipeline.yaml                  # Configuracion del pipeline (multi-tabla)
├── .env                               # Credenciales (NO se sube a git)
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

El pipeline se configura mediante `config/pipeline.yaml` con soporte para multiples tablas:

```yaml
pipeline:
  name: "digercic_etl"
  version: "3.0.0"
  parallel: false

extractions:
  - name: clientes_oracle
    source: oracle
    config:
      host: ${ORACLE_HOST}
      port: ${ORACLE_PORT}
      service: ${ORACLE_SERVICE}
      user: ${ORACLE_USER}
      password: ${ORACLE_PASSWORD}
      batch_size: 50000
    query: "SELECT * FROM CLIENTES"
    params: {}

loads:
  - name: clientes_to_postgres
    source: clientes_oracle
    target: postgresql
    config:
      host: ${POSTGRES_HOST}
      port: ${POSTGRES_PORT}
      database: ${POSTGRES_DATABASE}
      user: ${POSTGRES_USER}
      password: ${POSTGRES_PASSWORD}
      batch_size: 5000
      column_mapping:              # Mapeo Oracle → PostgreSQL
        CAMPAA: campana
    table: "clientes"
    mode: insert
```

### Resolucion de Variables

Las variables `${VAR}` en el YAML se resuelven automaticamente desde las variables de entorno (`.env`):

```yaml
host: ${ORACLE_HOST}  # → os.getenv("ORACLE_HOST")
```

### Column Mapping

Para manejar diferencias de nombres de columnas entre origen y destino:

```yaml
column_mapping:
  CAMPAA: campana        # Oracle (encoding normalizado) → PostgreSQL
  NOMBRE_LARGO: nombre     # Otro mapeo
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
# Oracle (Docker - adb-free)
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE=myatp
ORACLE_USER=CAPTACION
ORACLE_PASSWORD=DianaDB#2026X

# PostgreSQL (local)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=local_stage
POSTGRES_USER=postgres
POSTGRES_PASSWORD=admin

# SQL Server (opcional)
SQLSERVER_HOST=
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=
SQLSERVER_USER=
SQLSERVER_PASSWORD=
```

> **Nota**: Oracle corre en Docker (`adb-free`). Iniciar con: `docker start adb-free`

## Dependencias

| Paquete | Uso |
|---------|-----|
| `oracledb` | Conexion Oracle (driver lightweight) |
| `psycopg[binary]` | Conexion PostgreSQL |
| `pyodbc` | Conexion SQL Server |
| `polars` | Manipulacion de datos de alto rendimiento |
| `pyarrow` | Soporte Apache Arrow para Polars |
| `pyyaml` | Configuracion YAML |
| `python-dotenv` | Variables de entorno |
| `openpyxl` | Lectura de archivos Excel |

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

## Batch Processing

Para tablas grandes (millones de registros), el ETL usa procesamiento por lotes:

- **Oracle Extractor**: `batch_size=50000` (OFFSET/FETCH NEXT)
- **PostgreSQL Loader**: `batch_size=5000` (insercion por lotes)
- **File Extractor/Loader**: Polars para manejo eficiente de memoria

Ejemplo de extraccion en lotes desde Oracle:
```python
# Genera queries como:
# SELECT * FROM tabla OFFSET 0 ROWS FETCH NEXT 50000 ROWS ONLY
# SELECT * FROM tabla OFFSET 50000 ROWS FETCH NEXT 50000 ROWS ONLY
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

## Tablas Actuales

| Tabla | Fuente | Destino | Registros |
|-------|--------|---------|-----------|
| `CLIENTES` | Oracle | PostgreSQL | 101 |
| `CAPTACIONES` | Oracle | PostgreSQL | 5 |
