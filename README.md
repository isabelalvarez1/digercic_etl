# ETL DIGERCIC

Proyecto ETL para extraer datos desde Oracle, transformarlos y cargarlos en PostgreSQL.

## Arquitectura

El pipeline se configura mediante un archivo YAML (`config/pipeline.yaml`) que define cada paso del proceso ETL.

```
config/pipeline.yaml  -->  main.py  -->  Extract  -->  Transform  -->  Load
     |                                      |              |              |
     v                                      v              v              v
 Deficion                          Oracle Pool     Polars/Python   PostgreSQL Pool
```

## Estado del Proyecto

| Modulo | Estado | Descripcion |
|--------|--------|-------------|
| `config/settings.py` | Completo | Variables de entorno desde `.env` |
| `config/logging_config.py` | Completo | Logger a archivo y consola |
| `config/config_loader.py` | Completo | Carga configuracion YAML |
| `config/pipeline.yaml` | Completo | Definicion del pipeline ETL |
| `database/oracle_pool.py` | Completo | Pool de conexiones Oracle |
| `main.py` | Completo | Orquestador ETL basado en YAML |
| `database/postgres_pool.py` | Vacio | Pendiente implementar |
| `extract/oracle_extractor.py` | Vacio | Pendiente implementar |
| `transform/digercic_transform.py` | Vacio | Pendiente implementar |
| `load/postgres_loader.py` | Vacio | Pendiente implementar |

## Estructura

```
digercic_etl/
├── config/
│   └── pipeline.yaml           # Configuracion del pipeline ETL
├── app/
│   ├── __init__.py
│   ├── main.py                 # Orquestador ETL
│   ├── config/
│   │   ├── __init__.py
│   │   ├── config_loader.py    # Loader de YAML
│   │   ├── logging_config.py   # Config de logging
│   │   └── settings.py         # Variables de entorno
│   ├── database/
│   │   ├── __init__.py
│   │   ├── oracle_pool.py      # Pool Oracle
│   │   └── postgres_pool.py    # Pool PostgreSQL (vacio)
│   ├── extract/
│   │   ├── __init__.py
│   │   └── oracle_extractor.py # Extractor Oracle (vacio)
│   ├── transform/
│   │   ├── __init__.py
│   │   └── digercic_transform.py # Transform (vacio)
│   ├── load/
│   │   ├── __init__.py
│   │   └── postgres_loader.py  # Loader PostgreSQL (vacio)
│   ├── pipelines/
│   │   └── __init__.py
│   ├── utils/
│   │   └── __init__.py
│   └── logs/
├── tests/
├── .env                        # NO se sube al repositorio
├── .env.example                # Plantilla de variables
├── .gitignore
├── README.md
└── requirements.txt
```

## Configuracion YAML

El archivo `config/pipeline.yaml` define:

```yaml
pipeline:
  name: "digercic_etl"
  version: "1.0.0"

extract:
  source: oracle
  enabled: true
  query: "SELECT * FROM tabla"
  params:
    fecha_inicio: "2024-01-01"

transform:
  enabled: true
  operations:
    - name: limpiar_nulos
      type: drop_nulls
      columns: [id, nombre]

load:
  target: postgres
  enabled: true
  table: "destino_tabla"
  mode: upsert
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
| `pyyaml` | Configuracion YAML |
| `oracledb` | Conexion a Oracle |
| `psycopg` | Conexion a PostgreSQL |
| `polars` | Manipulacion de datos |
| `pyarrow` | Soporte Apache Arrow |
| `pydantic` | Validacion de datos |
| `structlog` | Logging estructurado |

## Flujo del Pipeline

1. **Extract** - Lee datos de Oracle segun query definida en YAML
2. **Transform** - Aplica operaciones (drop_nulls, cast, replace, etc.)
3. **Load** - Inserta/upsert en PostgreSQL

## Git

El archivo `.env` esta excluido del repositorio via `.gitignore` para proteger las credenciales.

Para subir cambios:

```bash
git add .
git commit -m "mensaje descriptivo"
git push origin master
```
