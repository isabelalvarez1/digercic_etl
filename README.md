# ETL DIGERCIC - Arquitectura Multi-Fuente

Pipeline ETL para extraer datos de multiples fuentes (Oracle, PostgreSQL) y cargarlos en un Data Warehouse, usando **Polars** para procesamiento de alto rendimiento.

## Arquitectura

```
┌─────────────────────────────────────────────────┐
│            PIPELINE MANAGER (Orquestador)        │
│      Coordina Extract → Transform → Load         │
│        Soporta modo paralelo y secuencial        │
└──────────────────────┬──────────────────────────┘
                       │
     ┌──────────┬──────┴───────┬──────────┐
     ▼          ▼              ▼          ▼
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
├── run.py                              # Punto de entrada
├── app/
│   ├── core/                           # Nucleo de la arquitectura
│   │   ├── __init__.py
│   │   ├── factory.py                  # Factory Pattern (ExtractorFactory/LoaderFactory)
│   │   ├── pipeline_manager.py         # Orquestador ETL con validacion de recursos
│   │   ├── utils.py                    # Utilidades: deteccion de recursos del sistema
│   │   ├── extractors/                 # Extractores por fuente
│   │   │   ├── __init__.py
│   │   │   ├── base_extractor.py       # Clase abstracta con patron template
│   │   │   └── oracle_extractor.py     # Oracle (oracledb, batch OFFSET/FETCH)
│   │   └── loaders/                    # Loaders por destino
│   │       ├── __init__.py
│   │       ├── base_loader.py          # Clase abstracta
│   │       └── postgres_loader.py      # PostgreSQL (Polars batch, column_mapping)
│   └── config/
│       └── logging_config.py           # Config de logging con archivos por tabla
├── config/
│   └── pipeline.yaml                   # Configuracion del pipeline (multi-tabla)
├── .env                                # Credenciales (NO se sube a git)
├── .env.example                        # Plantilla
├── .gitignore
├── README.md
└── requirements.txt
```

## Fuentes de Datos Soportadas

| Fuente | Tipo | Extractor | Loader |
|--------|------|-----------|--------|
| Oracle | Base de datos | `OracleExtractor` | - |
| PostgreSQL | Base de datos | - | `PostgresLoader` |

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
      truncate_before_load: true
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

### Truncate Before Load

Para truncar la tabla antes de insertar:

```yaml
truncate_before_load: true  # TRUNCATE TABLE antes de INSERT
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
# Asegurarse que Oracle este corriendo (Docker)
docker start adb-free

# Ejecutar ETL
python run.py
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
```

> **Nota**: Oracle corre en Docker (`adb-free`). Iniciar con: `docker start adb-free`

## Dependencias

| Paquete | Uso |
|---------|-----|
| `oracledb` | Conexion Oracle (driver lightweight) |
| `psycopg[binary]` | Conexion PostgreSQL |
| `polars` | Manipulacion de datos de alto rendimiento |
| `pyarrow` | Soporte Apache Arrow para Polars |
| `pyyaml` | Configuracion YAML |
| `python-dotenv` | Variables de entorno |
| `openpyxl` | Lectura de archivos Excel |
| `psutil` | Deteccion de recursos del sistema |

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

## Batch Processing con Deteccion de Recursos

El ETL detecta automaticamente los recursos del sistema y calcula el tamano de lote optimo:

### Deteccion de Recursos

```python
from core.utils import get_system_resources

resources = get_system_resources()
# {
#   "cpu_cores_logical": 8,
#   "cpu_cores_physical": 4,
#   "memory_total_gb": 15.79,
#   "memory_available_gb": 2.03,
#   "memory_percent_used": 87.2
# }
```

### Calculo de Lotes Optimos

```python
from core.utils import calculate_optimal_batch_size

batch_info = calculate_optimal_batch_size(total_rows=22000000, resources=resources)
# {
#   "total_rows": 22000000,
#   "batch_size": 80000,
#   "batch_count": 275,
#   "rows_per_core": 2750000,
#   "estimated_memory_mb": 7812.5
# }
```

### Configuracion por Defecto

| Componente | Batch Size | Descripcion |
|------------|------------|-------------|
| Oracle Extractor | 50000 | Registros por OFFSET/FETCH |
| PostgreSQL Loader | 5000 | Registros por INSERT |
| Optimizado | 80000 | Calculado segun CPU y memoria |

### Ejemplo de Lotes

```sql
-- Oracle: Extraccion por lotes
SELECT * FROM CLIENTES OFFSET 0 ROWS FETCH NEXT 80000 ROWS ONLY
SELECT * FROM CLIENTES OFFSET 80000 ROWS FETCH NEXT 80000 ROWS ONLY
SELECT * FROM CLIENTES OFFSET 160000 ROWS FETCH NEXT 80000 ROWS ONLY
```

## Logging por Tabla

Cada tabla tiene su propio archivo de log con informacion detallada:

```
app/logs/
├── clientes_2026-08-13.log      # Log especifico de clientes
├── captaciones_2026-08-13.log   # Log especifico de captaciones
└── digercic_etl.log             # Log general del pipeline
```

### Ejemplo de Log (Extraccion)

```
INICIO EXTRACCION: clientes
Fecha inicio: 2026-08-13 16:39:04
--------------------------------------------------
Paso 1/6: Detectando recursos del sistema...
  CPU Cores Logicales: 8
  CPU Cores Fisicos: 4
  Memoria Total: 15.79 GB
  Memoria Disponible: 2.03 GB
  Memoria Usada: 87.2%
Paso 2/6: Preparando cursor...
  Cursor creado
Paso 3/6: Obteniendo estructura de la tabla...
  Columnas encontradas: 7
  Nombres: ID_CLIENTE, NOMBRES, APELLIDOS, EMAIL, TELEFONO, CIUDAD, FECHA_REGISTRO
Paso 4/6: Contando registros totales...
  Total registros: 101
Paso 5/6: Calculando lotes optimos...
  Batch Size Configurado: 50000
  Batch Size Optimizado: 80000
  Batch Size Final: 80000
  Total Lotes: 1
  Registros por Core: 12
  Memoria Estimada por Lote: 7812.5 MB
Paso 6/6: Extrayendo datos...
--------------------------------------------------
  CHUNK 1/1
    Registros: 101
    Chunk Size: 80000
    Offset: 0
    Tiempo: 0.14s
    Progreso: 100.0%
    Acumulado: 101/101
--------------------------------------------------
EXTRACCION COMPLETADA
  Registros extraidos: 101
  Chunks procesados: 1
  Chunk Size utilizado: 80000
  Tiempo total: 0.16 segundos
  Velocidad: 644 registros/segundo
  CPU Cores utilizados: 8
  Memoria disponible: 2.03 GB
Fecha fin: 2026-08-13 16:39:05
==================================================
```

### Ejemplo de Log (Carga)

```
INICIO CARGA: clientes
Fecha inicio: 2026-08-13 16:39:05
--------------------------------------------------
Paso 1/7: Detectando recursos del sistema...
  CPU Cores Logicales: 8
  Memoria Disponible: 2.01 GB
Paso 2/7: Convirtiendo datos a DataFrame...
  Registros: 101
  Columnas originales: 7
Paso 3/7: Normalizando nombres de columnas...
  Columnas normalizadas: ['ID_CLIENTE', 'NOMBRES', 'APELLIDOS', 'EMAIL', 'TELEFONO', 'CIUDAD', 'FECHA_REGISTRO']
Paso 4/7: Aplicando mapeo de columnas...
  No hay mapeo configurado
Paso 5/7: Calculando lotes optimos...
  Batch Size Configurado: 5000
  Batch Size Optimizado: 80000
  Batch Size Final: 5000
  Total Lotes: 1
Paso 6/7: Preparando tabla destino...
  Tabla clientes truncada
Paso 7/7: Insertando datos por lotes...
--------------------------------------------------
  CHUNK 1/1
    Registros: 101
    Chunk Size: 5000
    Offset: 0
    Tiempo: 0.02s
    Progreso: 100.0%
    Acumulado: 101/101
--------------------------------------------------
CARGA COMPLETADA
  Registros insertados: 101
  Chunks procesados: 1
  Chunk Size utilizado: 5000
  Tiempo total: 0.03 segundos
  Velocidad: 2914 registros/segundo
  CPU Cores utilizados: 8
  Memoria disponible: 2.01 GB
Fecha fin: 2026-08-13 16:39:05
==================================================
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
