# ETL DIGERCIC

Proyecto ETL para extraer datos desde Oracle, transformarlos y cargarlos en PostgreSQL.

## Estructura

```
digercic_etl/
├── app/
│   ├── config/          # Configuracion y logging
│   ├── database/        # Pools de conexiones
│   ├── extract/         # Extraccion desde Oracle
│   ├── transform/       # Transformacion de datos
│   ├── load/            # Carga a PostgreSQL
│   ├── pipelines/       # Orquestacion ETL
│   ├── utils/           # Utilidades
│   ├── logs/            # Archivos de log
│   └── main.py          # Punto de entrada
├── tests/
├── .env.example
├── .gitignore
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

| Variable | Descripcion |
|----------|-------------|
| ORACLE_HOST | Host de Oracle |
| ORACLE_PORT | Puerto Oracle (default: 1521) |
| ORACLE_SERVICE | Service name de Oracle |
| ORACLE_USER | Usuario de Oracle |
| ORACLE_PASSWORD | Password de Oracle |
| POSTGRES_HOST | Host de PostgreSQL |
| POSTGRES_PORT | Puerto PostgreSQL (default: 5432) |
| POSTGRES_DATABASE | Base de datos PostgreSQL |
| POSTGRES_USER | Usuario de PostgreSQL |
| POSTGRES_PASSWORD | Password de PostgreSQL |

## Tecnologias

- Python 3.14
- oracledb (Oracle)
- psycopg (PostgreSQL)
- polars (manipulacion de datos)
- pydantic (validacion)
- structlog (logging)
