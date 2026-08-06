from dotenv import load_dotenv
import os

# Carga las variables del archivo .env
load_dotenv()

# ==========================
# ORACLE
# ==========================

ORACLE_HOST = os.getenv("ORACLE_HOST")
ORACLE_PORT = os.getenv("ORACLE_PORT")
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE")
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")

# ==========================
# POSTGRESQL
# ==========================

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")