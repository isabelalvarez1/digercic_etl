#!/usr/bin/env python3
"""
Check de conexiones para Airflow.
Valida que Oracle y Postgres estén accesibles antes de lanzar el ETL.
Usado por el task check_connections del DAG.
"""
import sys
from pathlib import Path

# Agregar app al path
app_dir = Path(__file__).parent.parent / "app"
sys.path.insert(0, str(app_dir))

import os
import re
import yaml
from dotenv import load_dotenv
from config.logging_config import logger

def _resolve_env_vars(config):
    if isinstance(config, str):
        return re.sub(r'\$\{(\w+)\}', lambda m: os.getenv(m.group(1), m.group(0)), config)
    elif isinstance(config, dict):
        return {k: _resolve_env_vars(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_resolve_env_vars(i) for i in config]
    return config

def check_connections(config_path="config/pipeline.yaml", env_path=".env"):
    load_dotenv(env_path, override=True)
    logger.info("="*60)
    logger.info("[CHECK] Validando conexiones Oracle y PostgreSQL...")
    logger.info(f"[CHECK] .env cargado desde: {Path(env_path).resolve()} - ORACLE_HOST_MSP={os.getenv('ORACLE_HOST_MSP')}")
    logger.info("="*60)

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    config = _resolve_env_vars(config)

    from core.factory import ExtractorFactory, LoaderFactory

    extractions = config.get("extractions", [])
    loads = config.get("loads", [])
    errors = []

    # Probar cada extracción Oracle
    for ext in extractions:
        name = ext.get("name")
        try:
            extractor = ExtractorFactory.create(ext.get("source"), ext.get("config", {}))
            extractor.connect()
            # Test simple: obtener versión
            extractor._detect_oracle_version()
            logger.info(f"[CHECK] Oracle OK - {name}: {ext.get('config', {}).get('host')}:{ext.get('config', {}).get('port')}")
            extractor.disconnect()
        except Exception as e:
            logger.error(f"[CHECK] Oracle FAILED - {name}: {e}")
            errors.append(f"Oracle {name}: {e}")

    # Probar cada carga Postgres
    for load in loads:
        name = load.get("name")
        try:
            loader = LoaderFactory.create(load.get("target"), load.get("config", {}))
            loader.connect()
            logger.info(f"[CHECK] PostgreSQL OK - {name}: {load.get('config', {}).get('host')}:{load.get('config', {}).get('port')}")
            loader.disconnect()
        except Exception as e:
            logger.error(f"[CHECK] PostgreSQL FAILED - {name}: {e}")
            errors.append(f"Postgres {name}: {e}")

    if errors:
        logger.error("="*60)
        logger.error("[CHECK] FALLO - Conexiones con error:")
        for err in errors:
            logger.error(f"  - {err}")
        logger.error("="*60)
        print("AIRFLOW_CHECK_FAILED", file=sys.stderr, flush=True)
        sys.exit(1)

    logger.info("="*60)
    logger.info("[CHECK] TODAS LAS CONEXIONES OK - Listo para ETL")
    logger.info("="*60)
    print("AIRFLOW_CHECK_OK", flush=True)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Check conexiones ETL")
    parser.add_argument("--config", default="config/pipeline.yaml")
    parser.add_argument("--env", default=".env")
    args = parser.parse_args()
    check_connections(args.config, args.env)
