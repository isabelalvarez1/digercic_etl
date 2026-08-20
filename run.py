import sys
import argparse
from pathlib import Path

# Agregar carpeta app al path
app_dir = Path(__file__).parent / "app"
sys.path.insert(0, str(app_dir))

import yaml
from dotenv import load_dotenv
from config.logging_config import logger
from core.pipeline_manager import PipelineManager


def main(config_path=None, env_path=None):
    """
    Punto de entrada principal para el pipeline ETL.
    """
    logger.info("=" * 50)
    logger.info("ETL DIGERCIC - MULTI-FUENTE ORACLE")
    logger.info("=" * 50)

    try:
        # Cargar variables de entorno
        if env_path is None:
            env_path = str(Path(__file__).parent / ".env")
        load_dotenv(env_path)
        logger.info(f"Variables de entorno cargadas desde: {env_path}")

        # Cargar configuracion
        if config_path is None:
            config_path = str(Path(__file__).parent / "config" / "pipeline.yaml")

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        logger.info(f"Pipeline: {config.get('pipeline', {}).get('name', 'desconocido')}")
        logger.info(f"Version: {config.get('pipeline', {}).get('version', '1.0')}")
        logger.info(f"Modo paralelo: {config.get('pipeline', {}).get('parallel', False)}")

        # Ejecutar pipeline
        manager = PipelineManager(config)
        result = manager.run()

        # Resumen
        logger.info("=" * 50)
        logger.info("RESUMEN DEL PIPELINE")
        logger.info("=" * 50)
        logger.info(f"Estado: {result.get('status', 'desconocido')}")
        logger.info(f"Tiempo: {result.get('elapsed_seconds', 0)}s")
        logger.info(f"Extracciones: {result.get('extractions', {})}")
        logger.info(f"Cargas: {result.get('loads', {})}")
        logger.info("=" * 50)

    except Exception as e:
        logger.exception(f"Error en pipeline principal: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL DIGERCIC - Multi-fuente Oracle")
    parser.add_argument(
        "--config", "-c",
        default=None,
        help="Ruta al archivo YAML de configuracion (default: config/pipeline.yaml)"
    )
    parser.add_argument(
        "--env", "-e",
        default=None,
        help="Ruta al archivo .env (default: .env)"
    )
    args = parser.parse_args()
    main(args.config, args.env)
