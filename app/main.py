import yaml
import os
from pathlib import Path
from dotenv import load_dotenv
from config.logging_config import logger
from core.pipeline_manager import PipelineManager


def main():
    """
    Punto de entrada principal para el pipeline ETL.
    
    Ejecuta el pipeline configurado en config/pipeline.yaml
    """
    logger.info("=" * 50)
    logger.info("ETL DIGERCIC - MULTI-FUENTE")
    logger.info("=" * 50)

    try:
        # Cargar variables de entorno
        env_path = Path(__file__).parent.parent / ".env"
        load_dotenv(env_path)
        logger.info(f"Variables de entorno cargadas desde: {env_path}")

        # Cargar configuracion
        config_path = Path(__file__).parent.parent / "config" / "pipeline.yaml"

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        logger.info(f"Pipeline: {config.get('pipeline', {}).get('name', 'desconocido')}")
        logger.info(f"Version: {config.get('pipeline', {}).get('version', '1.0')}")

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
    main()
