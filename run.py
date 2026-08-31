import sys
import argparse
from pathlib import Path

# Verificar virtual environment
venv_path = Path(__file__).parent / ".venv" / "Scripts" / "python.exe"
if not venv_path.exists():
    venv_path = Path(__file__).parent / ".venv" / "bin" / "python"

in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
if not in_venv:
    print("ADVERTENCIA: No estas usando el virtual environment.")
    print("Ejecuta: .venv\\Scripts\\activate  y luego  python run.py")
    print("O usa:    .venv\\Scripts\\python.exe run.py")
    print("")

# Agregar carpeta app al path
app_dir = Path(__file__).parent / "app"
sys.path.insert(0, str(app_dir))

import yaml
from dotenv import load_dotenv
from config.logging_config import logger
from core.pipeline_manager import PipelineManager


def main(config_path=None, env_path=None, table_filter=None):
    """
    Punto de entrada principal para el pipeline ETL.
    
    Args:
        config_path: Ruta al archivo YAML de configuracion
        env_path: Ruta al archivo .env
        table_filter: Nombre de la tabla a procesar (None = todas)
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

        # Filtrar por tabla especifica si se solicita
        if table_filter:
            config = _filter_config_by_table(config, table_filter)
            logger.info(f"Filtro de tabla activo: {table_filter}")

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


def _filter_config_by_table(config, table_name):
    """Filtra la configuracion para procesar solo una tabla especifica."""
    config = config.copy()
    
    # Buscar la extraccion que coincida con el nombre
    extractions = config.get("extractions", [])
    config["extractions"] = [e for e in extractions if e.get("name") == table_name]
    
    # Buscar la carga que coincida con el nombre de la extraccion
    loads = config.get("loads", [])
    config["loads"] = [l for l in loads if l.get("source") == table_name]
    
    if not config["extractions"]:
        raise ValueError(f"No se encontro extraccion con nombre: {table_name}")
    
    return config


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
    parser.add_argument(
        "--table", "-t",
        default=None,
        help="Nombre de la tabla a procesar (ej: cedulados_msp). Si no se especifica, procesa todas."
    )
    args = parser.parse_args()
    main(args.config, args.env, args.table)
