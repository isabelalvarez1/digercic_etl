import logging
import os
from pathlib import Path
from datetime import datetime


def setup_table_logger(table_name: str) -> logging.Logger:
    """
    Crea un logger separado para cada tabla.
    
    Genera archivos como: logs/clientes_2026-08-13.log
    
    Args:
        table_name: Nombre de la tabla (ej: "clientes", "captaciones")
        
    Returns:
        Logger configurado con archivo propio
    """
    # Crear carpeta logs si no existe (ruta relativa al proyecto raiz)
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Nombre del archivo con fecha
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"{table_name}_{today}.log"
    
    # Crear logger unico para esta tabla
    logger_name = f"table_{table_name}"
    table_logger = logging.getLogger(logger_name)
    
    # Evitar duplicar handlers si ya existe
    if not table_logger.handlers:
        table_logger.setLevel(logging.INFO)
        table_logger.propagate = False  # No enviar al logger principal
        
        # Formato detallado
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Handler para archivo
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        table_logger.addHandler(file_handler)
        
        # Handler para consola
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        table_logger.addHandler(stream_handler)
    
    return table_logger


# Logger general del pipeline
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("DIGERCIC_ETL")
