import logging
from pathlib import Path

# Crear carpeta logs si no existe
log_path = Path("app/logs")
log_path.mkdir(parents=True, exist_ok=True)

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("app/logs/digercic_etl.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("DIGERCIC_ETL")