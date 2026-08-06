import yaml
from pathlib import Path
from config.logging_config import logger


class ConfigLoader:
    """
    Carga la configuracion del pipeline desde un archivo YAML.
    """

    def __init__(self, config_path: str = None):

        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "pipeline.yaml"

        self.config_path = Path(config_path)
        self.config = None

    def load(self) -> dict:

        if not self.config_path.exists():
            logger.error(f"Archivo de configuracion no encontrado: {self.config_path}")
            raise FileNotFoundError(f"Config no encontrada: {self.config_path}")

        try:

            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)

            logger.info(f"Configuracion cargada desde: {self.config_path}")

            return self.config

        except yaml.YAMLError as e:
            logger.exception(f"Error parseando YAML: {e}")
            raise

    def get(self, key: str, default=None):

        if self.config is None:
            self.load()

        keys = key.split(".")
        value = self.config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default

        return value

    def validate(self) -> bool:

        required_sections = ["pipeline", "extract", "transform", "load"]

        for section in required_sections:
            if section not in self.config:
                logger.error(f"Seccion obligatoria no encontrada: {section}")
                return False

        return True
