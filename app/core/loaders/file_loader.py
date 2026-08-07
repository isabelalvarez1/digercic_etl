import json
import polars as pl
from pathlib import Path
from typing import Any, Dict, List
from config.logging_config import logger
from core.loaders.base_loader import BaseLoader


class FileLoader(BaseLoader):
    """
    Loader para archivos: CSV, Excel, JSON usando Polars.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.output_dir = None

    def connect(self) -> None:
        """Crea el directorio de salida si no existe."""
        output_dir = self.config.get("output_dir", "app/output")
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._connected = True
        logger.info(f"[FileLoader] Directorio de salida: {self.output_dir}")

    def load(self, data: List[Dict], table: str, mode: str = "insert") -> int:
        """
        Guarda datos en un archivo.
        
        Args:
            data: Lista de diccionarios
            table: Nombre del archivo (sin extension)
            mode: No aplica para archivos
        """
        if not self._connected:
            self.connect()

        if not data:
            logger.warning("[FileLoader] No hay datos para guardar")
            return 0

        output_format = self.config.get("format", "csv").lower()
        encoding = self.config.get("encoding", "utf-8")

        try:
            df = pl.DataFrame(data)

            if output_format == "csv":
                filepath = self.output_dir / f"{table}.csv"
                df.write_csv(filepath)
                logger.info(f"[FileLoader] CSV guardado: {filepath}")

            elif output_format == "json":
                filepath = self.output_dir / f"{table}.json"
                df.write_json(filepath)
                logger.info(f"[FileLoader] JSON guardado: {filepath}")

            elif output_format == "parquet":
                filepath = self.output_dir / f"{table}.parquet"
                df.write_parquet(filepath)
                logger.info(f"[FileLoader] Parquet guardado: {filepath}")

            else:
                raise ValueError(f"[FileLoader] Formato no soportado: {output_format}")

            return len(data)

        except Exception as e:
            logger.exception(f"[FileLoader] Error guardando archivo: {e}")
            raise

    def disconnect(self) -> None:
        """Cierra la conexion (no aplica para archivos)."""
        self._connected = False
        logger.info("[FileLoader] Desconectado")
