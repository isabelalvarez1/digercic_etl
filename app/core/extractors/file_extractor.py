import json
import polars as pl
from pathlib import Path
from typing import Any, Dict, List, Optional
from config.logging_config import logger
from core.extractors.base_extractor import BaseExtractor


class FileExtractor(BaseExtractor):
    """
    Extractor para archivos: CSV, Excel, TXT, JSON usando Polars.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.file_path = None

    def connect(self) -> None:
        """Valida que el archivo existe."""
        file_path = self.config.get("file_path")

        if not file_path:
            raise ValueError("[FileExtractor] file_path es requerido")

        self.file_path = Path(file_path)

        if not self.file_path.exists():
            raise FileNotFoundError(f"[FileExtractor] Archivo no encontrado: {file_path}")

        self._connected = True
        logger.info(f"[FileExtractor] Archivo encontrado: {file_path}")

    def extract(self, query: str = None, params: Optional[Dict] = None) -> List[Dict]:
        """
        Extrae datos de un archivo.
        
        Args:
            query: No se usa para archivos (opcional)
            params: Parametros adicionales (encoding, delimiter, etc.)
        """
        if not self._connected:
            self.connect()

        params = params or {}
        extension = self.file_path.suffix.lower()

        try:
            if extension == ".csv":
                return self._extract_csv(params)
            elif extension in [".xlsx", ".xls"]:
                return self._extract_excel(params)
            elif extension == ".txt":
                return self._extract_txt(params)
            elif extension == ".json":
                return self._extract_json(params)
            else:
                raise ValueError(f"[FileExtractor] Formato no soportado: {extension}")

        except Exception as e:
            logger.exception(f"[FileExtractor] Error extrayendo archivo: {e}")
            raise

    def _extract_csv(self, params: Dict) -> List[Dict]:
        """Extrae datos de archivos CSV usando Polars."""
        encoding = params.get("encoding", "utf-8")
        delimiter = params.get("delimiter", ",")
        n_rows = params.get("n_rows")
        columns = params.get("columns")

        df = pl.read_csv(
            self.file_path,
            encoding=encoding,
            separator=delimiter,
            n_rows=n_rows,
            columns=columns,
        )

        logger.info(f"[FileExtractor] CSV: {df.height} filas, {df.width} columnas")

        return df.to_dicts()

    def _extract_excel(self, params: Dict) -> List[Dict]:
        """Extrae datos de archivos Excel usando Polars."""
        sheet_name = params.get("sheet_name", 0)
        n_rows = params.get("n_rows")
        columns = params.get("columns")

        df = pl.read_excel(
            self.file_path,
            sheet_id=sheet_name if isinstance(sheet_name, int) else None,
            sheet_name=sheet_name if isinstance(sheet_name, str) else None,
        )

        # Filtrar columnas si se especifican
        if columns:
            df = df.select(columns)

        # Limitar filas si se especifica
        if n_rows:
            df = df.head(n_rows)

        logger.info(f"[FileExtractor] Excel: {df.height} filas, {df.width} columnas")

        return df.to_dicts()

    def _extract_txt(self, params: Dict) -> List[Dict]:
        """Extrae datos de archivos TXT (delimitados) usando Polars."""
        encoding = params.get("encoding", "utf-8")
        delimiter = params.get("delimiter", "\t")
        n_rows = params.get("n_rows")

        df = pl.read_csv(
            self.file_path,
            encoding=encoding,
            separator=delimiter,
            n_rows=n_rows,
        )

        logger.info(f"[FileExtractor] TXT: {df.height} filas, {df.width} columnas")

        return df.to_dicts()

    def _extract_json(self, params: Dict) -> List[Dict]:
        """Extrae datos de archivos JSON usando Polars."""
        with open(self.file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            logger.info(f"[FileExtractor] JSON: {len(data)} registros")
            return data
        else:
            logger.warning("[FileExtractor] JSON no es una lista")
            return [data]

    def disconnect(self) -> None:
        """Cierra la conexion (no aplica para archivos)."""
        self._connected = False
        logger.info("[FileExtractor] Desconectado")
