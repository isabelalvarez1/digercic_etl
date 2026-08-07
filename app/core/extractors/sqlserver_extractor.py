import pyodbc
from typing import Any, Dict, List, Optional
from config.logging_config import logger
from core.extractors.base_extractor import BaseExtractor


class SqlServerExtractor(BaseExtractor):
    """
    Extractor para bases de datos SQL Server.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def connect(self) -> None:
        """Establece conexion con SQL Server."""
        if self._connected:
            return

        try:
            host = self.config.get("host")
            port = self.config.get("port", 1433)
            database = self.config.get("database")
            user = self.config.get("user")
            password = self.config.get("password")
            driver = self.config.get("driver", "ODBC Driver 17 for SQL Server")

            conn_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
            )

            self.connection = pyodbc.connect(conn_string)
            self._connected = True
            logger.info(f"[SqlServerExtractor] Conectado a {host}:{port}/{database}")

        except Exception as e:
            logger.exception(f"[SqlServerExtractor] Error de conexion: {e}")
            raise

    def extract(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Extrae datos de SQL Server."""
        if not self._connected:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or {})
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            logger.info(f"[SqlServerExtractor] Extraidas {len(rows)} filas")

            return [dict(zip(columns, row)) for row in rows]

        finally:
            cursor.close()

    def disconnect(self) -> None:
        """Cierra la conexion."""
        if self.connection:
            self.connection.close()
            self._connected = False
            logger.info("[SqlServerExtractor] Desconectado")
