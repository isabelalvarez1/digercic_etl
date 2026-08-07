import psycopg
from typing import Any, Dict, List, Optional
from config.logging_config import logger
from core.extractors.base_extractor import BaseExtractor


class PostgresExtractor(BaseExtractor):
    """
    Extractor para bases de datos PostgreSQL.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def connect(self) -> None:
        """Establece conexion con PostgreSQL."""
        if self._connected:
            return

        try:
            host = self.config.get("host")
            port = self.config.get("port", 5432)
            database = self.config.get("database")
            user = self.config.get("user")
            password = self.config.get("password")

            conn_string = f"host={host} port={port} dbname={database} user={user} password={password}"

            self.connection = psycopg.connect(conn_string)
            self._connected = True
            logger.info(f"[PostgresExtractor] Conectado a {host}:{port}/{database}")

        except Exception as e:
            logger.exception(f"[PostgresExtractor] Error de conexion: {e}")
            raise

    def extract(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Extrae datos de PostgreSQL."""
        if not self._connected:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or {})
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            logger.info(f"[PostgresExtractor] Extraidas {len(rows)} filas")

            return [dict(zip(columns, row)) for row in rows]

        finally:
            cursor.close()

    def disconnect(self) -> None:
        """Cierra la conexion."""
        if self.connection:
            self.connection.close()
            self._connected = False
            logger.info("[PostgresExtractor] Desconectado")
