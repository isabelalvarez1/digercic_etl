import oracledb
from typing import Any, Dict, List, Optional
from config.logging_config import logger
from core.extractors.base_extractor import BaseExtractor


class OracleExtractor(BaseExtractor):
    """
    Extractor para bases de datos Oracle.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.pool = None

    def connect(self) -> None:
        """Establece conexion con Oracle usando pool de conexiones."""
        if self._connected:
            return

        try:
            host = self.config.get("host")
            port = self.config.get("port", 1521)
            service = self.config.get("service")
            user = self.config.get("user")
            password = self.config.get("password")

            dsn = f"{host}:{port}/{service}"

            self.pool = oracledb.create_pool(
                user=user,
                password=password,
                dsn=dsn,
                min=self.config.get("min_connections", 2),
                max=self.config.get("max_connections", 10),
                increment=self.config.get("increment", 2),
            )

            self._connected = True
            logger.info(f"[OracleExtractor] Conectado a {host}:{port}/{service}")

        except Exception as e:
            logger.exception(f"[OracleExtractor] Error de conexion: {e}")
            raise

    def extract(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Extrae datos de Oracle."""
        if not self._connected:
            self.connect()

        if params is None:
            params = {}

        connection = self.pool.acquire()

        try:
            cursor = connection.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            logger.info(f"[OracleExtractor] Extraidas {len(rows)} filas")

            return [dict(zip(columns, row)) for row in rows]

        finally:
            connection.close()

    def disconnect(self) -> None:
        """Cierra el pool de conexiones."""
        if self.pool:
            self.pool.close()
            self._connected = False
            logger.info("[OracleExtractor] Desconectado")
