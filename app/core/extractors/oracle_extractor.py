import polars as pl
import oracledb
from typing import Any, Dict, List, Optional
from config.logging_config import logger
from core.extractors.base_extractor import BaseExtractor


class OracleExtractor(BaseExtractor):
    """
    Extractor para Oracle con soporte para lotes usando Polars.
    
    Optimizado para tablas grandes (millones de registros).
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.batch_size = config.get("batch_size", 50000)

    def connect(self) -> None:
        """Establece conexion con Oracle."""
        if self._connected:
            return

        try:
            host = self.config.get("host")
            port = self.config.get("port", 1521)
            service = self.config.get("service")
            user = self.config.get("user")
            password = self.config.get("password")

            dsn = f"{host}:{port}/{service}"

            logger.info(f"[OracleExtractor] Conectando a {dsn}...")

            self.connection = oracledb.connect(
                user=user,
                password=password,
                dsn=dsn,
            )

            self._connected = True
            logger.info(f"[OracleExtractor] Conectado a {host}:{port}/{service}")

        except Exception as e:
            logger.exception(f"[OracleExtractor] Error de conexion: {e}")
            raise

    def extract(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Extrae datos de Oracle usando Polars con lotes.
        
        Para tablas grandes, usa FETCH FIRST n ROWS ONLY para procesar por lotes.
        """
        if not self._connected:
            self.connect()

        if params is None:
            params = {}

        try:
            cursor = self.connection.cursor()
            
            # Obtener columnas
            cursor.execute(query + " WHERE ROWNUM <= 1")
            columns = [desc[0] for desc in cursor.description]
            
            # Contar total de registros
            count_query = f"SELECT COUNT(*) FROM ({query})"
            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]
            logger.info(f"[OracleExtractor] Total registros a extraer: {total_rows}")
            
            # Extraer por lotes
            all_data = []
            offset = 0
            
            while offset < total_rows:
                batch_query = f"{query} OFFSET {offset} ROWS FETCH NEXT {self.batch_size} ROWS ONLY"
                cursor.execute(batch_query, params)
                rows = cursor.fetchall()
                
                if not rows:
                    break
                
                # Convertir a lista de diccionarios
                batch_data = [dict(zip(columns, row)) for row in rows]
                all_data.extend(batch_data)
                
                offset += self.batch_size
                logger.info(f"[OracleExtractor] Procesados {min(offset, total_rows)}/{total_rows} registros")
            
            cursor.close()
            
            logger.info(f"[OracleExtractor] Extraccion completada: {len(all_data)} registros")
            
            return all_data

        except Exception as e:
            logger.exception(f"[OracleExtractor] Error en extraccion: {e}")
            raise

    def extract_to_polars(self, query: str, params: Optional[Dict] = None) -> pl.DataFrame:
        """
        Extrae datos directamente a un DataFrame de Polars.
        
        Mas eficiente para procesamiento posterior.
        """
        data = self.extract(query, params)
        return pl.DataFrame(data)

    def disconnect(self) -> None:
        """Cierra la conexion."""
        if self.connection:
            self.connection.close()
            self._connected = False
            logger.info("[OracleExtractor] Desconectado")
