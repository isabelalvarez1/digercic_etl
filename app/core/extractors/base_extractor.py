from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
from config.logging_config import logger


class BaseExtractor(ABC):
    """
    Clase base abstracta para todos los extractores.
    
    Cada extractor debe implementar:
    - connect(): Establecer conexion con la fuente
    - extract(): Extraer datos
    - disconnect(): Cerrar conexion
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self._connected = False
        self._stats = {
            "start_time": None,
            "end_time": None,
            "rows_extracted": 0,
            "errors": []
        }

    @abstractmethod
    def connect(self) -> None:
        """Establece conexion con la fuente de datos."""
        pass

    @abstractmethod
    def extract(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Extrae datos de la fuente.
        
        Args:
            query: Consulta a ejecutar (SQL, ruta de archivo, etc.)
            params: Parametros adicionales
            
        Returns:
            Lista de diccionarios con los datos
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Cierra la conexion con la fuente."""
        pass

    def execute(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Ejecuta el proceso completo de extraccion.
        
        Returns:
            Lista de diccionarios con los datos extraidos
        """
        logger.info(f"[{self.__class__.__name__}] Iniciando extraccion...")
        self._stats["start_time"] = datetime.now()

        try:
            self.connect()
            data = self.extract(query, params)
            self._stats["rows_extracted"] = len(data)
            return data

        except Exception as e:
            self._stats["errors"].append(str(e))
            logger.exception(f"[{self.__class__.__name__}] Error en extraccion: {e}")
            raise

        finally:
            self._stats["end_time"] = datetime.now()
            elapsed = (self._stats["end_time"] - self._stats["start_time"]).seconds
            logger.info(
                f"[{self.__class__.__name__}] Extraccion completada. "
                f"Filas: {self._stats['rows_extracted']} | "
                f"Tiempo: {elapsed}s"
            )
            self.disconnect()

    @property
    def stats(self) -> Dict:
        """Retorna estadisticas de la extraccion."""
        return self._stats.copy()
