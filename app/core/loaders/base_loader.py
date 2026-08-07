from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
from config.logging_config import logger


class BaseLoader(ABC):
    """
    Clase base abstracta para todos los loaders.
    
    Cada loader debe implementar:
    - connect(): Establecer conexion con el destino
    - load(): Cargar datos
    - disconnect(): Cerrar conexion
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self._connected = False
        self._stats = {
            "start_time": None,
            "end_time": None,
            "rows_loaded": 0,
            "errors": []
        }

    @abstractmethod
    def connect(self) -> None:
        """Establece conexion con el destino."""
        pass

    @abstractmethod
    def load(self, data: List[Dict], table: str, mode: str = "insert") -> int:
        """
        Carga datos en el destino.
        
        Args:
            data: Lista de diccionarios con los datos
            table: Nombre de la tabla destino
            mode: Modo de carga (insert, upsert, replace)
            
        Returns:
            Numero de filas cargadas
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Cierra la conexion con el destino."""
        pass

    def execute(self, data: List[Dict], table: str, mode: str = "insert") -> int:
        """
        Ejecuta el proceso completo de carga.
        
        Returns:
            Numero de filas cargadas
        """
        logger.info(f"[{self.__class__.__name__}] Iniciando carga...")
        self._stats["start_time"] = datetime.now()

        try:
            self.connect()
            rows_loaded = self.load(data, table, mode)
            self._stats["rows_loaded"] = rows_loaded
            return rows_loaded

        except Exception as e:
            self._stats["errors"].append(str(e))
            logger.exception(f"[{self.__class__.__name__}] Error en carga: {e}")
            raise

        finally:
            self._stats["end_time"] = datetime.now()
            elapsed = (self._stats["end_time"] - self._stats["start_time"]).seconds
            logger.info(
                f"[{self.__class__.__name__}] Carga completada. "
                f"Filas: {self._stats['rows_loaded']} | "
                f"Tiempo: {elapsed}s"
            )
            self.disconnect()

    @property
    def stats(self) -> Dict:
        """Retorna estadisticas de la carga."""
        return self._stats.copy()
