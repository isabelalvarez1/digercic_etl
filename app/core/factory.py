from typing import Any, Dict, Type
from config.logging_config import logger

# Extractors
from core.extractors.base_extractor import BaseExtractor
from core.extractors.oracle_extractor import OracleExtractor
from core.extractors.postgres_extractor import PostgresExtractor
from core.extractors.sqlserver_extractor import SqlServerExtractor
from core.extractors.file_extractor import FileExtractor

# Loaders
from core.loaders.base_loader import BaseLoader
from core.loaders.postgres_loader import PostgresLoader
from core.loaders.sqlserver_loader import SqlServerLoader
from core.loaders.file_loader import FileLoader


class ExtractorFactory:
    """
    Factory para crear extractores segun el tipo de fuente.
    
    Tipos soportados:
    - oracle: Oracle Database
    - postgresql: PostgreSQL
    - sqlserver: SQL Server
    - csv: Archivos CSV
    - excel: Archivos Excel (.xlsx, .xls)
    - txt: Archivos TXT delimitados
    - json: Archivos JSON
    """

    _extractors: Dict[str, Type[BaseExtractor]] = {
        "oracle": OracleExtractor,
        "postgresql": PostgresExtractor,
        "postgres": PostgresExtractor,
        "sqlserver": SqlServerExtractor,
        "csv": FileExtractor,
        "excel": FileExtractor,
        "txt": FileExtractor,
        "json": FileExtractor,
        "file": FileExtractor,
    }

    @classmethod
    def create(cls, source_type: str, config: Dict[str, Any]) -> BaseExtractor:
        """
        Crea un extractor segun el tipo de fuente.
        
        Args:
            source_type: Tipo de fuente (oracle, postgresql, csv, etc.)
            config: Configuracion de la conexion
            
        Returns:
            Instancia del extractor correspondiente
            
        Raises:
            ValueError: Si el tipo de fuente no es soportado
        """
        source_lower = source_type.lower()

        if source_lower not in cls._extractors:
            supported = ", ".join(cls._extractors.keys())
            raise ValueError(
                f"Tipo de fuente no soportado: {source_type}. "
                f"Tipos validos: {supported}"
            )

        extractor_class = cls._extractors[source_lower]
        logger.info(f"[ExtractorFactory] Creando extractor: {source_lower}")

        return extractor_class(config)

    @classmethod
    def register(cls, source_type: str, extractor_class: Type[BaseExtractor]) -> None:
        """Registra un nuevo tipo de extractor."""
        cls._extractors[source_type.lower()] = extractor_class
        logger.info(f"[ExtractorFactory]Extractor registrado: {source_type}")

    @classmethod
    def get_supported_sources(cls) -> list:
        """Retorna la lista de fuentes soportadas."""
        return list(cls._extractors.keys())


class LoaderFactory:
    """
    Factory para crear loaders segun el tipo de destino.
    
    Tipos soportados:
    - postgresql: PostgreSQL
    - sqlserver: SQL Server
    - csv: Archivos CSV
    - excel: Archivos Excel
    - json: Archivos JSON
    """

    _loaders: Dict[str, Type[BaseLoader]] = {
        "postgresql": PostgresLoader,
        "postgres": PostgresLoader,
        "sqlserver": SqlServerLoader,
        "csv": FileLoader,
        "excel": FileLoader,
        "json": FileLoader,
        "file": FileLoader,
    }

    @classmethod
    def create(cls, target_type: str, config: Dict[str, Any]) -> BaseLoader:
        """
        Crea un loader segun el tipo de destino.
        
        Args:
            target_type: Tipo de destino (postgresql, sqlserver, csv, etc.)
            config: Configuracion del destino
            
        Returns:
            Instancia del loader correspondiente
        """
        target_lower = target_type.lower()

        if target_lower not in cls._loaders:
            supported = ", ".join(cls._loaders.keys())
            raise ValueError(
                f"Tipo de destino no soportado: {target_type}. "
                f"Tipos validos: {supported}"
            )

        loader_class = cls._loaders[target_lower]
        logger.info(f"[LoaderFactory] Creando loader: {target_lower}")

        return loader_class(config)

    @classmethod
    def register(cls, target_type: str, loader_class: Type[BaseLoader]) -> None:
        """Registra un nuevo tipo de loader."""
        cls._loaders[target_type.lower()] = loader_class
        logger.info(f"[LoaderFactory] Loader registrado: {target_type}")

    @classmethod
    def get_supported_targets(cls) -> list:
        """Retorna la lista de destinos soportados."""
        return list(cls._loaders.keys())
