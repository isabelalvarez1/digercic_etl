import re
import polars as pl
import psycopg
from typing import Any, Dict, List
from config.logging_config import logger
from core.loaders.base_loader import BaseLoader


class PostgresLoader(BaseLoader):
    """
    Loader para PostgreSQL con soporte para lotes usando Polars.
    
    Optimizado para tablas grandes (millones de registros).
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.batch_size = config.get("batch_size", 5000)
        self.column_mapping = config.get("column_mapping", {})
        self.normalize_columns = config.get("normalize_columns", True)
        self.truncate_before_load = config.get("truncate_before_load", False)

    def _normalize_column_name(self, col: str) -> str:
        """
        Normaliza el nombre de columna:
        - Convierte a mayusculas
        - Elimina caracteres no ASCII (ñ, acentos, etc.)
        """
        # Primero intentar reemplazar caracteres conocidos
        replacements = {
            'ñ': 'N', 'Ñ': 'N',
            'á': 'A', 'Á': 'A',
            'é': 'E', 'É': 'E',
            'í': 'I', 'Í': 'I',
            'ó': 'O', 'Ó': 'O',
            'ú': 'U', 'Ú': 'U',
            '\ufffd': '',  # Caracter de reemplazo Unicode
        }
        
        result = col
        for old, new in replacements.items():
            result = result.replace(old, new)
        
        # Eliminar cualquier caracter no ASCII restante
        result = re.sub(r'[^\x00-\x7F]', '', result)
        
        return result.upper()

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
            logger.info(f"[PostgresLoader] Conectado a {host}:{port}/{database}")

        except Exception as e:
            logger.exception(f"[PostgresLoader] Error de conexion: {e}")
            raise

    def load(self, data: List[Dict], table: str, mode: str = "insert") -> int:
        """
        Carga datos en PostgreSQL usando Polars con lotes.
        """
        if not self._connected:
            self.connect()

        if not data:
            logger.warning("[PostgresLoader] No hay datos para cargar")
            return 0

        try:
            # Convertir a DataFrame de Polars
            df = pl.DataFrame(data)
            
            # Normalizar nombres de columnas si esta habilitado
            if self.normalize_columns:
                new_columns = {col: self._normalize_column_name(col) for col in df.columns}
                df = df.rename(new_columns)
                logger.info(f"[PostgresLoader] Columnas normalizadas: {list(new_columns.values())}")
            
            # Aplicar mapeo de columnas adicional si existe
            if self.column_mapping:
                # Normalizar claves del mapping para que coincidan con columnas normalizadas
                normalized_mapping = {self._normalize_column_name(k): v for k, v in self.column_mapping.items()}
                # Solo renombrar columnas que existan en el DataFrame
                valid_mapping = {k: v for k, v in normalized_mapping.items() if k in df.columns}
                if valid_mapping:
                    df = df.rename(valid_mapping)
                    logger.info(f"[PostgresLoader] Mapeo aplicado: {valid_mapping}")
            
            logger.info(f"[PostgresLoader] DataFrame: {df.height} filas, {df.width} columnas")
            
            cursor = self.connection.cursor()
            
            # Truncar tabla si esta habilitado
            if self.truncate_before_load:
                cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
                self.connection.commit()
                logger.info(f"[PostgresLoader] Tabla {table} truncada")
            
            # Obtener nombres de columnas
            columns = df.columns
            col_names = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            
            cursor = self.connection.cursor()
            
            # Insertar por lotes
            total_inserted = 0
            batch_count = (df.height + self.batch_size - 1) // self.batch_size
            
            for i in range(batch_count):
                start_idx = i * self.batch_size
                end_idx = min((i + 1) * self.batch_size, df.height)
                
                batch_df = df.slice(start_idx, end_idx - start_idx)
                batch_data = batch_df.to_dicts()
                
                query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
                
                for row in batch_data:
                    values = tuple(row[col] for col in columns)
                    cursor.execute(query, values)
                
                total_inserted += len(batch_data)
                logger.info(f"[PostgresLoader] Lote {i+1}/{batch_count}: {len(batch_data)} registros")
            
            self.connection.commit()
            cursor.close()
            
            logger.info(f"[PostgresLoader] Carga completada: {total_inserted} registros en {table}")
            
            return total_inserted

        except Exception as e:
            self.connection.rollback()
            logger.exception(f"[PostgresLoader] Error en carga: {e}")
            raise

    def load_from_polars(self, df: pl.DataFrame, table: str, mode: str = "insert") -> int:
        """
        Carga directamente desde un DataFrame de Polars.
        """
        return self.load(df.to_dicts(), table, mode)

    def disconnect(self) -> None:
        """Cierra la conexion."""
        if self.connection:
            self.connection.close()
            self._connected = False
            logger.info("[PostgresLoader] Desconectado")
