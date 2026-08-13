import re
import polars as pl
import psycopg
from typing import Any, Dict, List
from datetime import datetime
from config.logging_config import logger, setup_table_logger
from core.loaders.base_loader import BaseLoader
from core.utils import get_system_resources, calculate_optimal_batch_size


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
        replacements = {
            'ñ': 'N', 'Ñ': 'N',
            'á': 'A', 'Á': 'A',
            'é': 'E', 'É': 'E',
            'í': 'I', 'Í': 'I',
            'ó': 'O', 'Ó': 'O',
            'ú': 'U', 'Ú': 'U',
            '\ufffd': '',
        }
        
        result = col
        for old, new in replacements.items():
            result = result.replace(old, new)
        
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

            logger.info(f"[PostgresLoader] Conectando a {host}:{port}/{database}...")
            logger.info(f"[PostgresLoader] Base de datos: {database}")

            conn_string = f"host={host} port={port} dbname={database} user={user} password={password}"

            self.connection = psycopg.connect(conn_string)
            self._connected = True
            logger.info(f"[PostgresLoader] Conexion exitosa a {host}:{port}/{database}")

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

        # Logger especifico para esta tabla
        table_logger = setup_table_logger(table)
        load_start = datetime.now()

        try:
            # Paso 1: Detectar recursos del sistema
            table_logger.info(f"{'='*50}")
            table_logger.info(f"INICIO CARGA: {table}")
            table_logger.info(f"Fecha inicio: {load_start.strftime('%Y-%m-%d %H:%M:%S')}")
            table_logger.info(f"{'-'*50}")
            
            table_logger.info("Paso 1/7: Detectando recursos del sistema...")
            resources = get_system_resources()
            table_logger.info(f"  CPU Cores Logicales: {resources['cpu_cores_logical']}")
            table_logger.info(f"  CPU Cores Fisicos: {resources['cpu_cores_physical']}")
            table_logger.info(f"  Memoria Total: {resources['memory_total_gb']} GB")
            table_logger.info(f"  Memoria Disponible: {resources['memory_available_gb']} GB")
            table_logger.info(f"  Memoria Usada: {resources['memory_percent_used']}%")
            
            # Paso 2: Preparar datos
            table_logger.info("Paso 2/7: Convirtiendo datos a DataFrame...")
            df = pl.DataFrame(data)
            total_rows = df.height
            table_logger.info(f"  Registros: {total_rows}")
            table_logger.info(f"  Columnas originales: {df.width}")
            
            # Paso 3: Normalizar columnas
            table_logger.info("Paso 3/7: Normalizando nombres de columnas...")
            if self.normalize_columns:
                new_columns = {col: self._normalize_column_name(col) for col in df.columns}
                df = df.rename(new_columns)
                table_logger.info(f"  Columnas normalizadas: {list(new_columns.values())}")
            
            # Paso 4: Aplicar mapeo
            table_logger.info("Paso 4/7: Aplicando mapeo de columnas...")
            if self.column_mapping:
                normalized_mapping = {self._normalize_column_name(k): v for k, v in self.column_mapping.items()}
                valid_mapping = {k: v for k, v in normalized_mapping.items() if k in df.columns}
                if valid_mapping:
                    df = df.rename(valid_mapping)
                    table_logger.info(f"  Mapeo aplicado: {valid_mapping}")
                else:
                    table_logger.info("  No hay mapeo para aplicar")
            else:
                table_logger.info("  No hay mapeo configurado")
            
            # Paso 5: Calcular lotes optimos
            table_logger.info("Paso 5/7: Calculando lotes optimos...")
            batch_info = calculate_optimal_batch_size(total_rows, resources)
            
            # Usar batch_size del config si es menor al calculado
            final_batch_size = min(self.batch_size, batch_info["batch_size"])
            final_batch_count = (total_rows + final_batch_size - 1) // final_batch_size
            
            table_logger.info(f"  Batch Size Configurado: {self.batch_size}")
            table_logger.info(f"  Batch Size Optimizado: {batch_info['batch_size']}")
            table_logger.info(f"  Batch Size Final: {final_batch_size}")
            table_logger.info(f"  Total Lotes: {final_batch_count}")
            table_logger.info(f"  Registros por Core: {batch_info['rows_per_core']}")
            table_logger.info(f"  Memoria Estimada por Lote: {batch_info['estimated_memory_mb']} MB")
            
            # Paso 6: Truncar tabla
            table_logger.info("Paso 6/7: Preparando tabla destino...")
            cursor = self.connection.cursor()
            
            if self.truncate_before_load:
                cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
                self.connection.commit()
                table_logger.info(f"  Tabla {table} truncada")
            else:
                table_logger.info(f"  Modo: Insertar sin truncar")
            
            # Paso 7: Insertar por lotes
            table_logger.info("Paso 7/7: Insertando datos por lotes...")
            table_logger.info(f"{'-'*50}")
            
            columns = df.columns
            col_names = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
            
            total_inserted = 0
            
            for i in range(final_batch_count):
                batch_start = datetime.now()
                start_idx = i * final_batch_size
                end_idx = min((i + 1) * final_batch_size, df.height)
                
                batch_df = df.slice(start_idx, end_idx - start_idx)
                batch_data = batch_df.to_dicts()
                
                for row in batch_data:
                    values = tuple(row[col] for col in columns)
                    cursor.execute(query, values)
                
                self.connection.commit()
                total_inserted += len(batch_data)
                
                batch_end = datetime.now()
                batch_duration = (batch_end - batch_start).total_seconds()
                
                # Calcular porcentaje completado
                percent_complete = (total_inserted / total_rows) * 100 if total_rows > 0 else 100
                
                table_logger.info(f"  CHUNK {i+1}/{final_batch_count}")
                table_logger.info(f"    Registros: {len(batch_data)}")
                table_logger.info(f"    Chunk Size: {final_batch_size}")
                table_logger.info(f"    Offset: {start_idx}")
                table_logger.info(f"    Tiempo: {batch_duration:.2f}s")
                table_logger.info(f"    Progreso: {percent_complete:.1f}%")
                table_logger.info(f"    Acumulado: {total_inserted}/{total_rows}")
            
            cursor.close()
            
            # Resumen de carga
            load_end = datetime.now()
            load_duration = (load_end - load_start).total_seconds()
            
            table_logger.info(f"{'-'*50}")
            table_logger.info(f"CARGA COMPLETADA")
            table_logger.info(f"  Registros insertados: {total_inserted}")
            table_logger.info(f"  Chunks procesados: {final_batch_count}")
            table_logger.info(f"  Chunk Size utilizado: {final_batch_size}")
            table_logger.info(f"  Tiempo total: {load_duration:.2f} segundos")
            table_logger.info(f"  Velocidad: {total_inserted/load_duration:.0f} registros/segundo" if load_duration > 0 else "  Velocidad: N/A")
            table_logger.info(f"  CPU Cores utilizados: {resources['cpu_cores_logical']}")
            table_logger.info(f"  Memoria disponible: {resources['memory_available_gb']} GB")
            table_logger.info(f"Fecha fin: {load_end.strftime('%Y-%m-%d %H:%M:%S')}")
            table_logger.info(f"{'='*50}")
            
            return total_inserted

        except Exception as e:
            load_end = datetime.now()
            load_duration = (load_end - load_start).total_seconds()
            
            table_logger.error(f"{'-'*50}")
            table_logger.error(f"ERROR EN CARGA")
            table_logger.error(f"  Error: {str(e)}")
            table_logger.error(f"  Tiempo hasta error: {load_duration:.2f} segundos")
            table_logger.error(f"{'='*50}")
            
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
