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
        self.column_mapping = config.get("column_mapping", {})  # Solo para renombrar explicitamente
        self.truncate_before_load = config.get("truncate_before_load", False)

    def _standardize_column_name(self, col: str) -> str:
        """
        Estandariza el nombre de columna automaticamente.
        
        Reglas:
        1. Eliminar espacios al inicio y final
        2. Eliminar caracteres de relleno (\ufffd)
        3. Detectar patrones de encoding roto (ej: CAMP\ufffd\ufffdA → campana)
        4. Reemplazar espacios multiples por guion bajo
        5. Eliminar caracteres no ASCII (ñ, acentos, etc.)
        6. Convertir a minusculas
        
        Ejemplos:
            " ID_CLIENTE " → id_cliente
            "NOMBRE COMPLETO" → nombre_completo
            CAMPA\ufffd\ufffdA → campana
            CAMPAÑA → campana
            NOMBRE_CIUDAD → nombre_ciudad
        """
        import unicodedata
        
        # Paso 1: Eliminar espacios al inicio y final
        result = col.strip()
        
        # Paso 2: Detectar patrones de encoding roto de Oracle
        # Oracle a veces retorna "CAMPA\ufffd\ufffdA" cuando deberia ser "CAMPAÑA"
        if '\ufffd' in result:
            result = result.replace('\ufffd\ufffd', 'ñ')
            result = result.replace('\ufffd', 'ñ')
        
        # Paso 3: Reemplazar acentos y ñ
        replacements = {
            'ñ': 'n', 'Ñ': 'n',
            'á': 'a', 'Á': 'a',
            'é': 'e', 'É': 'e',
            'í': 'i', 'Í': 'i',
            'ó': 'o', 'Ó': 'o',
            'ú': 'u', 'Ú': 'u',
        }
        for old, new in replacements.items():
            result = result.replace(old, new)
        
        # Paso 4: Reemplazar espacios multiples por guion bajo
        # "NOMBRE COMPLETO" → "nombre_completo"
        result = re.sub(r'\s+', '_', result)
        
        # Paso 5: Eliminar caracteres no ASCII restantes
        result = re.sub(r'[^\x00-\x7F]', '', result)
        
        # Paso 6: Convertir a minusculas
        return result.lower()

    def _create_table(self, cursor, table: str, df) -> None:
        """
        Crea tabla en PostgreSQL basada en la estructura del DataFrame.
        Mapea tipos de Polars a tipos de PostgreSQL.
        """
        # Mapeo de tipos de Polars a PostgreSQL
        type_mapping = {
            'Int8': 'SMALLINT',
            'Int16': 'SMALLINT',
            'Int32': 'INTEGER',
            'Int64': 'BIGINT',
            'UInt8': 'SMALLINT',
            'UInt16': 'INTEGER',
            'UInt32': 'BIGINT',
            'UInt64': 'BIGINT',
            'Float32': 'REAL',
            'Float64': 'DOUBLE PRECISION',
            'Utf8': 'TEXT',
            'String': 'TEXT',
            'Boolean': 'BOOLEAN',
            'Date': 'DATE',
            'Datetime': 'TIMESTAMP',
            'Time': 'TIME',
            'Binary': 'BYTEA',
            'Decimal': 'NUMERIC',
            'Categorical': 'TEXT',
            'Object': 'TEXT',
        }
        
        # Construir columnas
        columns = []
        for col in df.columns:
            col_type = str(df[col].dtype)
            pg_type = type_mapping.get(col_type, 'TEXT')
            columns.append(f"{col} {pg_type}")
        
        # Crear tabla
        create_sql = f"CREATE TABLE {table} ({', '.join(columns)})"
        cursor.execute(create_sql)
        self.connection.commit()

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
            table_logger.info(f"  Columnas originales: {df.columns}")
            
            # Paso 3: Estandarizar columnas (automatico)
            table_logger.info("Paso 3/7: Estandarizando nombres de columnas...")
            new_columns = {col: self._standardize_column_name(col) for col in df.columns}
            df = df.rename(new_columns)
            table_logger.info(f"  Columnas estandarizadas: {list(new_columns.values())}")
            
            # Paso 4: Aplicar mapeo explicito (solo renombres intencionales)
            table_logger.info("Paso 4/7: Aplicando renombres explicitos...")
            if self.column_mapping:
                # Convertir el mapeo a minusculas para comparar
                # Tambien estandarizar los valores (ej: Red_social -> red_social)
                normalized_mapping = {
                    self._standardize_column_name(k): self._standardize_column_name(v) 
                    for k, v in self.column_mapping.items()
                }
                valid_mapping = {k: v for k, v in normalized_mapping.items() if k in df.columns}
                if valid_mapping:
                    df = df.rename(valid_mapping)
                    table_logger.info(f"  Renombres aplicados: {valid_mapping}")
                else:
                    table_logger.info("  No hay renombres para aplicar")
            else:
                table_logger.info("  No hay renombres configurados")
            
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
            
            # Paso 6: Preparar tabla destino
            table_logger.info("Paso 6/7: Preparando tabla destino...")
            cursor = self.connection.cursor()
            
            # Verificar si la tabla existe
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table,))
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                # Crear tabla basada en la estructura del DataFrame
                table_logger.info(f"  Tabla {table} no existe. Creando...")
                self._create_table(cursor, table, df)
                table_logger.info(f"  Tabla {table} creada exitosamente")
            elif self.truncate_before_load:
                # Truncar si existe y esta configurado
                cursor.execute(f"TRUNCATE TABLE {table} CASCADE")
                self.connection.commit()
                table_logger.info(f"  Tabla {table} truncada")
            else:
                table_logger.info(f"  Tabla {table} existe. Modo: Insertar sin truncar")
            
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
