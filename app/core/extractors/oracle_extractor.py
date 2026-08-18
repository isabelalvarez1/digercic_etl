import polars as pl
import oracledb
from typing import Any, Dict, List, Optional
from datetime import datetime
from config.logging_config import logger, setup_table_logger
from core.extractors.base_extractor import BaseExtractor
from core.utils import get_system_resources, calculate_optimal_batch_size


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
            logger.info(f"[OracleExtractor] Usuario: {user}")

            self.connection = oracledb.connect(
                user=user,
                password=password,
                dsn=dsn,
            )

            self._connected = True
            logger.info(f"[OracleExtractor] Conexion exitosa a {host}:{port}/{service}")

        except Exception as e:
            logger.exception(f"[OracleExtractor] Error de conexion: {e}")
            raise

    def _get_columns(self, cursor, query: str, params: Dict) -> List[str]:
        """
        Obtiene los nombres de las columnas sin modificar el query original.
        
        Estrategias:
        1. Ejecutar el query con FETCH FIRST 0 ROWS ONLY (no retorna datos)
        2. Si falla, usar subquery con ROWNUM
        3. Si falla, ejecutar query normal y obtener description
        
        Args:
            cursor: Cursor de Oracle
            query: Query original del usuario
            params: Parametros del query
            
        Returns:
            Lista de nombres de columnas
        """
        import re
        
        # Estrategia 1: FETCH FIRST 0 ROWS (Oracle 12c+)
        try:
            test_query = f"{query} FETCH FIRST 0 ROWS ONLY"
            cursor.execute(test_query, params)
            return [desc[0] for desc in cursor.description]
        except Exception:
            pass
        
        # Estrategia 2: Subquery con ROWNUM
        try:
            test_query = f"SELECT * FROM ({query}) WHERE ROWNUM <= 0"
            cursor.execute(test_query, params)
            return [desc[0] for desc in cursor.description]
        except Exception:
            pass
        
        # Estrategia 3: Ejecutar query normal (obtiene 1 fila)
        try:
            # Para queries con ORDER BY, LIMIT, etc.
            test_query = query
            cursor.execute(test_query, params)
            columns = [desc[0] for desc in cursor.description]
            # Consumir el resultado para evitar problemas
            cursor.fetchall()
            return columns
        except Exception:
            pass
        
        # Estrategia 4: Extraer de la tabla principal (fallback)
        # Buscar "FROM tabla" y usar DESCRIBE
        match = re.search(r'FROM\s+(\w+\.\w+|\w+)', query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            try:
                cursor.execute(f"SELECT * FROM {table_name} WHERE ROWNUM <= 1")
                columns = [desc[0] for desc in cursor.description]
                cursor.fetchall()
                return columns
            except Exception:
                pass
        
        raise Exception(f"No se pudieron obtener las columnas del query: {query[:100]}...")

    def extract(self, query: str, params: Optional[Dict] = None, table_name: str = "unknown") -> List[Dict]:
        """
        Extrae datos de Oracle usando Polars con lotes.
        
        Para tablas grandes, usa OFFSET/FETCH NEXT para procesar por lotes.
        """
        if not self._connected:
            self.connect()

        if params is None:
            params = {}

        # Logger especifico para esta tabla
        table_logger = setup_table_logger(table_name)
        extraction_start = datetime.now()

        try:
            # Paso 1: Detectar recursos del sistema
            table_logger.info(f"{'='*50}")
            table_logger.info(f"INICIO EXTRACCION: {table_name}")
            table_logger.info(f"Fecha inicio: {extraction_start.strftime('%Y-%m-%d %H:%M:%S')}")
            table_logger.info(f"{'-'*50}")
            
            table_logger.info("Paso 1/6: Detectando recursos del sistema...")
            resources = get_system_resources()
            table_logger.info(f"  CPU Cores Logicales: {resources['cpu_cores_logical']}")
            table_logger.info(f"  CPU Cores Fisicos: {resources['cpu_cores_physical']}")
            table_logger.info(f"  Memoria Total: {resources['memory_total_gb']} GB")
            table_logger.info(f"  Memoria Disponible: {resources['memory_available_gb']} GB")
            table_logger.info(f"  Memoria Usada: {resources['memory_percent_used']}%")
            
            # Paso 2: Preparar cursor
            table_logger.info("Paso 2/6: Preparando cursor...")
            
            # Paso 3: Obtener columnas (sin modificar el query original)
            table_logger.info("Paso 3/6: Obteniendo estructura de la tabla...")
            col_cursor = self.connection.cursor()
            columns = self._get_columns(col_cursor, query, params)
            col_cursor.close()
            cursor = self.connection.cursor()
            table_logger.info("  Cursor creado")
            table_logger.info(f"  Columnas encontradas: {len(columns)}")
            table_logger.info(f"  Nombres: {', '.join(columns)}")
            
            # Paso 4: Contar registros
            table_logger.info("Paso 4/6: Contando registros totales...")
            count_query = f"SELECT COUNT(*) FROM ({query})"
            cursor.execute(count_query)
            total_rows = cursor.fetchone()[0]
            table_logger.info(f"  Total registros: {total_rows}")
            
            # Paso 5: Calcular lotes optimos
            table_logger.info("Paso 5/6: Calculando lotes optimos...")
            batch_info = calculate_optimal_batch_size(total_rows, resources)
            
            # Usar batch_size del config si es mayor al calculado
            final_batch_size = max(self.batch_size, batch_info["batch_size"])
            final_batch_count = (total_rows + final_batch_size - 1) // final_batch_size
            
            table_logger.info(f"  Batch Size Configurado: {self.batch_size}")
            table_logger.info(f"  Batch Size Optimizado: {batch_info['batch_size']}")
            table_logger.info(f"  Batch Size Final: {final_batch_size}")
            table_logger.info(f"  Total Lotes: {final_batch_count}")
            table_logger.info(f"  Registros por Core: {batch_info['rows_per_core']}")
            table_logger.info(f"  Memoria Estimada por Lote: {batch_info['estimated_memory_mb']} MB")
            
            # Paso 6: Extraer datos
            table_logger.info("Paso 6/6: Extrayendo datos...")
            table_logger.info(f"{'-'*50}")
            
            all_data = []
            offset = 0
            batch_num = 0
            
            while offset < total_rows:
                batch_num += 1
                batch_start = datetime.now()
                
                batch_query = f"{query} OFFSET {offset} ROWS FETCH NEXT {final_batch_size} ROWS ONLY"
                cursor.execute(batch_query, params)
                rows = cursor.fetchall()
                
                if not rows:
                    break
                
                # Convertir a lista de diccionarios
                batch_data = [dict(zip(columns, row)) for row in rows]
                all_data.extend(batch_data)
                
                offset += final_batch_size
                batch_end = datetime.now()
                batch_duration = (batch_end - batch_start).total_seconds()
                
                # Calcular porcentaje completado
                percent_complete = (len(all_data) / total_rows) * 100 if total_rows > 0 else 100
                
                table_logger.info(f"  CHUNK {batch_num}/{final_batch_count}")
                table_logger.info(f"    Registros: {len(rows)}")
                table_logger.info(f"    Chunk Size: {final_batch_size}")
                table_logger.info(f"    Offset: {offset - final_batch_size}")
                table_logger.info(f"    Tiempo: {batch_duration:.2f}s")
                table_logger.info(f"    Progreso: {percent_complete:.1f}%")
                table_logger.info(f"    Acumulado: {len(all_data)}/{total_rows}")
            
            cursor.close()
            
            # Resumen de extraccion
            extraction_end = datetime.now()
            extraction_duration = (extraction_end - extraction_start).total_seconds()
            
            table_logger.info(f"{'-'*50}")
            
            # Alerta si COUNT != extraidos
            if len(all_data) != total_rows:
                table_logger.warning(f"  ADVERTENCIA: COUNT={total_rows} pero extraidos={len(all_data)}")
            
            table_logger.info(f"EXTRACCION COMPLETADA")
            table_logger.info(f"  Registros extraidos: {len(all_data)}")
            table_logger.info(f"  Chunks procesados: {batch_num}")
            table_logger.info(f"  Chunk Size utilizado: {final_batch_size}")
            table_logger.info(f"  Tiempo total: {extraction_duration:.2f} segundos")
            table_logger.info(f"  Velocidad: {len(all_data)/extraction_duration:.0f} registros/segundo" if extraction_duration > 0 else "  Velocidad: N/A")
            table_logger.info(f"  CPU Cores utilizados: {resources['cpu_cores_logical']}")
            table_logger.info(f"  Memoria disponible: {resources['memory_available_gb']} GB")
            table_logger.info(f"Fecha fin: {extraction_end.strftime('%Y-%m-%d %H:%M:%S')}")
            table_logger.info(f"{'='*50}")
            
            return all_data

        except Exception as e:
            extraction_end = datetime.now()
            extraction_duration = (extraction_end - extraction_start).total_seconds()
            
            table_logger.error(f"{'-'*50}")
            table_logger.error(f"ERROR EN EXTRACCION")
            table_logger.error(f"  Error: {str(e)}")
            table_logger.error(f"  Tiempo hasta error: {extraction_duration:.2f} segundos")
            table_logger.error(f"{'='*50}")
            
            logger.exception(f"[OracleExtractor] Error en extraccion: {e}")
            raise

    def extract_to_polars(self, query: str, params: Optional[Dict] = None, table_name: str = "unknown") -> pl.DataFrame:
        """
        Extrae datos directamente a un DataFrame de Polars.
        
        Mas eficiente para procesamiento posterior.
        """
        data = self.extract(query, params, table_name)
        return pl.DataFrame(data)

    def disconnect(self) -> None:
        """Cierra la conexion."""
        if self.connection:
            self.connection.close()
            self._connected = False
            logger.info("[OracleExtractor] Desconectado")
