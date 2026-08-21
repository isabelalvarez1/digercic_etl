import os
import re
import concurrent.futures
from typing import Any, Dict, List
from datetime import datetime
from config.logging_config import logger, setup_table_logger
from core.factory import ExtractorFactory, LoaderFactory
from core.extractors.oracle_extractor import OracleExtractor
from core.loaders.postgres_loader import PostgresLoader
from core.resource_monitor import ResourceMonitor


class PipelineManager:
    """
    Orquestador de pipelines ETL.
    
    Ejecuta multiples fuentes de datos en paralelo o secuencialmente.
    Genera logs separados por tabla con tiempos de respuesta.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.results = {}

    def _resolve_env_vars(self, config: Any) -> Any:
        """Resuelve variables de entorno ${VAR} en la configuracion."""
        if isinstance(config, str):
            pattern = r'\$\{(\w+)\}'
            def replace_var(match):
                var_name = match.group(1)
                return os.getenv(var_name, match.group(0))
            return re.sub(pattern, replace_var, config)
        elif isinstance(config, dict):
            return {k: self._resolve_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._resolve_env_vars(item) for item in config]
        return config

    def _validate_resources(self) -> bool:
        """Valida que los recursos necesarios esten disponibles."""
        logger.info("[PipelineManager] Validando recursos...")
        
        # Verificar que existan extractors y loaders
        supported_sources = ExtractorFactory.get_supported_sources()
        supported_targets = LoaderFactory.get_supported_targets()
        
        logger.info(f"[PipelineManager] Fuentes soportadas: {supported_sources}")
        logger.info(f"[PipelineManager] Destinos soportados: {supported_targets}")
        
        # Verificar configuraciones
        extractions = self.config.get("extractions", [])
        loads = self.config.get("loads", [])
        
        logger.info(f"[PipelineManager] Extracciones configuradas: {len(extractions)}")
        logger.info(f"[PipelineManager] Cargas configuradas: {len(loads)}")
        
        # Validar que cada extraccion tenga su carga correspondiente
        extraction_names = [e.get("name") for e in extractions]
        for load in loads:
            source_name = load.get("source")
            if source_name not in extraction_names:
                logger.warning(f"[PipelineManager] Carga '{load.get('name')}' referencia fuente '{source_name}' que no existe")
        
        logger.info("[PipelineManager] Validacion completada")
        return True

    def run(self) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo definido en la configuracion.
        
        Returns:
            Diccionario con resultados de cada paso
        """
        logger.info("=" * 50)
        logger.info("PIPELINE MANAGER - INICIO")
        logger.info("=" * 50)

        start_time = datetime.now()

        try:
            # Validar recursos
            self._validate_resources()
            
            # Resolver variables de entorno en toda la configuracion
            logger.info("[PipelineManager] Resolviendo variables de entorno...")
            self.config = self._resolve_env_vars(self.config)
            
            # Obtener configuraciones
            extractions = self.config.get("extractions", [])
            loads = self.config.get("loads", [])
            
            if not extractions:
                logger.warning("No hay extracciones configuradas")
                return {}

            # Verificar si hay streaming (Oracle + PostgreSQL en la misma extraccion)
            if self._can_stream(extractions, loads):
                logger.info("[PipelineManager] Modo STREAMING: Extract + Load por chunks")
                result = self._run_streaming(extractions, loads)
            else:
                # Modo original: extraer todo, luego cargar
                parallel_mode = self.config.get("parallel", True)
                if parallel_mode:
                    all_data = self._run_parallel(extractions)
                else:
                    all_data = self._run_sequential(extractions)

                transformations = self.config.get("transformations", {})
                if transformations and transformations.get("enabled", False):
                    all_data = self._run_transformations(all_data, transformations)

                load_results = self._run_loads(all_data, loads)

                elapsed = (datetime.now() - start_time).seconds
                result = {
                    "status": "completed",
                    "elapsed_seconds": elapsed,
                    "extractions": {k: len(v) for k, v in all_data.items()},
                    "loads": load_results,
                }

            logger.info("=" * 50)
            logger.info(f"PIPELINE COMPLETADO en {result.get('elapsed_seconds', 0)}s")
            logger.info("=" * 50)

            return result

        except Exception as e:
            logger.exception(f"Error en pipeline: {e}")
            raise

    def _can_stream(self, extractions: List[Dict], loads: List[Dict]) -> bool:
        """Verifica si se puede usar modo streaming (Oracle->PostgreSQL)."""
        if not loads:
            return False
        for ext in extractions:
            if ext.get("source") == "oracle":
                return True
        return False

    def _run_streaming(self, extractions: List[Dict], loads: List[Dict]) -> Dict[str, Any]:
        """
        Modo streaming: extrae de Oracle y carga a PostgreSQL chunk por chunk.
        Incluye resume automatico si falla y workers paralelos.
        """
        logger.info("=" * 50)
        logger.info("MODO STREAMING: Extract + Load por chunks")
        logger.info("=" * 50)

        start_time = datetime.now()
        extraction_results = {}
        load_results = {}

        for ext_config in extractions:
            name = ext_config.get("name", "unnamed")
            source_type = ext_config.get("source")
            source_config = ext_config.get("config", {})
            query = ext_config.get("query", "")
            params = ext_config.get("params", {})

            load_config = None
            for lc in loads:
                if lc.get("source") == name:
                    load_config = lc
                    break

            if not load_config:
                logger.warning(f"[{name}] No hay load configurado, usando modo normal")
                continue

            target_type = load_config.get("target")
            target_config = load_config.get("config", {})
            table = load_config.get("table", "")
            batch_size = int(ext_config.get("config", {}).get("batch_size", 100000))

            try:
                extractor = ExtractorFactory.create(source_type, source_config)
                loader = LoaderFactory.create(target_type, target_config)
                monitor = ResourceMonitor()

                extractor.connect()
                loader.connect()
                loader.create_control_table()
                monitor.register_connection()

                columns = extractor.get_columns(query, params)
                total_rows = extractor.get_count(query, params)
                logger.info(f"[{name}] Total registros: {total_rows}")
                logger.info(f"[{name}] Columnas: {len(columns)}")

                loader.prepare_table(table, columns)

                last_chunk = loader.get_last_chunk(name)
                if last_chunk > 0:
                    logger.info(f"[{name}] RESUME desde chunk {last_chunk + 1} (ultimo completado: {last_chunk})")
                    start_offset = last_chunk * batch_size
                    total_loaded = last_chunk * batch_size
                else:
                    logger.info(f"[{name}] Iniciando desde chunk 1")
                    start_offset = 0
                    total_loaded = 0

                offset = start_offset
                chunk_num = last_chunk
                chunk_start = datetime.now()

                while offset < total_rows:
                    monitor.wait_for_resources(task_name=name)
                    
                    chunk_num += 1
                    chunk_data = extractor.extract_batch(query, offset, batch_size, columns, params)

                    if not chunk_data:
                        break

                    loaded = loader.insert_batch(chunk_data, table)
                    loader.save_chunk_status(name, table, chunk_num, loaded, "OK")
                    total_loaded += loaded

                    offset += batch_size
                    chunk_end = datetime.now()
                    chunk_duration = (chunk_end - chunk_start).total_seconds()
                    chunk_start = chunk_end

                    percent = (total_loaded / total_rows) * 100 if total_rows > 0 else 100
                    remaining = total_rows - total_loaded
                    eta_seconds = (remaining / (total_loaded / chunk_duration)) if total_loaded > 0 and chunk_duration > 0 else 0
                    eta_min = int(eta_seconds / 60)

                    status = monitor.get_status()
                    logger.info(f"  CHUNK {chunk_num} | {loaded} registros | {chunk_duration:.1f}s | Total: {total_loaded}/{total_rows} ({percent:.1f}%) | ETA: {eta_min}min | CPU: {status['cpu_percent']:.1f}% | RAM: {status['ram_available_gb']:.1f}GB")

                extraction_results[name] = total_loaded
                load_results[name] = total_loaded

                logger.info(f"[{name}] COMPLETADO: {total_loaded} registros en {(datetime.now() - start_time).seconds}s")

                monitor.unregister_connection()
                extractor.disconnect()
                loader.disconnect()

            except Exception as e:
                logger.exception(f"[{name}] Error en streaming: {e}")
                extraction_results[name] = 0
                load_results[name] = 0

        elapsed = (datetime.now() - start_time).seconds

        return {
            "status": "completed",
            "elapsed_seconds": elapsed,
            "extractions": extraction_results,
            "loads": load_results,
        }

    def _run_parallel(self, extractions: List[Dict]) -> Dict[str, List]:
        """Ejecuta extracciones en paralelo."""
        logger.info("Modo: PARALELO")
        all_data = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}

            for ext_config in extractions:
                name = ext_config.get("name", "unnamed")
                source_type = ext_config.get("source")
                source_config = ext_config.get("config", {})
                query = ext_config.get("query", "")
                params = ext_config.get("params", {})
                
                # Obtener nombre de tabla del query
                table_name = self._extract_table_name(query)

                extractor = ExtractorFactory.create(source_type, source_config)
                future = executor.submit(extractor.execute, query, params, table_name)
                futures[future] = name

            for future in concurrent.futures.as_completed(futures):
                name = futures[future]
                try:
                    data = future.result()
                    all_data[name] = data
                    logger.info(f"[{name}] Extraidas {len(data)} filas")
                except Exception as e:
                    logger.exception(f"[{name}] Error en extraccion: {e}")
                    all_data[name] = []

        return all_data

    def _run_sequential(self, extractions: List[Dict]) -> Dict[str, List]:
        """Ejecuta extracciones secuencialmente."""
        logger.info("Modo: SECUENCIAL")
        all_data = {}

        for ext_config in extractions:
            name = ext_config.get("name", "unnamed")
            source_type = ext_config.get("source")
            source_config = ext_config.get("config", {})
            query = ext_config.get("query", "")
            params = ext_config.get("params", {})
            
            # Obtener nombre de tabla del query
            table_name = self._extract_table_name(query)

            try:
                extractor = ExtractorFactory.create(source_type, source_config)
                data = extractor.execute(query, params, table_name)
                all_data[name] = data
                logger.info(f"[{name}] Extraidas {len(data)} filas")
            except Exception as e:
                logger.exception(f"[{name}] Error en extraccion: {e}")
                all_data[name] = []

        return all_data

    def _extract_table_name(self, query: str) -> str:
        """Extrae el nombre de la tabla del query SQL."""
        # Buscar patrones como "FROM tabla" o "INTO tabla"
        match = re.search(r'(?:FROM|INTO|UPDATE)\s+(\w+)', query, re.IGNORECASE)
        if match:
            return match.group(1).lower()
        return "unknown"

    def _run_transformations(self, data: Dict[str, List], config: Dict) -> Dict[str, List]:
        """Ejecuta transformaciones sobre los datos."""
        logger.info("Ejecutando transformaciones...")
        
        # Por ahora retorna los datos sin transformar
        # Se puede extender con transformers personalizados
        return data

    def _run_loads(self, data: Dict[str, List], loads: List[Dict]) -> Dict[str, int]:
        """Ejecuta las cargas en los destinos con logging detallado por tabla."""
        logger.info("Ejecutando cargas...")
        load_results = {}

        for load_config in loads:
            name = load_config.get("name", "unnamed")
            target_type = load_config.get("target")
            target_config = load_config.get("config", {})
            source_name = load_config.get("source")
            table = load_config.get("table", "")
            mode = load_config.get("mode", "insert")

            if source_name not in data:
                logger.warning(f"[{name}] Fuente '{source_name}' no encontrada")
                continue

            source_data = data[source_name]

            if not source_data:
                logger.warning(f"[{name}] No hay datos para cargar")
                load_results[name] = 0
                continue

            try:
                loader = LoaderFactory.create(target_type, target_config)
                rows_loaded = loader.execute(source_data, table, mode)
                load_results[name] = rows_loaded
            except Exception as e:
                logger.exception(f"[{name}] Error en carga: {e}")
                load_results[name] = 0

        return load_results
