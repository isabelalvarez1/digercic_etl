"""Pruebas aisladas del streaming: no necesitan conexiones a bases reales."""
import importlib.util
import logging
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]


class FakeExtractor:
    def __init__(self, rows, count=None, fail=False):
        self.rows = rows
        self.count = len(rows) if count is None else count
        self.fail = fail
        self.queries = 0
        self.disconnected = False

    def connect(self):
        pass

    def get_columns(self, query, params):
        return ["ID"]

    def get_count(self, query, params):
        return self.count

    def iter_batches(self, query, batch_size, params):
        self.queries += 1
        for start in range(0, len(self.rows), batch_size):
            if self.fail and start > 0:
                raise RuntimeError("fallo Oracle")
            yield self.rows[start:start + batch_size]

    def disconnect(self):
        self.disconnected = True


class FakeLoader:
    def __init__(self):
        self.rows = []
        self.disconnected = False
        self.truncate_before_load = True

    def connect(self):
        pass

    def prepare_table(self, table, columns):
        self.rows.clear()

    def insert_batch(self, batch, table):
        self.rows.extend(batch)
        return len(batch)

    def count_rows(self, table):
        return len(self.rows)

    def disconnect(self):
        self.disconnected = True


class FakeMonitor:
    def __init__(self):
        self.active_connections = 0

    def register_connection(self):
        self.active_connections += 1

    def unregister_connection(self):
        self.active_connections -= 1

    def wait_for_resources(self, task_name=""):
        return {}

    def get_status(self):
        return {"cpu_percent": 0, "ram_available_gb": 1}


class StreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logger = logging.getLogger("test_streaming")
        modules = {
            "config": types.ModuleType("config"),
            "config.logging_config": types.ModuleType("config.logging_config"),
            "core": types.ModuleType("core"),
            "core.factory": types.ModuleType("core.factory"),
            "core.resource_monitor": types.ModuleType("core.resource_monitor"),
            "core.utils": types.ModuleType("core.utils"),
        }
        modules["config.logging_config"].logger = logger
        modules["config.logging_config"].setup_table_logger = lambda table: logger
        modules["core.factory"].ExtractorFactory = type("ExtractorFactory", (), {})
        modules["core.factory"].LoaderFactory = type("LoaderFactory", (), {})
        modules["core.resource_monitor"].ResourceMonitor = FakeMonitor
        modules["core.utils"].calculate_optimal_config = lambda total, cols: {
            "batch_size": 3, "batch_count": (total + 2) // 3,
            "system": {"cpu_cores": 1, "memory_available_gb": 1},
            "estimated_memory_mb": 1, "estimated_time_copy_min": 1,
        }
        with patch.dict(sys.modules, modules):
            spec = importlib.util.spec_from_file_location(
                "pipeline_manager_test", ROOT / "app/core/pipeline_manager.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        cls.module = module

    def run_pipeline(self, extractor):
        loader = FakeLoader()
        self.module.ExtractorFactory.create = lambda *args: extractor
        self.module.LoaderFactory.create = lambda *args: loader
        config = {
            "extractions": [{"name": "vista", "source": "oracle", "query": "SELECT ID FROM V"}],
            "loads": [{"name": "destino", "source": "vista", "target": "postgresql", "table": "destino"}],
        }
        # El import dentro de _run_streaming necesita el módulo simulado.
        utils = types.ModuleType("core.utils")
        utils.calculate_optimal_config = lambda total, cols: {
            "batch_size": 3, "batch_count": (total + 2) // 3,
            "system": {"cpu_cores": 1, "memory_available_gb": 1},
            "estimated_memory_mb": 1, "estimated_time_copy_min": 1,
        }
        with patch.dict(sys.modules, {"core.utils": utils}):
            try:
                result = self.module.PipelineManager(config)._run_streaming(
                    config["extractions"], config["loads"]
                )
                return result, loader
            finally:
                self.assertTrue(extractor.disconnected)
                self.assertTrue(loader.disconnected)

    def test_un_cursor_todas_las_filas_sin_duplicados(self):
        rows = [{"ID": n} for n in range(10)]
        extractor = FakeExtractor(rows)
        result, loader = self.run_pipeline(extractor)
        self.assertEqual(loader.rows, rows)
        self.assertEqual(result["loads"]["vista"], 10)
        self.assertEqual(extractor.queries, 1)

    def test_diferencia_de_conteo_falla(self):
        extractor = FakeExtractor([{"ID": n} for n in range(9)], count=10)
        with self.assertRaisesRegex(RuntimeError, "Conteo diferente"):
            self.run_pipeline(extractor)

    def test_error_del_productor_llega_al_dag(self):
        extractor = FakeExtractor([{"ID": n} for n in range(9)], fail=True)
        with self.assertRaisesRegex(RuntimeError, "fallo Oracle"):
            self.run_pipeline(extractor)

    def test_vista_vacia_termina_sin_falso_error(self):
        result, loader = self.run_pipeline(FakeExtractor([]))
        self.assertEqual(result["loads"]["vista"], 0)
        self.assertEqual(loader.rows, [])

    def test_conteo_real_postgres_debe_coincidir(self):
        with patch.object(FakeLoader, "count_rows", side_effect=[0, 2]):
            with self.assertRaisesRegex(RuntimeError, "PostgreSQL tiene 2 filas"):
                self.run_pipeline(FakeExtractor([{"ID": n} for n in range(3)]))

    def test_destino_con_filas_antes_de_cargar_falla(self):
        with patch.object(FakeLoader, "count_rows", return_value=1):
            with self.assertRaisesRegex(RuntimeError, "despues de prepararla"):
                self.run_pipeline(FakeExtractor([{"ID": n} for n in range(3)]))


class PostgresCopyTests(unittest.TestCase):
    def test_error_despues_del_copy_no_inserta_lote_dos_veces(self):
        base = types.ModuleType("core.loaders.base_loader")
        base.BaseLoader = type("BaseLoader", (), {})
        logging_config = types.ModuleType("config.logging_config")
        logging_config.logger = logging.getLogger("test_postgres")
        logging_config.setup_table_logger = lambda table: logging_config.logger
        utils = types.ModuleType("core.utils")
        utils.get_system_resources = lambda: {}
        utils.calculate_optimal_batch_size = lambda *args: {}
        psycopg = types.ModuleType("psycopg")
        psycopg.sql = types.ModuleType("psycopg.sql")
        with patch.dict(sys.modules, {
            "polars": types.ModuleType("polars"),
            "psycopg": psycopg,
            "psycopg.sql": psycopg.sql,
            "config.logging_config": logging_config,
            "core.loaders.base_loader": base,
            "core.utils": utils,
        }):
            sys.modules["polars"].DataFrame = object
            spec = importlib.util.spec_from_file_location(
                "postgres_loader_test", ROOT / "app/core/loaders/postgres_loader.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            loader = module.PostgresLoader.__new__(module.PostgresLoader)
            loader._ensure_connected = lambda: None
            loader._copy_batch = Mock(side_effect=RuntimeError("fallo tras confirmar COPY"))
            loader._insert_batch_fallback = Mock()
            with self.assertRaisesRegex(RuntimeError, "fallo tras confirmar COPY"):
                loader.insert_batch([{"ID": 1}], "destino")
            loader._insert_batch_fallback.assert_not_called()


class OracleCursorTests(unittest.TestCase):
    def test_consulta_unica_y_fetchmany_sin_columna_adicional(self):
        class Cursor:
            description = [("ID",)]

            def __init__(self):
                self.calls = 0
                self.rows = [(n,) for n in range(8)]
                self.closed = False

            def execute(self, query, params):
                self.calls += 1
                self.query = query

            def fetchmany(self, size):
                batch, self.rows = self.rows[:size], self.rows[size:]
                return batch

            def close(self):
                self.closed = True

        cursor = Cursor()
        base = types.ModuleType("core.extractors.base_extractor")
        base.BaseExtractor = type("BaseExtractor", (), {})
        logging_config = types.ModuleType("config.logging_config")
        logging_config.logger = logging.getLogger("test_oracle")
        logging_config.setup_table_logger = lambda name: logging_config.logger
        utils = types.ModuleType("core.utils")
        utils.get_system_resources = lambda: {}
        utils.calculate_optimal_batch_size = lambda *args: {}
        with patch.dict(sys.modules, {
            "polars": types.ModuleType("polars"),
            "oracledb": types.ModuleType("oracledb"),
            "config.logging_config": logging_config,
            "core.extractors.base_extractor": base,
            "core.utils": utils,
        }):
            # Las anotaciones pl.DataFrame se evalúan durante el import.
            sys.modules["polars"].DataFrame = object
            spec = importlib.util.spec_from_file_location(
                "oracle_extractor_test", ROOT / "app/core/extractors/oracle_extractor.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            extractor = module.OracleExtractor.__new__(module.OracleExtractor)
            extractor.connection = types.SimpleNamespace(cursor=lambda: cursor)
            extractor._ensure_connected = lambda: None
            batches = list(extractor.iter_batches("SELECT ID FROM V", 3, {}))

        self.assertEqual([row for batch in batches for row in batch],
                         [{"ID": n} for n in range(8)])
        self.assertEqual(cursor.calls, 1)
        self.assertEqual(cursor.query, "SELECT ID FROM V")
        self.assertTrue(cursor.closed)


if __name__ == "__main__":
    unittest.main()
