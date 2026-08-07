import psycopg
from typing import Any, Dict, List, Optional
from config.logging_config import logger
from core.loaders.base_loader import BaseLoader


class PostgresLoader(BaseLoader):
    """
    Loader para bases de datos PostgreSQL.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

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
        """Carga datos en PostgreSQL."""
        if not self._connected:
            self.connect()

        if not data:
            logger.warning("[PostgresLoader] No hay datos para cargar")
            return 0

        columns = list(data[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        try:
            cursor = self.connection.cursor()

            if mode == "insert":
                query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
                rows_inserted = 0

                for row in data:
                    values = tuple(row[col] for col in columns)
                    cursor.execute(query, values)
                    rows_inserted += 1

                self.connection.commit()
                logger.info(f"[PostgresLoader] Insertadas {rows_inserted} filas en {table}")

                return rows_inserted

            elif mode == "upsert":
                update_cols = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns])
                query = f"""
                    INSERT INTO {table} ({col_names}) VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET {update_cols}
                """
                rows_upserted = 0

                for row in data:
                    values = tuple(row[col] for col in columns)
                    cursor.execute(query, values)
                    rows_upserted += 1

                self.connection.commit()
                logger.info(f"[PostgresLoader] Upsert {rows_upserted} filas en {table}")

                return rows_upserted

            elif mode == "replace":
                cursor.execute(f"TRUNCATE TABLE {table}")
                query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
                rows_inserted = 0

                for row in data:
                    values = tuple(row[col] for col in columns)
                    cursor.execute(query, values)
                    rows_inserted += 1

                self.connection.commit()
                logger.info(f"[PostgresLoader] Reemplazadas {rows_inserted} filas en {table}")

                return rows_inserted

            else:
                raise ValueError(f"[PostgresLoader] Modo no soportado: {mode}")

        except Exception as e:
            self.connection.rollback()
            raise

        finally:
            cursor.close()

    def disconnect(self) -> None:
        """Cierra la conexion."""
        if self.connection:
            self.connection.close()
            self._connected = False
            logger.info("[PostgresLoader] Desconectado")
