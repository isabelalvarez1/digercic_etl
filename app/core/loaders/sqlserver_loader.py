import pyodbc
from typing import Any, Dict, List, Optional
from config.logging_config import logger
from core.loaders.base_loader import BaseLoader


class SqlServerLoader(BaseLoader):
    """
    Loader para bases de datos SQL Server.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    def connect(self) -> None:
        """Establece conexion con SQL Server."""
        if self._connected:
            return

        try:
            host = self.config.get("host")
            port = self.config.get("port", 1433)
            database = self.config.get("database")
            user = self.config.get("user")
            password = self.config.get("password")
            driver = self.config.get("driver", "ODBC Driver 17 for SQL Server")

            conn_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
            )

            self.connection = pyodbc.connect(conn_string)
            self._connected = True
            logger.info(f"[SqlServerLoader] Conectado a {host}:{port}/{database}")

        except Exception as e:
            logger.exception(f"[SqlServerLoader] Error de conexion: {e}")
            raise

    def load(self, data: List[Dict], table: str, mode: str = "insert") -> int:
        """Carga datos en SQL Server."""
        if not self._connected:
            self.connect()

        if not data:
            logger.warning("[SqlServerLoader] No hay datos para cargar")
            return 0

        columns = list(data[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))

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
                logger.info(f"[SqlServerLoader] Insertadas {rows_inserted} filas en {table}")

                return rows_inserted

            elif mode == "upsert":
                # SQL Server usa MERGE para upsert
                update_cols = ", ".join([f"target.{col} = source.{col}" for col in columns])
                insert_cols = ", ".join(columns)
                source_cols = ", ".join([f"source.{col}" for col in columns])

                query = f"""
                    MERGE INTO {table} AS target
                    USING (SELECT ? AS id) AS source
                    ON target.id = source.id
                    WHEN MATCHED THEN
                        UPDATE SET {update_cols}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_cols}) VALUES ({source_cols});
                """
                # Nota: MERGE es mas complejo, esta es una simplificacion
                rows_upserted = 0

                for row in data:
                    values = tuple(row[col] for col in columns)
                    cursor.execute(query, values)
                    rows_upserted += 1

                self.connection.commit()
                logger.info(f"[SqlServerLoader] Upsert {rows_upserted} filas en {table}")

                return rows_upserted

            else:
                raise ValueError(f"[SqlServerLoader] Modo no soportado: {mode}")

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
            logger.info("[SqlServerLoader] Desconectado")
