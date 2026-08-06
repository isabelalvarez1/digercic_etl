import oracledb

from config.settings import (
    ORACLE_HOST,
    ORACLE_PORT,
    ORACLE_SERVICE,
    ORACLE_USER,
    ORACLE_PASSWORD,
)

from config.logging_config import logger


class OraclePool:
    """
    Pool de conexiones Oracle.

    Se crea una sola vez y todas las clases reutilizan las conexiones.
    """

    def __init__(self):

        self.pool = None

    def initialize(self):

        if self.pool is not None:
            return

        try:

            dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"

            logger.info("Inicializando Pool Oracle...")

            self.pool = oracledb.create_pool(
                user=ORACLE_USER,
                password=ORACLE_PASSWORD,
                dsn=dsn,
                min=2,
                max=10,
                increment=2,
            )

            logger.info("Pool Oracle inicializado correctamente.")

        except Exception as e:

            logger.exception(f"Error creando Pool Oracle: {e}")

            raise

    def acquire(self):

        if self.pool is None:
            self.initialize()

        return self.pool.acquire()

    def close(self):

        if self.pool:

            self.pool.close()

            logger.info("Pool Oracle cerrado.")