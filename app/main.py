from config.logging_config import logger
from database.oracle_pool import OraclePool


def main():

    logger.info("===================================")
    logger.info("ETL DIGERCIC")
    logger.info("===================================")

    pool = OraclePool()

    logger.info("Pool Oracle listo para inicializar.")

    # connection = pool.acquire()

    # connection.close()

    # pool.close()


if __name__ == "__main__":
    main()