from config.logging_config import logger
from config.config_loader import ConfigLoader
from database.oracle_pool import OraclePool


def run_extract(config: dict) -> list:

    logger.info("=== PASO 1: EXTRACT ===")

    extract_config = config.get("extract", {})

    if not extract_config.get("enabled", False):
        logger.info("Extract deshabilitado en configuracion")
        return []

    pool = OraclePool()

    query = extract_config.get("query", "")
    params = extract_config.get("params", {})

    logger.info(f"Ejecutando query...")

    connection = pool.acquire()

    try:
        cursor = connection.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        logger.info(f"Registros extraidos: {len(rows)}")

        return [dict(zip(columns, row)) for row in rows]

    except Exception as e:
        logger.exception(f"Error en extract: {e}")
        raise

    finally:
        connection.close()
        pool.close()


def run_transform(data: list, config: dict) -> list:

    logger.info("=== PASO 2: TRANSFORM ===")

    transform_config = config.get("transform", {})

    if not transform_config.get("enabled", False):
        logger.info("Transform deshabilitado en configuracion")
        return data

    operations = transform_config.get("operations", [])
    logger.info(f"Operaciones a ejecutar: {len(operations)}")

    result = data.copy()

    for op in operations:
        op_name = op.get("name", "sin_nombre")
        op_type = op.get("type", "")

        logger.info(f"Ejecutando operacion: {op_name} ({op_type})")

        if op_type == "drop_nulls":
            columns = op.get("columns", [])
            result = [row for row in result if all(row.get(col) is not None for col in columns)]

        elif op_type == "cast":
            pass

        elif op_type == "replace":
            pass

    logger.info(f"Registros despues de transform: {len(result)}")

    return result


def run_load(data: list, config: dict):

    logger.info("=== PASO 3: LOAD ===")

    load_config = config.get("load", {})

    if not load_config.get("enabled", False):
        logger.info("Load deshabilitado en configuracion")
        return

    table = load_config.get("table", "")
    mode = load_config.get("mode", "insert")
    batch_size = load_config.get("batch_size", 500)

    logger.info(f"Destino: {table} | Modo: {mode} | Registros: {len(data)}")

    # TODO: Implementar loader PostgreSQL
    logger.warning("Load no implementado aun")


def main():

    logger.info("===================================")
    logger.info("ETL DIGERCIC - INICIO")
    logger.info("===================================")

    loader = ConfigLoader()
    config = loader.load()

    if not loader.validate():
        logger.error("Configuracion invalida. Abortando.")
        return

    pipeline_name = loader.get("pipeline.name", "desconocido")
    logger.info(f"Pipeline: {pipeline_name}")

    data = run_extract(config)
    data = run_transform(data, config)
    run_load(data, config)

    logger.info("===================================")
    logger.info("ETL DIGERCIC - FIN")
    logger.info("===================================")


if __name__ == "__main__":
    main()
