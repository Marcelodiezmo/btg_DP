import logging
from pyspark.sql import DataFrame
from typing import List
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def save_table(
    df: DataFrame,
    db_name: str,
    table_name: str,
    path: str,
    file_format: str,
    write_method: str,
    partition_cols: List[str]
) -> None:
    """
    Guarda un DataFrame como una tabla particionada en el catálogo de Glue.

    :param df: DataFrame a guardar.
    :param db_name: Nombre de la base de datos en Glue.
    :param table_name: Nombre de la tabla.
    :param path: Ruta S3 donde se guardarán los datos.
    :param file_format: Formato de archivo (por ejemplo, 'parquet', 'orc').
    :param write_method: Método de escritura ('overwrite', 'append', etc.).
    :param partition_cols: Columnas por las que se particionará la tabla.
    """
    try:
        logger.info(f"Inicia el guardado de datos en la tabla {db_name}.{table_name}")

        hive_options = {
            "path": path,
            "database": db_name,
            "table": table_name,
            "mode": write_method,
            "format": file_format,
        }

        df.write \
            .partitionBy(*partition_cols) \
            .mode(write_method) \
            .format(file_format) \
            .options(**hive_options) \
            .saveAsTable(f"{db_name}.{table_name}")

        logger.info(f"Finaliza el guardado de datos en la tabla {db_name}.{table_name}")

    except Exception as e:
        logger.error(f"Error guardando la tabla {db_name}.{table_name}: {e}")
        raise










