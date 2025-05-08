from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, dayofmonth, current_date, lpad
import logging


logger = logging.getLogger(__name__)

def fecha_ejecucion(df: DataFrame) -> DataFrame:
    """
    Agrega las columnas year, month, day y fecha_ejecucion al DataFrame,
    con formato de dos dígitos para month y day.

    :param df: DataFrame original.
    :return: DataFrame con columnas de fecha agregadas.
    """
    try:
        logger.info("Agregando columnas de fecha de ejecución: year, month, day, fecha_ejecucion")

        return df.withColumn("year", year(current_date()).cast("string")) \
                 .withColumn("month", lpad(month(current_date()).cast("string"), 2, "0")) \
                 .withColumn("day", lpad(dayofmonth(current_date()).cast("string"), 2, "0")) \
                 .withColumn("fecha_ejecucion", current_date())
    except Exception as e:
        logger.error(f"Error al agregar columnas de fecha de ejecución: {e}")
        raise
