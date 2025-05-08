# ===============================
# Sección 1: Importación de dependencias
# ===============================

import os
import sys
import logging

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import  udf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
# Añadir ruta para importar módulos personalizados
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Módulos personalizados
from extra.read import read_csv_files
from extra.transform_data import (
    convertir_fecha,
    quitar_tildes,
    convertir_palabra_a_numero,
    reemplazar_nulos
)
from extra.utils import fecha_ejecucion
from extra.load import save_table
# ===============================
# Sección 2: Configuración de logging
# ===============================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===============================
# Sección 3: Configuración y creación de sesión Spark
# ===============================

spark_config = (
    SparkConf()
    .setAppName("Hudi data loading")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.hadoop.hive.metastore.client.factory.class", 
         "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
)

spark = (
    SparkSession.builder
    .appName("QR Fisicos")
    .enableHiveSupport()
    .config(conf=spark_config)
    .getOrCreate()
)

# ===============================
# Sección 4: Inicialización de Glue Context y Job
# ===============================

glue_context = GlueContext(spark.sparkContext)
job = Job(glue_context)

# ===============================
# Sección 5: Lectura de datos desde S3
# ===============================
GLUE_TABLE_NAME = 'co_transacciones'
GLUE_DB_NAME = 'energy_data'
GLUE_S3 = f's3://energia-btgpactual/curated/{GLUE_TABLE_NAME}/'

# CONSTANTS
GLUE_FILE_FORMAT = 'parquet'
GLUE_WRITE_METOD = 'append'

transacciones_ext = read_csv_files('s3://energia-btgpactual/raw/transacciones_ext.csv', ',')



# ===============================
# Sección 6: Transformaciones
# ===============================

try:
    convertir_udf = udf(convertir_palabra_a_numero, FloatType())
    transacciones_ext = transacciones_ext.withColumn("valor", convertir_udf(transacciones_ext["valor"]))
    transacciones_ext = transacciones_ext.withColumn("consumo_kwh", convertir_udf(transacciones_ext["consumo_kwh"]))
    transacciones_ext = quitar_tildes(transacciones_ext,"comentarios")
    transacciones_ext = convertir_fecha(transacciones_ext,"fecha")
    transacciones_ext = fecha_ejecucion(transacciones_ext)
    transacciones_ext = reemplazar_nulos(transacciones_ext, "valor")
    transacciones_ext = reemplazar_nulos(transacciones_ext, "consumo_kwh")
except Exception as e:
    logger.error(f"Fallo en la transformación de datos: {e}")
    raise


save_table(transacciones_ext, GLUE_DB_NAME, GLUE_TABLE_NAME, GLUE_S3, GLUE_FILE_FORMAT, GLUE_WRITE_METOD,["year","month","day"])
# ===============================
# Sección 7: Finalización del trabajo Glue
# ===============================

job.commit()
