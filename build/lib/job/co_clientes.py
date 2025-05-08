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

# Añadir ruta para importar módulos personalizados
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Módulos personalizados
from btg_DP.extra.read import read_csv_files
from btg_DP.extra.transform_data import (
    capitalizar_columna,
    eliminar_nulos,
    filtrar_emails_validos,
    limpiar_telefonos,
    quitar_tildes
)

from btg_DP.extra.load import save_table
from btg_DP.extra.utils import fecha_ejecucion
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
GLUE_TABLE_NAME = 'co_clientes'
GLUE_DB_NAME = 'energy_data'
GLUE_S3 = f's3://energia-btgpactual/curated/{GLUE_TABLE_NAME}/'

# CONSTANTS
GLUE_FILE_FORMAT = 'parquet'
GLUE_WRITE_METOD = 'append'


clientes_ext = read_csv_files('s3://energia-btgpactual/raw/clientes_ext.csv', ',')


# ===============================
# Sección 6: Transformaciones
# ===============================

try:
    clientes_ext = quitar_tildes(clientes_ext,"nombre")
    clientes_ext = quitar_tildes(clientes_ext,"tipo_cliente")
    clientes_ext = capitalizar_columna(clientes_ext, columna="nombre")
    clientes_ext = capitalizar_columna(clientes_ext, columna="tipo_cliente")
    clientes_ext = eliminar_nulos(clientes_ext)
    clientes_ext = filtrar_emails_validos(clientes_ext, columna="email")
    clientes_ext = limpiar_telefonos(clientes_ext, columna="telefono")
except Exception as e:
    logger.error(f"Fallo en la transformación de datos: {e}")
    raise

clientes_ext = fecha_ejecucion(clientes_ext)
save_table(clientes_ext, GLUE_DB_NAME, GLUE_TABLE_NAME, GLUE_S3, GLUE_FILE_FORMAT, GLUE_WRITE_METOD,["year","month","day"])
# ===============================
# Sección 7: Finalización del trabajo Glue
# ===============================

job.commit()
