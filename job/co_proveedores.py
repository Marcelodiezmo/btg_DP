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
from extra.read import read_csv_files
from extra.transform_data import (
    capitalizar_columna,
    eliminar_nulos,
    quitar_tildes
)

from extra.load import save_table
from extra.utils import fecha_ejecucion
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
GLUE_TABLE_NAME = 'co_proveedores'
GLUE_DB_NAME = 'energy_data'
GLUE_S3 = f's3://energia-btgpactual/curated/{GLUE_TABLE_NAME}/'

# CONSTANTS
GLUE_FILE_FORMAT = 'parquet'
GLUE_WRITE_METOD = 'append'



proveedores_ext = read_csv_files('s3://energia-btgpactual/raw/proveedores_ext.csv', ',')


# ===============================
# Sección 6: Transformaciones
# ===============================

try:
    proveedores_ext = quitar_tildes(proveedores_ext,"nombre")
    proveedores_ext = quitar_tildes(proveedores_ext,"region")
    proveedores_ext = capitalizar_columna(proveedores_ext, columna="nombre")
    proveedores_ext = capitalizar_columna(proveedores_ext, columna="region")
    proveedores_ext = eliminar_nulos(proveedores_ext)
except Exception as e:
    logger.error(f"Fallo en la transformación de datos: {e}")
    raise

proveedores_ext = fecha_ejecucion(proveedores_ext)
save_table(proveedores_ext, GLUE_DB_NAME, GLUE_TABLE_NAME, GLUE_S3, GLUE_FILE_FORMAT, GLUE_WRITE_METOD,["year","month","day"])
# ===============================
# Sección 7: Finalización del trabajo Glue
# ===============================

job.commit()
