
# Sección 1: Importación de dependencias

import sys
from datetime import datetime
import logging
import boto3
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, row_number, lit, length, concat, when, lower, levenshtein, coalesce, get_json_object, trim, expr

# AWS Glue imports
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.conf import SparkConf

# Sección 2: Configuración de logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sección 3: Configuración de Spark

# Configuración de Spark
spark_config = (
    SparkConf().setAppName("Hudi data loading")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
)

# Crear sesión de Spark
spark = (
    SparkSession.builder.appName("QR Fisicos")
    .enableHiveSupport()
    .config(conf=spark_config)
    .getOrCreate()
)


def read_csv_files(s3_path: str, separator: str) -> DataFrame:
    """
    Lee archivos CSV desde una ruta de S3, asigna encabezados si están presentes,
    infiere el esquema y agrega una columna 'source_file' con el nombre del archivo de origen.

    Parámetros:
        s3_path (str): La ruta en S3 del archivo o directorio que se va a leer.
        separator (str): El delimitador que se usa en el archivo CSV.

    Retorna:
        DataFrame: El DataFrame que contiene los datos de los archivos CSV.
    """
    try:
        logger.info(f"Iniciando lectura de archivos CSV desde {s3_path} con separador '{separator}'")
        df = spark.read.csv(
            s3_path,
            sep=separator,
            header=True,
            inferSchema=True
        )
        logger.info("Archivo CSV leído correctamente.")
        return df
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV desde {s3_path}: {e}")
        raise