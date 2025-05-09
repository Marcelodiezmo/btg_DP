import os
from dotenv import load_dotenv
from extra.utils import create_redshift_engine, load_parquet_from_s3_to_redshift
from datetime import datetime

load_dotenv()  # Carga las variables de entorno

# Datos de conexión a Redshift (ahora se leerán del .env)
host = os.getenv("REDSHIFT_HOST")
port = os.getenv("REDSHIFT_PORT")
dbname = os.getenv("REDSHIFT_DBNAME")
user = os.getenv("REDSHIFT_USER")
password = os.getenv("REDSHIFT_PASSWORD")
schema = os.getenv("REDSHIFT_SCHEMA", "public")  

# Información del archivo Parquet en S3 y la tabla de destino en Redshift
s3_bucket_name = os.getenv("S3_BUCKET_NAME")


# Obtener la fecha actual
now = datetime.now()
year = now.year
month = now.strftime("%m")  # Formatea el mes con dos dígitos (ej: 05)
day = now.strftime("%d")    # Formatea el día con dos dígitos (ej: 09)

# Definir las tablas y sus correspondientes prefijos en S3
tables_config = {
    "co_clientes": f"curated/co_clientes/year={year}/month={month}/day={day}/",
    "co_proveedores": f"curated/co_proveedores/year={year}/month={month}/day={day}/",
    "co_transacciones": f"curated/co_transacciones/year={year}/month={month}/day={day}/",
}

if not all([host, port, dbname, user, password, s3_bucket_name]):
    print("Error: Faltan variables de entorno para la conexión o S3 bucket.")
else:
    redshift_engine = create_redshift_engine(host, int(port), dbname, user, password)
    if redshift_engine:
        for table_name, prefix_template in tables_config.items():
            s3_prefix = prefix_template.format(year=year, month=month, day=day)
            s3_path_for_copy = f"s3://{s3_bucket_name}/{s3_prefix}*"
            print(f"Cargando datos para la tabla: {table_name} desde: {s3_path_for_copy}")
            load_parquet_from_s3_to_redshift(redshift_engine, s3_path_for_copy, table_name, schema)