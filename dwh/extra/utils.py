import os
import logging
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_redshift_engine(host: str, port: int, dbname: str, user: str, password: str) -> Engine | None:
    """
    Crea y devuelve un objeto Engine de SQLAlchemy para la conexión a Redshift.

    Args:
        host: Host de la instancia de Redshift.
        port: Puerto de la instancia de Redshift.
        dbname: Nombre de la base de datos de Redshift.
        user: Usuario de Redshift.
        password: Contraseña del usuario de Redshift.

    Returns:
        Un objeto Engine si la conexión es exitosa, None en caso de error.
    """
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    try:
        engine = create_engine(conn_str)
        # Prueba la conexión
        with engine.connect() as connection:
            logger.info("Conexión a Redshift establecida exitosamente.")
        return engine
    except Exception as e:
        logger.error(f"Error al crear el engine de Redshift: {e}")
        return None

def load_parquet_from_s3_to_redshift(engine: Engine, s3_path: str, redshift_table: str, redshift_schema: str) -> None:
    """
    Carga archivos Parquet desde una carpeta en S3 a una tabla de Redshift utilizando la función COPY.
    Requiere un objeto Engine de SQLAlchemy ya conectado a Redshift.

    Args:
        engine: Objeto Engine de SQLAlchemy conectado a Redshift.
        s3_path: Ruta al prefijo de la carpeta en S3 (e.g., 's3://your-bucket/path/to/').
                 Se utilizará un comodín '*' al final para incluir todos los archivos dentro.
        redshift_table: Nombre de la tabla de destino en Redshift.
        redshift_schema: Esquema de la tabla de destino en Redshift.
    """
    if engine is None:
        logger.error("No se proporcionó un engine de Redshift válido.")
        return

    try:
        with engine.connect() as connection:
            copy_sql = f"""
                COPY {redshift_schema}.{redshift_table}
                FROM '{s3_path}'
                FORMAT PARQUET;
            """
            logger.info(f"Ejecutando consulta COPY para la carpeta S3: {s3_path}")
            connection.execute(text(copy_sql))
            connection.commit()
        logger.info(f"Carga exitosa de los archivos Parquet desde '{s3_path}' a '{redshift_schema}.{redshift_table}'.")
    except Exception as e:
        logger.error(f"Error al cargar los archivos Parquet desde S3 a Redshift: {e}")
        raise

