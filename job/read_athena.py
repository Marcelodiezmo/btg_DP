import boto3
import pandas as pd
import time
import io
import logging
from typing import Optional

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración de constantes
REGION = 'us-east-1'
DATABASE = 'energy_data'
S3_OUTPUT = 's3://energia-btgpactual/curated/co_clientes/'


def query_athena(query: str, database: str, s3_output: str, region: str) -> Optional[str]:
    """
    Ejecuta una consulta en Amazon Athena y retorna el ID de ejecución.

    Args:
        query: Consulta SQL.
        database: Nombre de la base de datos de Athena.
        s3_output: Ruta S3 para los resultados.
        region: Región de AWS.

    Returns:
        ID de ejecución de la consulta si fue exitosa, None si falló.
    """
    athena_client = boto3.client('athena', region_name=region)
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_output}
        )
        execution_id = response['QueryExecutionId']
        logger.info(f"Consulta iniciada con ID: {execution_id}")
        return execution_id
    except Exception as e:
        logger.error(f"Error al ejecutar la consulta: {e}")
        return None


def wait_result_query(athena_client, execution_id: str, sleep_time: int = 2) -> bool:
    """
    Espera a que la consulta de Athena finalice.

    Args:
        athena_client: Cliente de boto3 para Athena.
        execution_id: ID de ejecución de la consulta.
        sleep_time: Tiempo de espera entre chequeos.

    Returns:
        True si la consulta fue exitosa, False si falló.
    """
    status = 'RUNNING'
    while status in ['RUNNING', 'QUEUED']:
        response = athena_client.get_query_execution(QueryExecutionId=execution_id)
        status = response['QueryExecution']['Status']['State']
        logger.info(f"Estado de la consulta: {status}")
        time.sleep(sleep_time)

    return status == 'SUCCEEDED'


def read_s3(s3_output: str, execution_id: str) -> pd.DataFrame:
    """
    Descarga el archivo de resultados de S3 y lo carga como un DataFrame de pandas.

    Args:
        s3_output: Ruta S3 base.
        execution_id: ID de ejecución para construir el path al resultado.

    Returns:
        DataFrame con los resultados.
    """
    result_path = f"{s3_output}{execution_id}.csv"
    bucket_name = result_path.replace("s3://", "").split("/")[0]
    key = "/".join(result_path.replace("s3://", "").split("/")[1:])

    s3_client = boto3.client('s3')
    logger.info(f"Descargando resultados desde: s3://{bucket_name}/{key}")
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    df = pd.read_csv(io.BytesIO(response['Body'].read()))
    return df


def main() -> None:
    query = '''
    SELECT
        t.transaccion_id,
        c.nombre AS nombre_cliente,
        p.nombre AS nombre_proveedor,
        t.fecha,
        t.valor,
        t.consumo_kwh
    FROM
        co_transacciones t
    LEFT JOIN co_clientes c ON t.cliente_id = c.cliente_id
    LEFT JOIN co_proveedores p ON t.proveedor_id = p.proveedor_id
    WHERE c.nombre IS NOT NULL
    ORDER BY
        t.fecha DESC
    '''

    execution_id = query_athena(query, DATABASE, S3_OUTPUT, REGION)
    if execution_id is None:
        logger.error("No se pudo ejecutar la consulta.")
        return

    athena_client = boto3.client('athena', region_name=REGION)
    if wait_result_query(athena_client, execution_id):
        df_resultado = read_s3(S3_OUTPUT, execution_id)
        logger.info("Consulta completada exitosamente.")
        print(df_resultado.head())
    else:
        logger.error("La consulta no se completó con éxito.")


if __name__ == '__main__':
    main()
