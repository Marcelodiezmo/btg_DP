import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, initcap, regexp_extract, when, length, substring,translate,to_date,trim

# Configurar logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def capitalizar_columna(df: DataFrame, columna: str = "nombre") -> DataFrame:
    try:
        logger.info(f"Aplicando capitalización a la columna '{columna}'")
        return df.withColumn(columna, initcap(col(columna)))
    except Exception as e:
        logger.error(f"Error en capitalizar_columna({columna}): {e}")
        raise

def eliminar_nulos(df: DataFrame) -> DataFrame:
    try:
        logger.info("Eliminando filas con valores nulos")
        return df.dropna()
    except Exception as e:
        logger.error(f"Error en eliminar_nulos: {e}")
        raise

def filtrar_emails_validos(df: DataFrame, columna: str = "email") -> DataFrame:
    try:
        logger.info(f"Filtrando correos electrónicos inválidos en columna '{columna}'")
        return df.filter(
            regexp_extract(col(columna), r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", 0) != ""
        )
    except Exception as e:
        logger.error(f"Error en filtrar_emails_validos({columna}): {e}")
        raise

def limpiar_telefonos(df: DataFrame, columna: str = "telefono") -> DataFrame:
    try:
        logger.info(f"Normalizando y filtrando columna '{columna}'")
        df = df.withColumn(
            columna,
            when(length(col(columna)) == 12, substring(col(columna), 3, 10))
            .otherwise(col(columna))
        )
        df = df.filter(length(col(columna)) == 10)
        return df
    except Exception as e:
        logger.error(f"Error en limpiar_telefonos({columna}): {e}")
        raise



def quitar_tildes(df, columna_origen: str, columna_destino: str = None):
    """
    Reemplaza tildes comunes en una columna usando translate (más rápido que UDF).

    Args:
        df: DataFrame de entrada.
        columna_origen: Nombre de la columna a limpiar.
        columna_destino: Nombre de la columna resultante. Si es None, sobreescribe.

    Returns:
        DataFrame con columna limpia.
    """
    origen = "áéíóúÁÉÍÓÚñÑ"
    destino = "aeiouAEIOUnN"
    columna_destino = columna_destino or columna_origen

    return df.withColumn(
        columna_destino,
        translate(col(columna_origen), origen, destino)
    )



def convertir_fecha(df: DataFrame, columna: str, nueva_columna: str = None) -> DataFrame:
    """
    Convierte una columna tipo timestamp a solo fecha.

    Parámetros:
        df (DataFrame): DataFrame de entrada.
        columna (str): Nombre de la columna con timestamp.
        nueva_columna (str, opcional): Nombre de la nueva columna. Si es None, se reemplaza la original.

    Retorna:
        DataFrame con la columna modificada o agregada.
    """
    try:
        col_destino = nueva_columna if nueva_columna else columna
        return df.withColumn(col_destino, to_date(columna))
    except Exception as e:
        print(f"Error al convertir la columna {columna} a fecha: {e}")
        raise



def convertir_palabra_a_numero(palabra):
    mapeo_numeros = {
        "cero": 0, "uno": 1, "dos": 2, "tres": 3, "cuatro": 4,
        "cinco": 5, "seis": 6, "siete": 7, "ocho": 8, "nueve": 9,
        "diez": 10, "once": 11, "doce": 12, "trece": 13, "catorce": 14,
        "quince": 15, "dieciséis": 16, "diecisiete": 17, "dieciocho": 18,
        "diecinueve": 19, "veinte": 20, "veintiuno": 21, "veintidós": 22,
        "veintitrés": 23, "veinticuatro": 24, "veinticinco": 25,
        "veintiséis": 26, "veintisiete": 27, "veintiocho": 28,
        "veintinueve": 29, "treinta": 30, "cuarenta": 40, "cincuenta": 50,
        "sesenta": 60, "setenta": 70, "ochenta": 80, "noventa": 90,
        "cien": 100, "doscientos": 200, "trescientos": 300,
        "cuatrocientos": 400, "quinientos": 500, "seiscientos": 600,
        "setecientos": 700, "ochocientos": 800, "novecientos": 900,
        "mil": 1000
    }

    try:
        if isinstance(palabra, str):
            palabra_lower = palabra.lower()
            if palabra_lower in mapeo_numeros:
                return float(mapeo_numeros[palabra_lower])
            else:
                return float(palabra.replace(",", "."))  # manejar strings numéricos
        elif isinstance(palabra, (int, float)):
            return float(palabra)
    except:
        return None  # si no se puede convertir, retorna nulo
    

def reemplazar_nulos(df: DataFrame, nombre_columna: str) -> DataFrame:
    """
    Reemplaza valores nulos o vacíos en una columna de un DataFrame de PySpark por 0.

    Args:
        df (DataFrame): El DataFrame de PySpark.
        nombre_columna (str): Nombre de la columna a procesar.

    Returns:
        DataFrame: Nuevo DataFrame con los valores nulos/vacíos reemplazados por 0 en la columna especificada.
    """
    try:
        if nombre_columna not in df.columns:
            logger.error(f"La columna '{nombre_columna}' no existe en el DataFrame.")
            raise ValueError(f"La columna '{nombre_columna}' no existe en el DataFrame.")
        
        logger.info(f"Reemplazando nulos y vacíos por 0 en la columna: {nombre_columna}")

        df = df.withColumn(
            nombre_columna,
            when(col(nombre_columna).isNull() | (trim(col(nombre_columna)) == ""), 0).otherwise(col(nombre_columna))
        )

        logger.info(f"Transformación completada para la columna '{nombre_columna}'.")

        return df

    except Exception as e:
        logger.exception(f"Ocurrió un error al procesar la columna '{nombre_columna}': {e}")
        raise