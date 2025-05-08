import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import LongType
import sys
import os

# Añadir ruta para importaciones personalizadas
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Importación de funciones a probar
from btg_DP.extra.transform_data import (
    convertir_fecha,
    quitar_tildes,
    reemplazar_nulos,
    filtrar_emails_validos
)

# Crear SparkSession global para pruebas
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("pytest-transformaciones") \
    .getOrCreate()

# Fixtures

@pytest.fixture
def mock_dataframe():
    """Simula un DataFrame para las pruebas con emails válidos e inválidos"""
    data = [
        ("user@example.com",),        # Válido
        ("user@domain.co",),          # Válido
        ("invalid-email@",),          # Inválido
        ("@invalid.com",),            # Inválido
        ("user.name@domain.com",),    # Válido
        ("missingdomain@.com",),      # Inválido
        ("test@valid.com",)           # Válido
    ]
    df = spark.createDataFrame(data, ["email"])
    return df

# Pruebas

# Test para convertir_fecha sobrescribiendo columna
def test_convertir_fecha_sobrescribe_columna():
    data = [("2024-05-07 12:34:56",), ("2024-01-01 00:00:00",)]
    df = spark.createDataFrame(data, ["timestamp_col"])

    df_result = convertir_fecha(df, "timestamp_col")

    assert "timestamp_col" in df_result.columns
    assert df_result.select("timestamp_col").first()[0].__class__.__name__ == "date"

# Test para convertir_fecha con nueva columna
def test_convertir_fecha_nueva_columna():
    data = [("2024-05-07 12:34:56",)]
    df = spark.createDataFrame(data, ["fecha_completa"])

    df_result = convertir_fecha(df, "fecha_completa", "solo_fecha")

    assert "fecha_completa" in df_result.columns
    assert "solo_fecha" in df_result.columns
    assert df_result.select("solo_fecha").first()[0].__class__.__name__ == "date"

# Test para convertir_fecha con columna inexistente
def test_convertir_fecha_columna_inexistente():
    df = spark.createDataFrame([(1,)], ["columna_dummy"])

    with pytest.raises(Exception):
        convertir_fecha(df, "no_existe")

# Test para quitar_tildes sobrescribiendo columna
def test_quitar_tildes_sobrescribe_columna():
    data = [("José",), ("María",), ("Ñandú",)]
    df = spark.createDataFrame(data, ["nombre"])

    df_result = quitar_tildes(df, "nombre")

    resultados = [row["nombre"] for row in df_result.collect()]
    assert resultados == ["Jose", "Maria", "Nandu"]

# Test para quitar_tildes con nueva columna
def test_quitar_tildes_nueva_columna():
    data = [("Óscar",)]
    df = spark.createDataFrame(data, ["nombre"])

    df_result = quitar_tildes(df, "nombre", "nombre_limpio")

    row = df_result.collect()[0]
    assert row["nombre"] == "Óscar"
    assert row["nombre_limpio"] == "Oscar"

# Test para filtrar emails válidos
def test_filtrar_emails_validos(mock_dataframe):
    """
    Verifica que la función filtrar_emails_validos filtre correctamente los correos electrónicos inválidos.
    """
    # Llamar a la función que estamos probando
    df_result = filtrar_emails_validos(mock_dataframe, "email")

    # Verificar que los emails válidos estén presentes en el DataFrame
    result_values = [row["email"] for row in df_result.collect()]
    valid_emails = ["user@example.com", "user@domain.co", "user.name@domain.com", "test@valid.com"]

    # Comprobamos que los correos válidos estén en el resultado
    assert set(result_values) == set(valid_emails)

# Test para columna inexistente en filtrar_emails_validos
def test_columna_no_existente(mock_dataframe):
    """
    Verifica que la función lance un error si la columna no existe en el DataFrame.
    """
    with pytest.raises(AnalysisException):
        filtrar_emails_validos(mock_dataframe, "columna_inexistente")
