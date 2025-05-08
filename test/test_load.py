import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from btg_DP.extra.load import save_table

# Configurar SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("pytest-save_table") \
    .getOrCreate()

@pytest.fixture
def mock_dataframe():
    """Simula un DataFrame para las pruebas"""
    df = MagicMock()
    df.write = MagicMock()
    return df

def test_save_table(mock_dataframe):
    """
    Verifica que save_table llame al método write de forma correcta.
    """
    # Mock de los métodos de escritura
    mock_write = mock_dataframe.write

    # Parámetros de prueba
    db_name = "test_db"
    table_name = "test_table"
    path = "s3://bucket/test_path"
    file_format = "parquet"
    write_method = "append"
    partition_cols = ["col1", "col2"]

    # Llamada a la función
    save_table(mock_dataframe, db_name, table_name, path, file_format, write_method, partition_cols)

    # Verificar que los métodos han sido llamados correctamente
    mock_write.partitionBy.assert_called_with("col1", "col2")
    mock_write.mode.assert_called_with(write_method)  # Aquí estamos esperando que el método mode haya sido llamado con "append"
    mock_write.format.assert_called_with(file_format)
    mock_write.saveAsTable.assert_called_with(f"{db_name}.{table_name}")