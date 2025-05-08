import pytest
from pathlib import Path
import sys
import os
import logging

# Añadir ruta para importar módulos personalizados
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from btg_DP.extra.read import read_csv_files
from pyspark.sql import SparkSession

# Crear SparkSession global como en el script principal
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("pytest-read_csv") \
    .getOrCreate()


def test_read_valid_csv(tmp_path: Path):
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("id,nombre\n1,Ana\n2,Juan")
    df = read_csv_files(f"file://{csv_file}", ",")
    assert df.count() == 2
    assert set(df.columns) == {"id", "nombre"}


def test_read_missing_csv():
    with pytest.raises(Exception):
        read_csv_files("file:///ruta/falsa.csv", ",")


def test_logging_on_success(tmp_path: Path, caplog):
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("id,nombre\n1,Ana\n2,Juan")

    with caplog.at_level(logging.INFO):
        df = read_csv_files(f"file://{csv_file}", ",")

    assert "Iniciando lectura de archivos CSV" in caplog.text
    assert "Archivo CSV leído correctamente." in caplog.text


def test_logging_on_failure(caplog):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            read_csv_files("file:///no_existe.csv", ",")

    assert "Error al leer el archivo CSV" in caplog.text
