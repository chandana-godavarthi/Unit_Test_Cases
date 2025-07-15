import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

# ✅ Import your function under test from common.py
from common import load_file


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("unit-tests").getOrCreate()


@pytest.fixture
def mock_dbutils():
    return MagicMock()


@pytest.fixture
def dummy_df(spark):
    data = [("file_col_value1", "db_col_value1")]
    schema = ["file_col_name", "db_col_name"]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def dummy_measr_df(spark):
    data = [("measr_phys_value1",)]
    schema = ["measr_phys_name"]
    return spark.createDataFrame(data, schema)


@pytest.fixture(autouse=True)
def mock_spark_parquet(monkeypatch, dummy_df):
    monkeypatch.setattr("pyspark.sql.DataFrameReader.parquet", lambda self, path: dummy_df)


@pytest.fixture(autouse=True)
def mock_spark_format(monkeypatch, dummy_df):
    mock_format_reader = MagicMock()
    mock_format_reader.option.return_value.load.return_value = dummy_df
    monkeypatch.setattr("pyspark.sql.DataFrameReader.format", lambda self, fmt: mock_format_reader)


@pytest.fixture(autouse=True)
def mock_write_operations(monkeypatch):
    mock_writer = MagicMock()
    mock_writer.format.return_value.save.return_value = None
    mock_mode = MagicMock(return_value=mock_writer)
    monkeypatch.setattr("pyspark.sql.DataFrameWriter.mode", lambda self, mode: mock_mode)


@patch('common.read_query_from_postgres')
def test_load_file_no_zip_found(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []
    mock_read_query.return_value = dummy_df

    result = load_file("RUN123", "fact", mock_dbutils, spark)
    assert result is None


@patch('common.read_query_from_postgres')
def test_load_file_zip_found(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_file = MagicMock()
    mock_file.name = "data_RUN123.zip"
    mock_file.path = "/mnt/tp-source-data/WORK/data_RUN123.zip"
    mock_dbutils.fs.ls.return_value = [mock_file]

    mock_read_query.return_value = dummy_df

    result = load_file("RUN123", "fact", mock_dbutils, spark)
    assert result is None


@patch('common.read_query_from_postgres')
def test_load_file_fact_type(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []
    mock_read_query.return_value = dummy_df

    result = load_file("RUN123", "fact", mock_dbutils, spark)
    assert result is None


@patch('common.read_query_from_postgres')
def test_load_file_mkt_type(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []
    mock_read_query.return_value = dummy_df

    result = load_file("RUN123", "mkt", mock_dbutils, spark)
    assert result is None


@patch('common.read_query_from_postgres')
def test_load_file_time_type(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []
    mock_read_query.return_value = dummy_df

    result = load_file("RUN123", "time", mock_dbutils, spark)
    assert result is None


@patch('common.read_query_from_postgres')
def test_load_file_invalid_file_type(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []
    mock_read_query.return_value = dummy_df

    result = load_file("RUN123", "invalid_type", mock_dbutils, spark)
    assert result is None
