import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from pyspark.sql import SparkSession, Row
import common


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


@pytest.fixture
def dummy_df(spark):
    return spark.createDataFrame([Row(file_col_name="col1", db_col_name="db_col1")])


@pytest.fixture
def dummy_measr_df(spark):
    return spark.createDataFrame([Row(measr_phys_name="measr1")])


@pytest.fixture
def mock_dbutils():
    return MagicMock()


@pytest.fixture
def mock_read_query(monkeypatch, dummy_measr_df):
    monkeypatch.setattr(common, "read_query_from_postgres", MagicMock(return_value=dummy_measr_df))


@pytest.fixture
def mock_parquet_reads(monkeypatch, dummy_df):
    # Mock spark.read.parquet
    parquet_mock = MagicMock(return_value=dummy_df)
    read_mock = MagicMock()
    read_mock.parquet = parquet_mock

    monkeypatch.setattr(common, "spark", MagicMock(read=read_mock))  # this works if common.spark exists

    # Also patch inside the test spark session’s read
    patcher = patch.object(SparkSession, 'read', new_callable=PropertyMock)
    patcher.start().return_value.parquet = parquet_mock

    yield

    patcher.stop()


@pytest.fixture
def mock_csv_reads(spark, dummy_df):
    csv_reader_mock = MagicMock()
    csv_reader_mock.option.return_value.option.return_value.load.return_value = dummy_df
    spark.read.format = MagicMock(return_value=csv_reader_mock)


@pytest.fixture
def mock_write(dummy_df):
    writer_mock = MagicMock()
    mode_mock = MagicMock()
    format_mock = MagicMock()
    save_mock = MagicMock()

    writer_mock.mode.return_value = mode_mock
    mode_mock.format.return_value = format_mock
    format_mock.save = save_mock

    patcher = patch.object(type(dummy_df), 'write', new_callable=PropertyMock, return_value=writer_mock)
    patcher.start()
    yield writer_mock
    patcher.stop()


def test_load_file_no_zip_found(spark, mock_dbutils, dummy_df, dummy_measr_df,
                                mock_read_query, mock_parquet_reads, mock_csv_reads, mock_write):
    mock_dbutils.fs.ls.return_value = []
    result = common.load_file("mkt", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
                              mock_dbutils, "schema", spark, "url", "db", "user", "pwd")
    assert result == "Success"


def test_load_file_zip_found(spark, mock_dbutils, dummy_df, dummy_measr_df,
                             mock_read_query, mock_parquet_reads, mock_csv_reads, mock_write):
    zip_file_mock = MagicMock()
    zip_file_mock.name = "data_RUN123.zip"
    zip_file_mock.path = "/mnt/tp-source-data/WORK/data_RUN123.zip"
    mock_dbutils.fs.ls.return_value = [zip_file_mock]

    result = common.load_file("prod", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
                              mock_dbutils, "schema", spark, "url", "db", "user", "pwd")
    assert result == "Success"


def test_load_file_fact_type(spark, mock_dbutils, dummy_df, dummy_measr_df,
                             mock_read_query, mock_parquet_reads, mock_csv_reads, mock_write):
    mock_dbutils.fs.ls.return_value = []

    result = common.load_file("fact", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
                              mock_dbutils, "schema", spark, "url", "db", "user", "pwd")
    assert result == "Success"


def test_load_file_time_type(spark, mock_dbutils, dummy_df, dummy_measr_df,
                             mock_read_query, mock_parquet_reads, mock_csv_reads, mock_write):
    mock_dbutils.fs.ls.return_value = []
    result = common.load_file("time", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
                              mock_dbutils, "schema", spark, "url", "db", "user", "pwd")
    assert result == "Success"


def test_load_file_invalid_type(spark, mock_dbutils, dummy_df, dummy_measr_df,
                                mock_read_query, mock_parquet_reads, mock_csv_reads, mock_write):
    mock_dbutils.fs.ls.return_value = []
    result = common.load_file("invalid", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
                              mock_dbutils, "schema", spark, "url", "db", "user", "pwd")
    assert result == "Success"
