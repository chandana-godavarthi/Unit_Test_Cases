import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from pyspark.sql import SparkSession
from pyspark.sql import Row
import common


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()


@pytest.fixture
def dummy_df(spark):
    data = [("col1", "col2"), ("val1", "val2")]
    columns = ["file_col_name", "db_col_name"]
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


def setup_common_mocks(spark, mock_dbutils, dummy_df):
    spark.read.parquet = MagicMock(return_value=dummy_df)
    spark.read.format = MagicMock(
        return_value=MagicMock(option=lambda *args, **kwargs:
                               MagicMock(option=lambda *args, **kwargs:
                                         MagicMock(load=MagicMock(return_value=dummy_df))))
    )
    mock_dbutils.fs.ls.return_value = []


@pytest.fixture
def mock_write(monkeypatch):
    writer_mock = MagicMock()
    writer_mock.mode.return_value.format.return_value.save = MagicMock()

    with patch("pyspark.sql.DataFrame.write", new_callable=PropertyMock) as mock_write_prop:
        mock_write_prop.return_value = writer_mock
        yield writer_mock


def test_load_file_no_zip_found(spark, mock_dbutils, dummy_df, dummy_measr_df, mock_read_query, mock_write):
    setup_common_mocks(spark, mock_dbutils, dummy_df)
    result = common.load_file(
        "mkt", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
        mock_dbutils, "schema", spark, "url", "db", "user", "pwd"
    )
    assert result == "Success"


def test_load_file_zip_found(spark, mock_dbutils, dummy_df, dummy_measr_df, mock_read_query, mock_write):
    mock_dbutils.fs.ls.return_value = [MagicMock(name="data_RUN123.zip", path="/mnt/tp-source-data/WORK/data_RUN123.zip")]
    spark.read.parquet = MagicMock(return_value=dummy_df)
    spark.read.format = MagicMock(
        return_value=MagicMock(option=lambda *args, **kwargs:
                               MagicMock(option=lambda *args, **kwargs:
                                         MagicMock(load=MagicMock(return_value=dummy_df))))
    )
    result = common.load_file(
        "prod", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
        mock_dbutils, "schema", spark, "url", "db", "user", "pwd"
    )
    assert result == "Success"


def test_load_file_fact_type(spark, mock_dbutils, dummy_df, dummy_measr_df, mock_read_query, mock_write):
    setup_common_mocks(spark, mock_dbutils, dummy_df)
    result = common.load_file(
        "fact", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
        mock_dbutils, "schema", spark, "url", "db", "user", "pwd"
    )
    assert result == "Success"


def test_load_file_time_type(spark, mock_dbutils, dummy_df, dummy_measr_df, mock_read_query, mock_write):
    setup_common_mocks(spark, mock_dbutils, dummy_df)
    result = common.load_file(
        "time", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
        mock_dbutils, "schema", spark, "url", "db", "user", "pwd"
    )
    assert result == "Success"


def test_load_file_invalid_type(spark, mock_dbutils, dummy_df, dummy_measr_df, mock_read_query, mock_write):
    setup_common_mocks(spark, mock_dbutils, dummy_df)
    result = common.load_file(
        "invalid", "RUN123", "C123", "STEP%", "vendor", "notebook", ",",
        mock_dbutils, "schema", spark, "url", "db", "user", "pwd"
    )
    assert result == "Success"
