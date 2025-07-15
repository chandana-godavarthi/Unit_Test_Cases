import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
import common  # Replace with your actual module name if different

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()

@pytest.fixture
def mock_dbutils():
    return MagicMock()

@pytest.fixture
def dummy_df(spark):
    schema = StructType([
        StructField("file_col_name", StringType(), True),
        StructField("db_col_name", StringType(), True)
    ])
    data = [("col1", "db1"), ("col2", "db2")]
    return spark.createDataFrame(data, schema)

@pytest.fixture
def dummy_measr_df(spark):
    schema = StructType([
        StructField("measr_phys_name", StringType(), True)
    ])
    data = [("db1",), ("db2",)]
    return spark.createDataFrame(data, schema)

@patch('common.read_query_from_postgres')
def test_load_file_no_zip_found(mock_read_query, spark, mock_dbutils, mock_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = [
        MagicMock(), MagicMock()
    ]
    mock_dbutils.fs.ls.return_value[0].name = "data1.csv"
    mock_dbutils.fs.ls.return_value[1].name = "another.gz"

    spark.read.parquet = MagicMock(return_value=mock_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=mock_df))))
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "prod", "RUN123", "CNTRT1", "FACT_%", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'


@patch('common.read_query_from_postgres')
def test_load_file_zip_found(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = [MagicMock()]
    mock_dbutils.fs.ls.return_value[0].name = "data_RUN123.zip"
    mock_dbutils.fs.ls.return_value[0].path = "/mnt/tp-source-data/WORK/data_RUN123.zip"

    spark.read.parquet = MagicMock(return_value=dummy_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=dummy_df))))
    dummy_df.write.mode.return_value.format.return_value.save = MagicMock()
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "prod", "RUN123", "CNTRT1", "FACT_%", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'

@patch('common.read_query_from_postgres')
def test_load_file_fact_type(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []

    spark.read.parquet = MagicMock(return_value=dummy_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=dummy_df))))
    dummy_df.write.mode.return_value.format.return_value.save = MagicMock()
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "fact", "RUN123", "CNTRT1", "FACT_%", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'

@patch('common.read_query_from_postgres')
def test_load_file_mkt_type(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []
    spark.read.parquet = MagicMock(return_value=dummy_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=dummy_df))))
    dummy_df.write.mode.return_value.format.return_value.save = MagicMock()
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "mkt", "RUN123", "CNTRT1", "FACT_%", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'

@patch('common.read_query_from_postgres')
def test_load_file_time_type(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []
    spark.read.parquet = MagicMock(return_value=dummy_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=dummy_df))))
    dummy_df.write.mode.return_value.format.return_value.save = MagicMock()
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "time", "RUN123", "CNTRT1", "FACT_%", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'

@patch('common.read_query_from_postgres')
def test_load_file_invalid_file_type(mock_read_query, spark, mock_dbutils, dummy_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []
    spark.read.parquet = MagicMock(return_value=dummy_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=dummy_df))))
    dummy_df.write.mode.return_value.format.return_value.save = MagicMock()
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "unknown", "RUN123", "CNTRT1", "FACT_%", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'
