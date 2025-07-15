import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import Row

import common  # replace this with your actual module name if needed

# SparkSession fixture
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()

# Dummy DataFrame fixture
@pytest.fixture
def dummy_measr_df(spark):
    data = [("measure1",), ("measure2",)]
    return spark.createDataFrame(data, ["measr_phys_name"])

# Mock dbutils fixture
@pytest.fixture
def mock_dbutils():
    return MagicMock()

# Mocked DataFrame fixture with write chain mocked
@pytest.fixture
def mock_df():
    df = MagicMock()
    df.write.mode.return_value.format.return_value.save = MagicMock()
    df.withColumn.return_value = df
    df.columns = ['file_col_name', 'db_col_name']
    return df

# 1️⃣ Test no zip found
@patch('common.read_query_from_postgres')
def test_load_file_no_zip_found(mock_read_query, spark, mock_dbutils, mock_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name="data1.csv"), MagicMock(name="another.gz")
    ]

    spark.read.parquet = MagicMock(return_value=mock_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=mock_df))))
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "prod", "RUN123", "CNTRT1", "FACT_%", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'


# 2️⃣ Test zip found
@patch('common.read_query_from_postgres')
def test_load_file_zip_found(mock_read_query, spark, mock_dbutils, mock_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = [MagicMock(name="data_RUN123.zip", path="/mnt/tp-source-data/WORK/data_RUN123.zip")]

    spark.read.parquet = MagicMock(return_value=mock_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=mock_df))))
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "prod", "RUN123", "CNTRT1", "FACT_%", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'


# 3️⃣ Fact type
@patch('common.read_query_from_postgres')
def test_load_file_fact_type(mock_read_query, spark, mock_dbutils, mock_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []

    spark.read.parquet = MagicMock(return_value=mock_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=mock_df))))
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "prod", "RUN123", "CNTRT1", "FACT_TYPE", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'


# 4️⃣ Market type
@patch('common.read_query_from_postgres')
def test_load_file_mkt_type(mock_read_query, spark, mock_dbutils, mock_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []

    spark.read.parquet = MagicMock(return_value=mock_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=mock_df))))
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "prod", "RUN123", "CNTRT1", "MKT_TYPE", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'


# 5️⃣ Time type
@patch('common.read_query_from_postgres')
def test_load_file_time_type(mock_read_query, spark, mock_dbutils, mock_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []

    spark.read.parquet = MagicMock(return_value=mock_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=mock_df))))
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "prod", "RUN123", "CNTRT1", "TIME_TYPE", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'


# 6️⃣ Invalid file type
@patch('common.read_query_from_postgres')
def test_load_file_invalid_file_type(mock_read_query, spark, mock_dbutils, mock_df, dummy_measr_df):
    mock_dbutils.fs.ls.return_value = []

    spark.read.parquet = MagicMock(return_value=mock_df)
    spark.read.format = MagicMock(return_value=MagicMock(option=lambda *a, **k: MagicMock(load=MagicMock(return_value=mock_df))))
    mock_read_query.return_value = dummy_measr_df

    result = common.load_file(
        "prod", "RUN123", "CNTRT1", "INVALID_TYPE", "vendor", "notebook",
        ",", mock_dbutils, "schema", spark, "jdbcurl", "dbname", "user", "pwd"
    )
    assert result == 'Success'
