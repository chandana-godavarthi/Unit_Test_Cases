import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import common

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestSession").getOrCreate()

@pytest.fixture
def mock_dbutils():
    dbutils = MagicMock()
    return dbutils

@pytest.fixture
def mock_read_query_from_postgres():
    with patch("common.read_query_from_postgres") as mock:
        yield mock

@pytest.fixture
def sample_col_mapping_df(spark):
    schema = StructType([
        StructField("cntrt_id", StringType(), True),
        StructField("dmnsn_name", StringType(), True),
        StructField("file_col_name", StringType(), True),
        StructField("db_col_name", StringType(), True),
    ])
    data = [("123", "PROD", "colA", "dbA")]
    return spark.createDataFrame(data, schema)

@pytest.fixture
def sample_measr_df(spark):
    schema = StructType([StructField("measr_phys_name", StringType(), True)])
    data = [("dbA",)]
    return spark.createDataFrame(data, schema)

@pytest.mark.parametrize("file_type", ['prod', 'fact', 'mkt', 'time'])
def test_load_file_success(spark, mock_dbutils, sample_col_mapping_df, sample_measr_df, mock_read_query_from_postgres, file_type):
    # Mock file system listing
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name="sample_123.zip", path="/mnt/tp-source-data/WORK/sample_123.zip")
    ]

    # Mock parquet read for column mapping
    spark.read.parquet = MagicMock(return_value=sample_col_mapping_df)

    # Mock read_query_from_postgres for measures
    mock_read_query_from_postgres.return_value = sample_measr_df

    # Mock CSV file reads
    spark.read.format().option().option().load = MagicMock(return_value=sample_col_mapping_df)

    result = common.load_file(
        file_type=file_type,
        RUN_ID="123",
        CNTRT_ID="123",
        STEP_FILE_PATTERN="sample_%_extr.csv",
        vendor_pattern="vendor",
        notebook_name="test",
        delimiter=",",
        dbutils=mock_dbutils,
        postgres_schema="schema",
        spark=spark,
        refDBjdbcURL="url",
        refDBname="dbname",
        refDBuser="user",
        refDBpwd="pwd"
    )
    assert result == "Success"

def test_load_file_no_zip_found(spark, mock_dbutils, sample_col_mapping_df, sample_measr_df, mock_read_query_from_postgres):
    mock_dbutils.fs.ls.return_value = []  # No files

    spark.read.parquet = MagicMock(return_value=sample_col_mapping_df)
    mock_read_query_from_postgres.return_value = sample_measr_df
    spark.read.format().option().option().load = MagicMock(return_value=sample_col_mapping_df)

    result = common.load_file(
        file_type="prod",
        RUN_ID="123",
        CNTRT_ID="123",
        STEP_FILE_PATTERN="sample_%_extr.csv",
        vendor_pattern="vendor",
        notebook_name="test",
        delimiter=",",
        dbutils=mock_dbutils,
        postgres_schema="schema",
        spark=spark,
        refDBjdbcURL="url",
        refDBname="dbname",
        refDBuser="user",
        refDBpwd="pwd"
    )
    assert result == "Success"

def test_load_file_invalid_file_type(spark, mock_dbutils, sample_col_mapping_df, sample_measr_df, mock_read_query_from_postgres):
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name="sample_123.zip", path="/mnt/tp-source-data/WORK/sample_123.zip")
    ]
    spark.read.parquet = MagicMock(return_value=sample_col_mapping_df)
    mock_read_query_from_postgres.return_value = sample_measr_df
    spark.read.format().option().option().load = MagicMock(return_value=sample_col_mapping_df)

    result = common.load_file(
        file_type="other",
        RUN_ID="123",
        CNTRT_ID="123",
        STEP_FILE_PATTERN="sample_%_extr.csv",
        vendor_pattern="vendor",
        notebook_name="test",
        delimiter=",",
        dbutils=mock_dbutils,
        postgres_schema="schema",
        spark=spark,
        refDBjdbcURL="url",
        refDBname="dbname",
        refDBuser="user",
        refDBpwd="pwd"
    )
    assert result == "Success"
