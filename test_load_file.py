import pytest
from unittest import mock
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from common import load_file

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()

@pytest.fixture
def dummy_df(spark):
    schema = StructType([
        StructField("file_col_name", StringType(), True),
        StructField("db_col_name", StringType(), True)
    ])
    data = [("col1", "db_col1"), ("col2", "db_col2")]
    return spark.createDataFrame(data, schema)

@pytest.fixture
def dummy_measr_df(spark):
    schema = StructType([StructField("measr_phys_name", StringType(), True)])
    data = [("measr1",), ("measr2",)]
    return spark.createDataFrame(data, schema)

@pytest.fixture
def mock_dbutils():
    dbutils = MagicMock()
    dbutils.fs.ls.return_value = []
    return dbutils

@pytest.fixture
def mock_parquet_reader(dummy_df):
    with patch("pyspark.sql.readwriter.DataFrameReader.parquet", return_value=dummy_df):
        yield

@pytest.fixture
def mock_read_query_postgres(dummy_measr_df):
    with patch("common.read_query_from_postgres", return_value=dummy_measr_df):
        yield

@pytest.fixture
def mock_csv_reader(dummy_df):
    with patch("pyspark.sql.readwriter.DataFrameReader.format") as mock_format:
        mock_reader = mock_format.return_value
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = dummy_df
        yield

@pytest.fixture
def mock_sql(spark):
    with patch.object(SparkSession, 'sql', return_value=spark.createDataFrame([], StructType([]))):
        yield

@pytest.mark.parametrize("file_type", ["prod", "mkt", "fact", "time", "unknown"])
def test_load_file_all_types(spark, mock_dbutils, mock_parquet_reader, 
                             mock_csv_reader, mock_read_query_postgres, 
                             file_type, mock_sql):
    result = load_file(
        file_type=file_type,
        RUN_ID="12345",
        CNTRT_ID="6789",
        STEP_FILE_PATTERN="testfile_%.csv",
        vendor_pattern="vendor",
        notebook_name="test_notebook",
        delimiter=",",
        dbutils=mock_dbutils,
        postgres_schema="public",
        spark=spark,
        refDBjdbcURL="jdbc:postgresql://localhost:5432/",
        refDBname="testdb",
        refDBuser="testuser",
        refDBpwd="testpass"
    )

    assert result == "Success"

@pytest.mark.parametrize("file_type", ["prod", "mkt", "fact", "time"])
def test_load_file_no_runid_file_found(spark, mock_dbutils, mock_parquet_reader,
                                       mock_csv_reader, mock_read_query_postgres,
                                       file_type, mock_sql):
    # No files in dbutils.fs.ls — should still work fine
    mock_dbutils.fs.ls.return_value = []

    result = load_file(
        file_type=file_type,
        RUN_ID="99999",
        CNTRT_ID="111",
        STEP_FILE_PATTERN="data_%.csv",
        vendor_pattern="vendor",
        notebook_name="test_notebook",
        delimiter=",",
        dbutils=mock_dbutils,
        postgres_schema="public",
        spark=spark,
        refDBjdbcURL="jdbc:postgresql://localhost:5432/",
        refDBname="testdb",
        refDBuser="testuser",
        refDBpwd="testpass"
    )

    assert result == "Success"

def test_load_file_no_column_mapping(spark, mock_dbutils, mock_read_query_postgres, mock_sql):
    empty_df = spark.createDataFrame([], StructType([
        StructField("file_col_name", StringType(), True),
        StructField("db_col_name", StringType(), True)
    ]))

    with patch("pyspark.sql.readwriter.DataFrameReader.parquet", return_value=empty_df):
        with patch("pyspark.sql.readwriter.DataFrameReader.format") as mock_format:
            mock_reader = mock_format.return_value
            mock_reader.option.return_value = mock_reader
            mock_reader.load.return_value = empty_df

            result = load_file(
                file_type="prod",
                RUN_ID="12345",
                CNTRT_ID="6789",
                STEP_FILE_PATTERN="file_%.csv",
                vendor_pattern="vendor",
                notebook_name="test_notebook",
                delimiter=",",
                dbutils=mock_dbutils,
                postgres_schema="public",
                spark=spark,
                refDBjdbcURL="jdbc:postgresql://localhost:5432/",
                refDBname="testdb",
                refDBuser="testuser",
                refDBpwd="testpass"
            )

            assert result == "Success"
