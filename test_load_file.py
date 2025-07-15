import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock
from common import load_file
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()

@pytest.fixture
def mock_dbutils():
    return MagicMock()

@pytest.fixture
def mock_parquet_reader(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr("common.read_parquet_files", mock)
    return mock

@pytest.fixture
def mock_csv_reader(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr("common.read_csv_files", mock)
    return mock

@pytest.fixture
def mock_read_query_postgres(monkeypatch, spark):
    mock = MagicMock()
    # Mocked DataFrame to match production schema
    schema = StructType([
        StructField("cntrt_id", IntegerType(), True),
        StructField("dmnsn_name", StringType(), True),
        StructField("file_col_name", StringType(), True),
        StructField("db_col_name", StringType(), True)
    ])
    data = [
        (6789, "some_dmnsn", "file_col_name", "db_col_name")
    ]
    df = spark.createDataFrame(data, schema)
    mock.return_value = df
    monkeypatch.setattr("common.read_query_postgres", mock)
    return mock

@pytest.fixture
def mock_sql(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr("common.run_sql_query", mock)
    return mock

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
    # Basic assertion: result is a DataFrame if expected, or None for unknown type
    if file_type in ["prod", "mkt", "fact", "time"]:
        assert result is not None
        assert result.columns  # DataFrame has columns
    else:
        assert result is None

def test_load_file_no_zip_found(spark, mock_dbutils, mock_parquet_reader,
                                mock_read_query_postgres, mock_sql):
    mock_dbutils.fs.ls.return_value = []  # no files found
    result = load_file(
        file_type="prod",
        RUN_ID="12345",
        CNTRT_ID="6789",
        STEP_FILE_PATTERN="no_match_%.csv",
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
    assert result is None

def test_load_file_with_zip_found(spark, mock_dbutils, mock_parquet_reader,
                                  mock_read_query_postgres, mock_sql):
    # Simulate one matching file
    file_info = MagicMock()
    file_info.name = "vendor_testfile_20240715.zip"
    mock_dbutils.fs.ls.return_value = [file_info]

    result = load_file(
        file_type="prod",
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
    assert result is not None
    assert result.columns

