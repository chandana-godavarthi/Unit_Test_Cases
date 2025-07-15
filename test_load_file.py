import pytest
from unittest.mock import MagicMock, patch
import common


@pytest.fixture
def mock_env():
    mock_spark = MagicMock()
    mock_dbutils = MagicMock()

    mock_read_parquet = MagicMock()
    mock_spark.read.parquet.return_value = mock_read_parquet
    mock_read_parquet.filter.return_value.where.return_value = mock_read_parquet
    mock_read_parquet.select.return_value.distinct.return_value = mock_read_parquet
    mock_read_parquet.columns = ['col1', 'col2']

    mock_dbutils.fs.ls.return_value = []

    mock_df = MagicMock()
    mock_df.columns = ['col1', 'col2']
    mock_spark.read.format().option().option().load.return_value = mock_df

    mock_spark.sql.return_value.collect.return_value = []
    mock_df.withColumnRenamed.side_effect = lambda x, y: mock_df
    mock_df.withColumn.side_effect = lambda *args, **kwargs: mock_df
    mock_df.drop.side_effect = lambda *args: mock_df
    mock_df.distinct.return_value = mock_df
    mock_df.dropna.return_value = mock_df

    return mock_spark, mock_dbutils, mock_read_parquet, mock_df


@patch("common.read_query_from_postgres")
def test_load_file_success(mock_postgres, mock_env):
    mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

    mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []

    result = common.load_file(
        file_type="prod",
        RUN_ID="123",
        CNTRT_ID="CONTRACT",
        STEP_FILE_PATTERN="file_%.csv",
        vendor_pattern="vendor",
        notebook_name="test_notebook",
        delimiter=",",
        dbutils=mock_dbutils,
        postgres_schema="schema",
        spark=mock_spark,
        refDBjdbcURL="url",
        refDBname="dbname",
        refDBuser="user",
        refDBpwd="pwd"
    )

    assert result == "Success"
    mock_spark.read.parquet.assert_called_once()
    mock_postgres.assert_called_once()


@patch("common.read_query_from_postgres")
def test_load_file_fact_type(mock_postgres, mock_env):
    mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

    mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []
    mock_dbutils.fs.ls.return_value = []

    result = common.load_file(
        file_type="fact",
        RUN_ID="123",
        CNTRT_ID="CONTRACT",
        STEP_FILE_PATTERN="file_%.csv",
        vendor_pattern="vendor",
        notebook_name="test_notebook",
        delimiter=",",
        dbutils=mock_dbutils,
        postgres_schema="schema",
        spark=mock_spark,
        refDBjdbcURL="url",
        refDBname="dbname",
        refDBuser="user",
        refDBpwd="pwd"
    )

    assert result == "Success"
    mock_postgres.assert_called_once()


def test_load_file_invalid_type(mock_env):
    mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

    result = common.load_file(
        file_type="unknown",
        RUN_ID="123",
        CNTRT_ID="CONTRACT",
        STEP_FILE_PATTERN="file_%.csv",
        vendor_pattern="vendor",
        notebook_name="test_notebook",
        delimiter=",",
        dbutils=mock_dbutils,
        postgres_schema="schema",
        spark=mock_spark,
        refDBjdbcURL="url",
        refDBname="dbname",
        refDBuser="user",
        refDBpwd="pwd"
    )

    assert result == "Success"


@patch("common.read_query_from_postgres")
def test_load_file_zip_file(mock_postgres, mock_env):
    mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

    mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []

    # Mock file list with zip file containing RUN_ID
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name="file1.zip", path="/mnt/tp-source-data/WORK/file1.zip")
    ]

    result = common.load_file(
        file_type="prod",
        RUN_ID="file1",
        CNTRT_ID="CONTRACT",
        STEP_FILE_PATTERN="file_%.csv",
        vendor_pattern="vendor",
        notebook_name="test_notebook",
        delimiter=",",
        dbutils=mock_dbutils,
        postgres_schema="schema",
        spark=mock_spark,
        refDBjdbcURL="url",
        refDBname="dbname",
        refDBuser="user",
        refDBpwd="pwd"
    )

    assert result == "Success"
    mock_postgres.assert_called_once()

