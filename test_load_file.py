import pytest
from unittest.mock import MagicMock, patch
import common


@pytest.fixture
def mock_env():
    mock_spark = MagicMock()
    mock_dbutils = MagicMock()
    mock_read_parquet = MagicMock()
    mock_df = MagicMock()

    mock_spark.read.parquet.return_value = mock_read_parquet
    mock_spark.read.format().option().option().load.return_value = mock_df

    return mock_spark, mock_dbutils, mock_read_parquet, mock_df


@patch("common.F.col", lambda x: MagicMock())
@patch("common.F.when", lambda condition, value: MagicMock())
@patch("common.F.concat_ws", lambda sep, *cols: MagicMock())
@patch("common.F.regexp_replace", lambda col, pattern, replacement: MagicMock())
@patch("common.read_query_from_postgres")
def test_load_file_success(mock_postgres, mock_env):
    mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

    mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []
    mock_dbutils.fs.ls.return_value = []

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


@patch("common.F.col", lambda x: MagicMock())
@patch("common.F.when", lambda condition, value: MagicMock())
@patch("common.F.concat_ws", lambda sep, *cols: MagicMock())
@patch("common.F.regexp_replace", lambda col, pattern, replacement: MagicMock())
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


@patch("common.F.col", lambda x: MagicMock())
@patch("common.F.when", lambda condition, value: MagicMock())
@patch("common.F.concat_ws", lambda sep, *cols: MagicMock())
@patch("common.F.regexp_replace", lambda col, pattern, replacement: MagicMock())
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


@patch("common.F.col", lambda x: MagicMock())
@patch("common.F.when", lambda condition, value: MagicMock())
@patch("common.F.concat_ws", lambda sep, *cols: MagicMock())
@patch("common.F.regexp_replace", lambda col, pattern, replacement: MagicMock())
@patch("common.read_query_from_postgres")
def test_load_file_zip_file(mock_postgres, mock_env):
    mock_spark, mock_dbutils, mock_read_parquet, mock_df = mock_env

    mock_postgres.return_value.select.return_value.distinct.return_value.collect.return_value = []

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
