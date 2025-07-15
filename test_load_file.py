import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, Row
from common import load_file, get_dbutils, read_run_params
import argparse


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("TestSession").getOrCreate()


@pytest.fixture
def mock_dbutils():
    mock = MagicMock()
    mock.fs.ls.return_value = [
        MagicMock(name="testfile.zip", path="/mnt/tp-source-data/WORK/testfile.zip")
    ]
    return mock


@pytest.fixture
def mock_col_mappings_df(spark):
    return spark.createDataFrame([
        Row(cntrt_id="123", dmnsn_name="prod", file_col_name="column1", db_col_name="mapped_column1"),
        Row(cntrt_id="123", dmnsn_name="prod", file_col_name="column2", db_col_name="mapped_column2"),
        Row(cntrt_id="123", dmnsn_name="fact", file_col_name="measr#1", db_col_name="measr#1"),
    ])


@pytest.fixture
def mock_measr_df(spark):
    return spark.createDataFrame([
        Row(measr_phys_name="mapped_column1"),
        Row(measr_phys_name="mapped_column2"),
        Row(measr_phys_name="measr#1"),
    ])


@pytest.fixture
def mock_csv_df(spark):
    return spark.createDataFrame([
        Row(**{"column1": "val1", "column2": "val2", "measr#1": "5"})
    ])


@patch("common.read_query_from_postgres")
@patch("common.spark.read.parquet")
@patch("common.spark.read.format")
@pytest.mark.parametrize("file_type", ["prod", "mkt", "time", "fact", "other"])
def test_load_file_all_types(mock_read_format, mock_read_parquet, mock_read_query, file_type, spark, mock_dbutils, mock_col_mappings_df, mock_measr_df, mock_csv_df):
    mock_format = MagicMock()
    mock_format.option.return_value.load.return_value = mock_csv_df
    mock_read_format.return_value = mock_format

    if file_type == "fact":
        mock_read_parquet.side_effect = [mock_col_mappings_df, mock_col_mappings_df]
    else:
        mock_read_parquet.side_effect = [mock_col_mappings_df]

    mock_read_query.return_value = mock_measr_df

    result = load_file(
        file_type=file_type,
        RUN_ID="123",
        CNTRT_ID="123",
        STEP_FILE_PATTERN="testfile_%.csv",
        vendor_pattern="*",
        notebook_name="notebook",
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


def test_get_dbutils_with_spark(spark):
    dbutils = get_dbutils(spark)
    assert dbutils is not None


@patch("argparse.ArgumentParser.parse_args")
def test_read_run_params(mock_parse_args):
    mock_parse_args.return_value = argparse.Namespace(FILE_NAME="abc.csv", CNTRT_ID="999", RUN_ID="888")
    args = read_run_params()
    assert args.FILE_NAME == "abc.csv"
    assert args.CNTRT_ID == "999"
    assert args.RUN_ID == "888"
