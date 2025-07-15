import pytest
from unittest.mock import MagicMock
from pyspark.sql import Row

@pytest.mark.parametrize("file_type", ['prod', 'fact', 'mkt', 'time'])
def test_load_file_success(spark, mock_dbutils, sample_col_mapping_df, sample_measr_df, mock_read_query_from_postgres, file_type):
    # Mock file system listing
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name="sample_123.zip", path="/mnt/tp-source-data/WORK/sample_123.zip")
    ]

    # Mock parquet read
    spark.read.parquet = MagicMock(return_value=sample_col_mapping_df)

    # Mock query to Postgres
    mock_read_query_from_postgres.return_value = sample_measr_df

    # Mock CSV file reads
    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = sample_col_mapping_df
    spark.read.format.return_value = mock_reader

    # Assuming load_file is the function you're testing
    from load_file import load_file
    result_df = load_file(spark, mock_dbutils, file_type, mock_read_query_from_postgres)
    assert result_df is not None


def test_load_file_no_zip_found(spark, mock_dbutils, sample_col_mapping_df, sample_measr_df, mock_read_query_from_postgres):
    mock_dbutils.fs.ls.return_value = []

    spark.read.parquet = MagicMock(return_value=sample_col_mapping_df)
    mock_read_query_from_postgres.return_value = sample_measr_df

    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = sample_col_mapping_df
    spark.read.format.return_value = mock_reader

    from load_file import load_file
    with pytest.raises(Exception):  # Or your specific exception
        load_file(spark, mock_dbutils, 'prod', mock_read_query_from_postgres)


def test_load_file_invalid_file_type(spark, mock_dbutils, sample_col_mapping_df, sample_measr_df, mock_read_query_from_postgres):
    mock_dbutils.fs.ls.return_value = [
        MagicMock(name="sample_123.zip", path="/mnt/tp-source-data/WORK/sample_123.zip")
    ]

    spark.read.parquet = MagicMock(return_value=sample_col_mapping_df)
    mock_read_query_from_postgres.return_value = sample_measr_df

    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = sample_col_mapping_df
    spark.read.format.return_value = mock_reader

    from load_file import load_file
    with pytest.raises(ValueError):  # Adjust to your actual error type if different
        load_file(spark, mock_dbutils, 'invalid_type', mock_read_query_from_postgres)
