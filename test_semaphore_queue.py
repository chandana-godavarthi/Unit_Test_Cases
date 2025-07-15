import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType, TimestampType
from pyspark.sql.functions import lit, current_timestamp
import common


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestSession").getOrCreate()


def test_semaphore_queue_with_valid_paths(spark):
    paths = ["/mnt/file1", "/mnt/file2"]
    run_id = 123
    catalog_name = "test_catalog"

    mock_df = spark.createDataFrame([Row(lock_path=path) for path in paths])

    # Mock DataFrame transformations and write operations
    with patch.object(spark, 'createDataFrame', return_value=mock_df) as mock_create_df, \
         patch.object(mock_df, 'withColumn', side_effect=lambda col, val: mock_df) as mock_with_column, \
         patch.object(mock_df, 'createOrReplaceTempView') as mock_create_view, \
         patch.object(mock_df, 'write') as mock_write:

        mock_format = MagicMock()
        mock_write.format.return_value = mock_format
        mock_mode = MagicMock()
        mock_format.mode.return_value = mock_mode

        result = common.semaphore_queue(run_id, paths, catalog_name, spark)

        # Validate returned string
        assert result == "'/mnt/file1', '/mnt/file2'"

        # Validate DataFrame creation call
        mock_create_df.assert_called_once()

        # Validate withColumn calls
        assert mock_with_column.call_count == 3

        # Validate temp view creation
        mock_create_view.assert_called_once_with("paths_df")

        # Validate write operation
        mock_write.format.assert_called_once_with("delta")
        mock_format.mode.assert_called_once_with("append")
        mock_mode.saveAsTable.assert_called_once_with("test_catalog.internal_tp.tp_run_lock_plc")


def test_semaphore_queue_with_empty_paths(spark):
    paths = []
    run_id = 456
    catalog_name = "test_catalog"

    mock_df = spark.createDataFrame([Row(lock_path=path) for path in paths])

    with patch.object(spark, 'createDataFrame', return_value=mock_df) as mock_create_df, \
         patch.object(mock_df, 'withColumn', side_effect=lambda col, val: mock_df) as mock_with_column, \
         patch.object(mock_df, 'createOrReplaceTempView') as mock_create_view, \
         patch.object(mock_df, 'write') as mock_write:

        mock_format = MagicMock()
        mock_write.format.return_value = mock_format
        mock_mode = MagicMock()
        mock_format.mode.return_value = mock_mode

        result = common.semaphore_queue(run_id, paths, catalog_name, spark)

        # Validate returned string for empty list
        assert result == ""

        mock_create_df.assert_called_once()
        assert mock_with_column.call_count == 3
        mock_create_view.assert_called_once_with("paths_df")
        mock_write.format.assert_called_once_with("delta")
        mock_format.mode.assert_called_once_with("append")
        mock_mode.saveAsTable.assert_called_once_with("test_catalog.internal_tp.tp_run_lock_plc")

