import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row
import common


def test_semaphore_queue_success():
    mock_spark = MagicMock()

    # Mock DataFrame
    mock_df = MagicMock()

    # Mock createDataFrame to return our mock_df
    mock_spark.createDataFrame.return_value = mock_df

    # Mock withColumn to chain
    mock_df.withColumn.return_value = mock_df

    # Mock createOrReplaceTempView
    mock_df.createOrReplaceTempView.return_value = None

    # Mock write.format().mode().saveAsTable() chain
    mock_write = MagicMock()
    mock_mode = MagicMock()
    mock_format = MagicMock()
    mock_df.write = mock_write
    mock_write.format.return_value = mock_format
    mock_format.mode.return_value = mock_mode
    mock_mode.saveAsTable.return_value = None

    run_id = 1
    paths = ['path1', 'path2']
    catalog_name = 'catalog_name'

    result = common.semaphore_queue(run_id, paths, catalog_name, mock_spark)

    # Verify createDataFrame called correctly
    expected_rows = [Row(lock_path='path1'), Row(lock_path='path2')]
    mock_spark.createDataFrame.assert_called_once_with(expected_rows)

    # Verify withColumn chaining called 3 times
    assert mock_df.withColumn.call_count == 3

    # Verify createOrReplaceTempView called once
    mock_df.createOrReplaceTempView.assert_called_once_with("paths_df")

    # Verify write-format-mode-saveAsTable chain
    mock_write.format.assert_called_once_with("delta")
    mock_format.mode.assert_called_once_with("append")
    mock_mode.saveAsTable.assert_called_once_with(f"{catalog_name}.internal_tp.tp_run_lock_plc")

    # Verify result check_path string
    expected_check_path = "'path1', 'path2'"
    assert result == expected_check_path


def test_semaphore_queue_empty_paths():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None
    mock_write = MagicMock()
    mock_mode = MagicMock()
    mock_format = MagicMock()
    mock_df.write = mock_write
    mock_write.format.return_value = mock_format
    mock_format.mode.return_value = mock_mode
    mock_mode.saveAsTable.return_value = None

    run_id = 1
    paths = []
    catalog_name = 'catalog_name'

    result = common.semaphore_queue(run_id, paths, catalog_name, mock_spark)

    # Ensure createDataFrame called with empty list
    mock_spark.createDataFrame.assert_called_once_with([])

    # Other checks
    assert mock_df.withColumn.call_count == 3
    mock_df.createOrReplaceTempView.assert_called_once_with("paths_df")
    mock_write.format.assert_called_once_with("delta")
    mock_format.mode.assert_called_once_with("append")
    mock_mode.saveAsTable.assert_called_once_with(f"{catalog_name}.internal_tp.tp_run_lock_plc")

    # Expected check_path string for empty list
    assert result == ""


def test_semaphore_queue_write_failure():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.createOrReplaceTempView.return_value = None
    mock_write = MagicMock()
    mock_mode = MagicMock()
    mock_format = MagicMock()
    mock_df.write = mock_write
    mock_write.format.return_value = mock_format
    mock_format.mode.return_value = mock_mode

    # Simulate write failure
    mock_mode.saveAsTable.side_effect = Exception("write fail")

    run_id = 1
    paths = ['path1']
    catalog_name = 'catalog_name'

    with pytest.raises(Exception, match="write fail"):
        common.semaphore_queue(run_id, paths, catalog_name, mock_spark)
