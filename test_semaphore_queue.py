import pytest
from unittest.mock import MagicMock
from pyspark.sql import Row
import common


def test_semaphore_queue_success():
    mock_spark = MagicMock()

    # Mock DataFrame
    mock_df = MagicMock()

    # Mock createDataFrame to return our mock_df
    mock_spark.createDataFrame.return_value = mock_df

    # Mock withColumn to chain
    mock_df.withColumn.side_effect = lambda *args, **kwargs: mock_df

    # Mock createOrReplaceTempView
    mock_df.createOrReplaceTempView.return_value = None

    # Mock write chain
    mock_write = MagicMock()
    mock_df.write = mock_write
    mock_write_format = MagicMock()
    mock_write.format.return_value = mock_write_format
    mock_write_mode = MagicMock()
    mock_write_format.mode.return_value = mock_write_mode
    mock_write_mode.saveAsTable.return_value = None

    run_id = 1
    paths = ['path1', 'path2']
    catalog_name = 'catalog_name'

    result = common.semaphore_queue(run_id, paths, catalog_name, mock_spark)

    expected_rows = [Row(lock_path='path1'), Row(lock_path='path2')]
    mock_spark.createDataFrame.assert_called_once_with(expected_rows)
    assert mock_df.withColumn.call_count == 3
    mock_df.createOrReplaceTempView.assert_called_once_with("paths_df")
    mock_write.format.assert_called_once_with("delta")
    mock_write_format.mode.assert_called_once_with("append")
    mock_write_mode.saveAsTable.assert_called_once_with(f"{catalog_name}.internal_tp.tp_run_lock_plc")

    expected_check_path = "'path1', 'path2'"
    assert result == expected_check_path


def test_semaphore_queue_empty_paths():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_df.withColumn.side_effect = lambda *args, **kwargs: mock_df
    mock_df.createOrReplaceTempView.return_value = None
    mock_write = MagicMock()
    mock_df.write = mock_write
    mock_write_format = MagicMock()
    mock_write.format.return_value = mock_write_format
    mock_write_mode = MagicMock()
    mock_write_format.mode.return_value = mock_write_mode
    mock_write_mode.saveAsTable.return_value = None

    run_id = 1
    paths = []
    catalog_name = 'catalog_name'

    result = common.semaphore_queue(run_id, paths, catalog_name, mock_spark)

    mock_spark.createDataFrame.assert_called_once_with([])
    assert mock_df.withColumn.call_count == 3
    mock_df.createOrReplaceTempView.assert_called_once_with("paths_df")
    mock_write.format.assert_called_once_with("delta")
    mock_write_format.mode.assert_called_once_with("append")
    mock_write_mode.saveAsTable.assert_called_once_with(f"{catalog_name}.internal_tp.tp_run_lock_plc")

    assert result == ""


def test_semaphore_queue_write_failure():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_df.withColumn.side_effect = lambda *args, **kwargs: mock_df
    mock_df.createOrReplaceTempView.return_value = None
    mock_write = MagicMock()
    mock_df.write = mock_write
    mock_write_format = MagicMock()
    mock_write.format.return_value = mock_write_format
    mock_write_mode = MagicMock()
    mock_write_format.mode.return_value = mock_write_mode

    # Simulate write failure
    mock_write_mode.saveAsTable.side_effect = Exception("write fail")

    run_id = 1
    paths = ['path1']
    catalog_name = 'catalog_name'

    with pytest.raises(Exception, match="write fail"):
        common.semaphore_queue(run_id, paths, catalog_name, mock_spark)
