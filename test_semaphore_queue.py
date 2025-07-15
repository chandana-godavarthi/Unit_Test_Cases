import pytest
from pyspark.sql import Row
from unittest.mock import patch, MagicMock

def test_semaphore_queue_with_valid_paths(spark):
    paths = ["/mnt/file1", "/mnt/file2"]
    run_id = 123
    catalog_name = "test_catalog"

    mock_df = spark.createDataFrame([Row(lock_path=path) for path in paths])

    mock_writer = MagicMock()
    with patch.object(spark, 'createDataFrame', return_value=mock_df), \
         patch.object(mock_df, 'withColumn', side_effect=lambda col, val: mock_df), \
         patch.object(mock_df, 'createOrReplaceTempView'), \
         patch('pyspark.sql.DataFrame.write', new_callable=MagicMock, return_value=mock_writer):

        from semaphore_queue import semaphore_queue
        semaphore_queue(spark, paths, run_id, catalog_name)

    mock_writer.save.assert_called()


def test_semaphore_queue_with_empty_paths(spark):
    paths = []
    run_id = 456
    catalog_name = "test_catalog"

    from semaphore_queue import semaphore_queue
    result = semaphore_queue(spark, paths, run_id, catalog_name)
    assert result is None  # Assuming your implementation returns None on empty list
