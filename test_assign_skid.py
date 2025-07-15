import pytest
from unittest.mock import MagicMock, patch
import common


def test_assign_skid_type_not_in_list():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    result = common.assign_skid(mock_df, 1, 'invalid_type', 'catalog_name', mock_spark)
    # Should do nothing — return None
    assert result is None


def test_assign_skid_runid_exists():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.columns = ['run_id', 'extrn_prod_id', 'prod_skid']

    # Mock the spark.sql().count() to return >0 (existing run_id)
    mock_spark.sql.return_value.count.return_value = 1

    result_df = MagicMock()
    mock_spark.sql.return_value = result_df

    with patch.object(result_df, 'createOrReplaceTempView') as mock_tempview, \
         patch.object(result_df, 'select', return_value=result_df):

        result = common.assign_skid(mock_df, 1, 'prod', 'catalog_name', mock_spark)

        assert result == result_df
        mock_df.createOrReplaceTempView.assert_called()
        mock_spark.sql.assert_called()
        result_df.createOrReplaceTempView.assert_called()
        result_df.select.assert_called()


def test_assign_skid_runid_not_exists():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.columns = ['run_id', 'extrn_prod_id', 'prod_skid']

    # Simulate spark.sql().count() = 0 (run_id not exists)
    count_mock = MagicMock(return_value=0)
    mock_spark.sql.return_value.count = count_mock

    # Mock returned DataFrame
    result_df = MagicMock()
    mock_spark.sql.side_effect = [MagicMock(),  # for run_id check query
                                  result_df,    # for skid mapping select query
                                  result_df]    # for final join query

    with patch.object(result_df, 'write') as mock_write, \
         patch.object(result_df, 'createOrReplaceTempView') as mock_tempview, \
         patch.object(result_df, 'select', return_value=result_df):

        result = common.assign_skid(mock_df, 1, 'prod', 'catalog_name', mock_spark)

        # Check that write.mode().saveAsTable() was called
        mock_write.mode.return_value.saveAsTable.assert_called_once()

        assert result == result_df
        mock_df.createOrReplaceTempView.assert_called()
        result_df.createOrReplaceTempView.assert_called()
        result_df.select.assert_called()


def test_assign_skid_exception():
    mock_spark = MagicMock()
    mock_df = MagicMock()

    # Force spark.sql to raise an exception
    mock_spark.sql.side_effect = Exception("mock error")

    with pytest.raises(Exception) as exc_info:
        common.assign_skid(mock_df, 1, 'prod', 'catalog_name', mock_spark)

    assert "mock error" in str(exc_info.value)
