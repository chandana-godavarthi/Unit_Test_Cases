import pytest
from unittest.mock import patch, MagicMock
from common import get_dbutils

def test_get_dbutils_with_pyspark_dbutils():
    mock_spark = MagicMock()
    mock_dbutils = MagicMock()

    with patch("pyspark.dbutils.DBUtils", return_value=mock_dbutils):
        result = get_dbutils(mock_spark)
        assert result == mock_dbutils

def test_get_dbutils_with_ipython_session_and_dbutils():
    mock_spark = MagicMock()
    mock_ipython = MagicMock()
    mock_dbutils = MagicMock()

    # Simulate ImportError for pyspark.dbutils.DBUtils
    with patch("pyspark.dbutils.DBUtils", side_effect=ImportError):
        with patch("IPython.get_ipython", return_value=mock_ipython):
            mock_ipython.user_ns = {"dbutils": mock_dbutils}
            result = get_dbutils(mock_spark)
            assert result == mock_dbutils

def test_get_dbutils_with_no_pyspark_dbutils_and_no_ipython():
    mock_spark = MagicMock()

    with patch("pyspark.dbutils.DBUtils", side_effect=ImportError):
        with patch("IPython.get_ipython", return_value=None):
            result = get_dbutils(mock_spark)
            assert result is None
