import pytest
from unittest.mock import patch, MagicMock, Mock
from common import get_dbutils

def test_get_dbutils_with_pyspark_dbutils():
    mock_spark = MagicMock()
    mock_dbutils_class = MagicMock()
    mock_dbutils_instance = MagicMock()

    mock_dbutils_class.return_value = mock_dbutils_instance

    # Mock the pyspark.dbutils module and DBUtils class inside it
    with patch.dict('sys.modules', {
        'pyspark.dbutils': Mock(DBUtils=mock_dbutils_class)
    }):
        result = get_dbutils(mock_spark)

        mock_dbutils_class.assert_called_once_with(mock_spark)
        assert result == mock_dbutils_instance

def test_get_dbutils_with_ipython_session_and_dbutils():
    mock_spark = MagicMock()
    mock_ipython = MagicMock()
    mock_dbutils = MagicMock()

    # Simulate pyspark.dbutils ImportError by removing from sys.modules
    with patch.dict('sys.modules', {}):
        with patch("IPython.get_ipython", return_value=mock_ipython):
            mock_ipython.user_ns = {"dbutils": mock_dbutils}
            result = get_dbutils(mock_spark)

            assert result == mock_dbutils

def test_get_dbutils_with_no_pyspark_dbutils_and_no_ipython():
    mock_spark = MagicMock()

    # Simulate pyspark.dbutils ImportError and no IPython session
    with patch.dict('sys.modules', {}):
        with patch("IPython.get_ipython", return_value=None):
            result = get_dbutils(mock_spark)

            assert result is None
