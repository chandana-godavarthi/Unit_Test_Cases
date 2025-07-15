import pytest
from unittest.mock import MagicMock, call
import common  # assuming your function is in common.py

def test_read_from_postgres_calls_spark_read_correctly():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_read = MagicMock()

    mock_spark.read = mock_read
    mock_format = mock_read.format.return_value
    mock_option = mock_format.option.return_value
    mock_option.option.return_value = mock_option  # for chained calls
    mock_option.load.return_value = mock_df

    # Inputs
    object_name = "test_table"
    refDBjdbcURL = "jdbc:postgresql://localhost:5432"
    refDBname = "testdb"
    refDBuser = "testuser"
    refDBpwd = "testpwd"

    result_df = common.read_from_postgres(
        object_name, mock_spark, refDBjdbcURL, refDBname, refDBuser, refDBpwd
    )

    # Check format called correctly
    mock_read.format.assert_called_once_with("jdbc")

    # Expected calls in order (we don't need to assert exact call order unless necessary, but we can verify key calls)
    expected_calls = [
        call.option("driver", "org.postgresql.Driver"),
        call.option("url", f"{refDBjdbcURL}/{refDBname}"),
        call.option("dbtable", f"{object_name}"),
        call.option("user", refDBuser),
        call.option("password", refDBpwd),
        call.option("ssl", True),
        call.option("sslmode", "require"),
        call.option("sslfactory", "org.postgresql.ssl.NonValidatingFactory")
    ]

    mock_format.option.assert_has_calls(expected_calls, any_order=False)
    mock_option.load.assert_called_once()

    assert result_df == mock_df


def test_read_from_postgres_load_raises_exception():
    mock_spark = MagicMock()
    mock_read = MagicMock()
    mock_spark.read = mock_read
    mock_format = mock_read.format.return_value
    mock_option = mock_format.option.return_value
    mock_option.option.return_value = mock_option

    mock_option.load.side_effect = Exception("Load failed")

    with pytest.raises(Exception, match="Load failed"):
        common.read_from_postgres(
            "test_table",
            mock_spark,
            "jdbc:postgresql://localhost:5432",
            "testdb",
            "user",
            "pwd"
        )
