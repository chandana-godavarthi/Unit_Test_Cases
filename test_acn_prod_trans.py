import pytest
from unittest.mock import MagicMock
from common import acn_prod_trans

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    spark.read.parquet.return_value = MagicMock()
    return spark

@pytest.fixture
def mock_dataframe():
    df = MagicMock()
    df.createOrReplaceTempView.return_value = None
    df.count.return_value = 5
    df.withColumn.return_value = df
    df.unionByName.return_value = df
    df.groupBy.return_value.agg.return_value = df
    df.drop.return_value = df
    df.createOrReplaceTempView.return_value = None
    return df

def test_acn_prod_trans_calls_sql_and_parquet_correctly(mock_spark, mock_dataframe, monkeypatch):
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    # Mock spark.sql to return our mock dataframe
    mock_spark.sql.return_value = mock_dataframe
    mock_spark.read.parquet.return_value = mock_dataframe

    # Run the function
    result_df = acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    # Check parquet path constructed properly
    expected_parquet_path = f"/mnt/tp-source-data/temp/materialised/{run_id}/load_product_df_prod_extrn"
    mock_spark.read.parquet.assert_called_once_with(expected_parquet_path)

    # Check it called spark.sql at least once for prod_dim schema
    assert mock_spark.sql.call_count >= 3  # prod_dim, dedup, final join etc.

    # Check DataFrame transformations
    assert mock_dataframe.withColumn.called
    assert mock_dataframe.unionByName.called
    assert mock_dataframe.groupBy.called
    assert mock_dataframe.drop.called

    # Verify final result is returned
    assert result_df == mock_spark.sql.return_value

def test_acn_prod_trans_parquet_empty(mock_spark, monkeypatch):
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    # Mock empty DataFrame
    mock_empty_df = MagicMock()
    mock_empty_df.count.return_value = 0
    mock_empty_df.createOrReplaceTempView.return_value = None
    mock_empty_df.withColumn.return_value = mock_empty_df
    mock_empty_df.unionByName.return_value = mock_empty_df
    mock_empty_df.groupBy.return_value.agg.return_value = mock_empty_df
    mock_empty_df.drop.return_value = mock_empty_df

    mock_spark.sql.return_value = mock_empty_df
    mock_spark.read.parquet.return_value = mock_empty_df

    # Run the function
    result_df = acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    # Should still return a DataFrame even if empty
    assert result_df is not None
    assert result_df == mock_spark.sql.return_value

def test_acn_prod_trans_dataframe_methods_called(mock_spark, mock_dataframe):
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark.sql.return_value = mock_dataframe
    mock_spark.read.parquet.return_value = mock_dataframe

    result_df = acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    # Verify key DataFrame methods are called
    mock_dataframe.createOrReplaceTempView.assert_called()
    mock_dataframe.withColumn.assert_called()
    mock_dataframe.unionByName.assert_called()
    mock_dataframe.groupBy.assert_called()
    mock_dataframe.drop.assert_called()
    assert result_df == mock_spark.sql.return_value

def test_acn_prod_trans_final_join_query(mock_spark, mock_dataframe):
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark.sql.return_value = mock_dataframe
    mock_spark.read.parquet.return_value = mock_dataframe

    acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    # Extract final join query
    queries = [call.args[0] for call in mock_spark.sql.call_args_list]
    final_join_query = [q for q in queries if "LEFT OUTER JOIN" in q]
    assert len(final_join_query) == 1

def test_acn_prod_trans_with_invalid_spark(monkeypatch):
    # Simulate invalid Spark session
    with pytest.raises(AttributeError):
        acn_prod_trans(1, "runid", "catalog", None)
