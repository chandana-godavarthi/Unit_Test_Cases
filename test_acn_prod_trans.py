import pytest
from unittest.mock import MagicMock
from common import acn_prod_trans
import pyspark.sql.functions as F


# Fixture to mock spark session
@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    spark.read.parquet.return_value = MagicMock()
    return spark


# Fixture to mock a DataFrame and chainable methods
@pytest.fixture
def mock_dataframe():
    df = MagicMock()
    df.createOrReplaceTempView.return_value = None
    df.count.return_value = 5
    df.withColumn.return_value = df
    df.unionByName.return_value = df
    df.groupBy.return_value.agg.return_value = df
    df.drop.return_value = df
    return df


# Fixture to patch pyspark.sql.functions functions
@pytest.fixture(autouse=True)
def mock_pyspark_functions(monkeypatch):
    monkeypatch.setattr(F, 'col', MagicMock(return_value=MagicMock(name="col")))
    monkeypatch.setattr(F, 'trim', MagicMock(return_value=MagicMock(name="trim")))
    monkeypatch.setattr(F, 'when', MagicMock(return_value=MagicMock(name="when")))
    monkeypatch.setattr(F, 'date_format', MagicMock(return_value=MagicMock(name="date_format")))


# Test: SQL and Parquet reads happen, methods called
def test_acn_prod_trans_calls_sql_and_parquet_correctly(mock_spark, mock_dataframe):
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark.sql.return_value = mock_dataframe
    mock_spark.read.parquet.return_value = mock_dataframe

    result_df = acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.sql.assert_called()
    mock_spark.read.parquet.assert_called()
    assert result_df is not None


# Test: Parquet input empty (count=0) case
def test_acn_prod_trans_parquet_empty(mock_spark, monkeypatch):
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_empty_df = MagicMock()
    mock_empty_df.count.return_value = 0
    mock_empty_df.createOrReplaceTempView.return_value = None
    mock_empty_df.withColumn.return_value = mock_empty_df
    mock_empty_df.unionByName.return_value = mock_empty_df
    mock_empty_df.groupBy.return_value.agg.return_value = mock_empty_df
    mock_empty_df.drop.return_value = mock_empty_df

    mock_spark.sql.return_value = mock_empty_df
    mock_spark.read.parquet.return_value = mock_empty_df

    result_df = acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    mock_spark.sql.assert_called()
    mock_spark.read.parquet.assert_called()
    assert result_df is not None


# Test: DataFrame methods are chained and called
def test_acn_prod_trans_dataframe_methods_called(mock_spark, mock_dataframe):
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark.sql.return_value = mock_dataframe
    mock_spark.read.parquet.return_value = mock_dataframe

    result_df = acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    assert mock_dataframe.withColumn.called
    assert mock_dataframe.createOrReplaceTempView.called
    assert result_df is not None


# Test: final join query execution (basic assertion)
def test_acn_prod_trans_final_join_query(mock_spark, mock_dataframe):
    run_id = "123"
    srce_sys_id = 456
    catalog_name = "test_catalog"

    mock_spark.sql.return_value = mock_dataframe
    mock_spark.read.parquet.return_value = mock_dataframe

    result_df = acn_prod_trans(srce_sys_id, run_id, catalog_name, mock_spark)

    assert result_df is not None
