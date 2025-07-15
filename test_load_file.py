import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql import functions as F
from unittest.mock import MagicMock, patch
import os
import zipfile
import re
from datetime import time
from common import load_file

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("pytest") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_dbutils():
    return MagicMock()

@pytest.fixture
def setup_test_environment(spark, mock_dbutils):
    # Setup test data
    test_run_id = "12345"
    test_cntrt_id = "67890"
    test_file_pattern = "test_file_%.csv"
    test_vendor_pattern = "test_vendor"
    test_notebook_name = "test_notebook"
    test_delimiter = ","
    test_postgres_schema = "test_schema"
    
    # Mock database credentials
    test_db_creds = {
        "refDBjdbcURL": "jdbc:postgresql://test:5432",
        "refDBname": "test_db",
        "refDBuser": "test_user",
        "refDBpwd": "test_pwd"
    }
    
    return {
        "spark": spark,
        "dbutils": mock_dbutils,
        "run_id": test_run_id,
        "cntrt_id": test_cntrt_id,
        "file_pattern": test_file_pattern,
        "vendor_pattern": test_vendor_pattern,
        "notebook_name": test_notebook_name,
        "delimiter": test_delimiter,
        "postgres_schema": test_postgres_schema,
        "db_creds": test_db_creds
    }

def test_load_file_prod_type(setup_test_environment):
    # Arrange
    context = setup_test_environment
    file_type = "prod"
    
    # Create test data
    test_data = [("prod1", "category1", "super1"), ("prod2", "category2", "super2")]
    test_df = context["spark"].createDataFrame(test_data, ["PROD", "CATEGORY", "SUPER_CATEGORY"])
    
    # Mock filesystem operations
    context["dbutils"].fs.ls.return_value = [MagicMock(name="test_file_12345.csv", path="dbfs:/mnt/tp-source-data/WORK/test_file_12345.csv")]
    
    # Mock column mappings
    col_mapping_data = [
        ("PROD", "prod_id"),
        ("CATEGORY", "category"),
        ("SUPER_CATEGORY", "super_category")
    ]
    col_mapping_df = context["spark"].createDataFrame(col_mapping_data, ["file_col_name", "db_col_name"])
    
    # Mock reading from parquet
    with patch("pyspark.sql.DataFrameReader.parquet", return_value=col_mapping_df):
        # Mock reading CSV
        with patch("pyspark.sql.DataFrameReader.csv", return_value=test_df):
            # Act
            result = load_file(
                file_type=file_type,
                RUN_ID=context["run_id"],
                CNTRT_ID=context["cntrt_id"],
                STEP_FILE_PATTERN=context["file_pattern"],
                vendor_pattern=context["vendor_pattern"],
                notebook_name=context["notebook_name"],
                delimiter=context["delimiter"],
                dbutils=context["dbutils"],
                postgres_schema=context["postgres_schema"],
                spark=context["spark"],
                refDBjdbcURL=context["db_creds"]["refDBjdbcURL"],
                refDBname=context["db_creds"]["refDBname"],
                refDBuser=context["db_creds"]["refDBuser"],
                refDBpwd=context["db_creds"]["refDBpwd"]
            )
    
    # Assert
    assert result == "Success"
    context["dbutils"].fs.ls.assert_called_once_with("/mnt/tp-source-data/WORK/")

def test_load_file_mkt_type(setup_test_environment):
    # Arrange
    context = setup_test_environment
    file_type = "mkt"
    
    # Create test data
    test_data = [("mkt1", "region1", "country1"), ("mkt2", "region2", "country2")]
    test_df = context["spark"].createDataFrame(test_data, ["MKT", "REGION", "COUNTRY"])
    
    # Mock filesystem operations
    context["dbutils"].fs.ls.return_value = [MagicMock(name="test_file_12345.csv", path="dbfs:/mnt/tp-source-data/WORK/test_file_12345.csv")]
    
    # Mock column mappings
    col_mapping_data = [
        ("MKT", "market_id"),
        ("REGION", "region"),
        ("COUNTRY", "country")
    ]
    col_mapping_df = context["spark"].createDataFrame(col_mapping_data, ["file_col_name", "db_col_name"])
    
    # Mock reading from parquet
    with patch("pyspark.sql.DataFrameReader.parquet", return_value=col_mapping_df):
        # Mock reading CSV
        with patch("pyspark.sql.DataFrameReader.csv", return_value=test_df):
            # Act
            result = load_file(
                file_type=file_type,
                RUN_ID=context["run_id"],
                CNTRT_ID=context["cntrt_id"],
                STEP_FILE_PATTERN=context["file_pattern"],
                vendor_pattern=context["vendor_pattern"],
                notebook_name=context["notebook_name"],
                delimiter=context["delimiter"],
                dbutils=context["dbutils"],
                postgres_schema=context["postgres_schema"],
                spark=context["spark"],
                refDBjdbcURL=context["db_creds"]["refDBjdbcURL"],
                refDBname=context["db_creds"]["refDBname"],
                refDBuser=context["db_creds"]["refDBuser"],
                refDBpwd=context["db_creds"]["refDBpwd"]
            )
    
    # Assert
    assert result == "Success"

def test_load_file_fact_type(setup_test_environment):
    # Arrange
    context = setup_test_environment
    file_type = "fact"
    
    # Create test data
    test_data = [("prod1", "mkt1", 100), ("prod2", "mkt2", 200)]
    test_df = context["spark"].createDataFrame(test_data, ["PROD", "MKT", "VALUE"])
    
    # Mock filesystem operations
    context["dbutils"].fs.ls.return_value = [MagicMock(name="test_file_12345.csv", path="dbfs:/mnt/tp-source-data/WORK/test_file_12345.csv")]
    
    # Mock column mappings
    col_mapping_data = [
        ("PROD", "product_id"),
        ("MKT", "market_id"),
        ("VALUE", "value")
    ]
    col_mapping_df = context["spark"].createDataFrame(col_mapping_data, ["file_col_name", "db_col_name"])
    
    # Mock reading from parquet
    with patch("pyspark.sql.DataFrameReader.parquet", return_value=col_mapping_df):
        # Mock reading CSV
        with patch("pyspark.sql.DataFrameReader.csv", return_value=test_df):
            # Act
            result = load_file(
                file_type=file_type,
                RUN_ID=context["run_id"],
                CNTRT_ID=context["cntrt_id"],
                STEP_FILE_PATTERN=context["file_pattern"],
                vendor_pattern=context["vendor_pattern"],
                notebook_name=context["notebook_name"],
                delimiter=context["delimiter"],
                dbutils=context["dbutils"],
                postgres_schema=context["postgres_schema"],
                spark=context["spark"],
                refDBjdbcURL=context["db_creds"]["refDBjdbcURL"],
                refDBname=context["db_creds"]["refDBname"],
                refDBuser=context["db_creds"]["refDBuser"],
                refDBpwd=context["db_creds"]["refDBpwd"]
            )
    
    # Assert
    assert result == "Success"

def test_load_file_time_type(setup_test_environment):
    # Arrange
    context = setup_test_environment
    file_type = "time"
    
    # Create test data
    test_data = [("2023-01-01", "Q1", "Jan"), ("2023-02-01", "Q1", "Feb")]
    test_df = context["spark"].createDataFrame(test_data, ["DATE", "QUARTER", "MONTH"])
    
    # Mock filesystem operations
    context["dbutils"].fs.ls.return_value = [MagicMock(name="test_file_12345.csv", path="dbfs:/mnt/tp-source-data/WORK/test_file_12345.csv")]
    
    # Mock column mappings
    col_mapping_data = [
        ("DATE", "date"),
        ("QUARTER", "quarter"),
        ("MONTH", "month")
    ]
    col_mapping_df = context["spark"].createDataFrame(col_mapping_data, ["file_col_name", "db_col_name"])
    
    # Mock reading from parquet
    with patch("pyspark.sql.DataFrameReader.parquet", return_value=col_mapping_df):
        # Mock reading CSV
        with patch("pyspark.sql.DataFrameReader.csv", return_value=test_df):
            # Act
            result = load_file(
                file_type=file_type,
                RUN_ID=context["run_id"],
                CNTRT_ID=context["cntrt_id"],
                STEP_FILE_PATTERN=context["file_pattern"],
                vendor_pattern=context["vendor_pattern"],
                notebook_name=context["notebook_name"],
                delimiter=context["delimiter"],
                dbutils=context["dbutils"],
                postgres_schema=context["postgres_schema"],
                spark=context["spark"],
                refDBjdbcURL=context["db_creds"]["refDBjdbcURL"],
                refDBname=context["db_creds"]["refDBname"],
                refDBuser=context["db_creds"]["refDBuser"],
                refDBpwd=context["db_creds"]["refDBpwd"]
            )
    
    # Assert
    assert result == "Success"

def test_load_file_with_zip_file(setup_test_environment):
    # Arrange
    context = setup_test_environment
    file_type = "prod"
    
    # Create test data
    test_data = [("prod1", "category1"), ("prod2", "category2")]
    test_df = context["spark"].createDataFrame(test_data, ["PROD", "CATEGORY"])
    
    # Mock filesystem operations to return a zip file
    context["dbutils"].fs.ls.return_value = [MagicMock(name="test_file_12345.zip", path="dbfs:/mnt/tp-source-data/WORK/test_file_12345.zip")]
    
    # Mock column mappings
    col_mapping_data = [("PROD", "prod_id"), ("CATEGORY", "category")]
    col_mapping_df = context["spark"].createDataFrame(col_mapping_data, ["file_col_name", "db_col_name"])
    
    # Mock reading from parquet
    with patch("pyspark.sql.DataFrameReader.parquet", return_value=col_mapping_df):
        # Mock reading CSV
        with patch("pyspark.sql.DataFrameReader.csv", return_value=test_df):
            # Act
            result = load_file(
                file_type=file_type,
                RUN_ID=context["run_id"],
                CNTRT_ID=context["cntrt_id"],
                STEP_FILE_PATTERN=context["file_pattern"],
                vendor_pattern=context["vendor_pattern"],
                notebook_name=context["notebook_name"],
                delimiter=context["delimiter"],
                dbutils=context["dbutils"],
                postgres_schema=context["postgres_schema"],
                spark=context["spark"],
                refDBjdbcURL=context["db_creds"]["refDBjdbcURL"],
                refDBname=context["db_creds"]["refDBname"],
                refDBuser=context["db_creds"]["refDBuser"],
                refDBpwd=context["db_creds"]["refDBpwd"]
            )
    
    # Assert
    assert result == "Success"

def test_load_file_with_special_char_columns(setup_test_environment):
    # Arrange
    context = setup_test_environment
    file_type = "prod"
    
    # Create test data with special char columns
    test_data = [("prod1", "val1", "val2"), ("prod2", "val3", "val4")]
    test_df = context["spark"].createDataFrame(test_data, ["PROD", "COL#1", "COL#2"])
    
    # Mock filesystem operations
    context["dbutils"].fs.ls.return_value = [MagicMock(name="test_file_12345.csv", path="dbfs:/mnt/tp-source-data/WORK/test_file_12345.csv")]
    
    # Mock column mappings with special char handling
    col_mapping_data = [
        ("PROD", "prod_id"),
        ("COL#1", "col"),
        ("COL#2", "col")
    ]
    col_mapping_df = context["spark"].createDataFrame(col_mapping_data, ["file_col_name", "db_col_name"])
    
    # Mock reading from parquet
    with patch("pyspark.sql.DataFrameReader.parquet", return_value=col_mapping_df):
        # Mock reading CSV
        with patch("pyspark.sql.DataFrameReader.csv", return_value=test_df):
            # Act
            result = load_file(
                file_type=file_type,
                RUN_ID=context["run_id"],
                CNTRT_ID=context["cntrt_id"],
                STEP_FILE_PATTERN=context["file_pattern"],
                vendor_pattern=context["vendor_pattern"],
                notebook_name=context["notebook_name"],
                delimiter=context["delimiter"],
                dbutils=context["dbutils"],
                postgres_schema=context["postgres_schema"],
                spark=context["spark"],
                refDBjdbcURL=context["db_creds"]["refDBjdbcURL"],
                refDBname=context["db_creds"]["refDBname"],
                refDBuser=context["db_creds"]["refDBuser"],
                refDBpwd=context["db_creds"]["refDBpwd"]
            )
    
    # Assert
    assert result == "Success"

def test_load_file_with_multiple_mappings(setup_test_environment):
    # Arrange
    context = setup_test_environment
    file_type = "prod"
    
    # Create test data
    test_data = [("prod1", "desc1"), ("prod2", "desc2")]
    test_df = context["spark"].createDataFrame(test_data, ["PROD", "DESC"])
    
    # Mock filesystem operations
    context["dbutils"].fs.ls.return_value = [MagicMock(name="test_file_12345.csv", path="dbfs:/mnt/tp-source-data/WORK/test_file_12345.csv")]
    
    # Mock column mappings with multiple mappings
    col_mapping_data = [
        ("PROD", "prod_id,product_id"),  # Multiple mappings
        ("DESC", "description")
    ]
    col_mapping_df = context["spark"].createDataFrame(col_mapping_data, ["file_col_name", "db_col_name"])
    
    # Mock reading from parquet
    with patch("pyspark.sql.DataFrameReader.parquet", return_value=col_mapping_df):
        # Mock reading CSV
        with patch("pyspark.sql.DataFrameReader.csv", return_value=test_df):
            # Act
            result = load_file(
                file_type=file_type,
                RUN_ID=context["run_id"],
                CNTRT_ID=context["cntrt_id"],
                STEP_FILE_PATTERN=context["file_pattern"],
                vendor_pattern=context["vendor_pattern"],
                notebook_name=context["notebook_name"],
                delimiter=context["delimiter"],
                dbutils=context["dbutils"],
                postgres_schema=context["postgres_schema"],
                spark=context["spark"],
                refDBjdbcURL=context["db_creds"]["refDBjdbcURL"],
                refDBname=context["db_creds"]["refDBname"],
                refDBuser=context["db_creds"]["refDBuser"],
                refDBpwd=context["db_creds"]["refDBpwd"]
            )
    
    # Assert
    assert result == "Success"

def test_load_file_with_null_measure_values(setup_test_environment):
    # Arrange
    context = setup_test_environment
    file_type = "prod"
    
    # Create test data with null measure values
    test_data = [
        ("prod1", "category1", None, None),
        ("prod2", "category2", 100, 200)
    ]
    test_df = context["spark"].createDataFrame(test_data, ["PROD", "CATEGORY", "SALES", "UNITS"])
    
    # Mock filesystem operations
    context["dbutils"].fs.ls.return_value = [MagicMock(name="test_file_12345.csv", path="dbfs:/mnt/tp-source-data/WORK/test_file_12345.csv")]
    
    # Mock column mappings
    col_mapping_data = [
        ("PROD", "prod_id"),
        ("CATEGORY", "category"),
        ("SALES", "sales"),
        ("UNITS", "units")
    ]
    col_mapping_df = context["spark"].createDataFrame(col_mapping_data, ["file_col_name", "db_col_name"])
    
    # Mock measure lookup
    measure_data = [("sales",), ("units",)]
    measure_df = context["spark"].createDataFrame(measure_data, ["measr_phys_name"])
    
    # Mock reading from parquet and postgres
    with patch("pyspark.sql.DataFrameReader.parquet", return_value=col_mapping_df):
        with patch("common.read_query_from_postgres", return_value=measure_df):
            # Mock reading CSV
            with patch("pyspark.sql.DataFrameReader.csv", return_value=test_df):
                # Act
                result = load_file(
                    file_type=file_type,
                    RUN_ID=context["run_id"],
                    CNTRT_ID=context["cntrt_id"],
                    STEP_FILE_PATTERN=context["file_pattern"],
                    vendor_pattern=context["vendor_pattern"],
                    notebook_name=context["notebook_name"],
                    delimiter=context["delimiter"],
                    dbutils=context["dbutils"],
                    postgres_schema=context["postgres_schema"],
                    spark=context["spark"],
                    refDBjdbcURL=context["db_creds"]["refDBjdbcURL"],
                    refDBname=context["db_creds"]["refDBname"],
                    refDBuser=context["db_creds"]["refDBuser"],
                    refDBpwd=context["db_creds"]["refDBpwd"]
                )
    
    # Assert
    assert result == "Success"
