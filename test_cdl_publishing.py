import pytest
from unittest.mock import patch, MagicMock
import sys
import types
import common

# Mock Configuration and MetaPSClient globally for common module
mock_config_class = MagicMock()
mock_meta_class = MagicMock()

mock_common_modules = types.SimpleNamespace(
    Configuration=mock_config_class,
    MetaPSClient=mock_meta_class
)

# Inject into sys.modules so when common uses it, it's already there
sys.modules['common.Configuration'] = mock_config_class
sys.modules['common.MetaPSClient'] = mock_meta_class


def test_cdl_publishing_success():
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["table1", "table2"]}
    mock_meta_client = MagicMock()

    mock_config_class.load_for_default_environment_notebook.return_value = mock_config
    mock_meta_class.configure.return_value.get_client.return_value = mock_meta_client

    common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)

    assert mock_meta_client.publish_table.call_count == len(mock_config["tables"])
    mock_meta_client.start_publishing.assert_called_once()


def test_cdl_publishing_empty_tables():
    mock_dbutils = MagicMock()
    mock_config = {"tables": []}
    mock_meta_client = MagicMock()

    mock_config_class.load_for_default_environment_notebook.return_value = mock_config
    mock_meta_class.configure.return_value.get_client.return_value = mock_meta_client

    common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)

    mock_meta_client.publish_table.assert_not_called()
    mock_meta_client.start_publishing.assert_called_once()


def test_cdl_publishing_config_load_failure():
    mock_dbutils = MagicMock()
    mock_config_class.load_for_default_environment_notebook.side_effect = Exception("load fail")

    with pytest.raises(Exception, match="load fail"):
        common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)


def test_cdl_publishing_publish_table_failure():
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["table1"]}
    mock_meta_client = MagicMock()

    mock_config_class.load_for_default_environment_notebook.return_value = mock_config
    mock_meta_class.configure.return_value.get_client.return_value = mock_meta_client
    mock_meta_client.publish_table.side_effect = Exception("publish fail")

    with pytest.raises(Exception, match="publish fail"):
        common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)


def test_cdl_publishing_start_publishing_failure():
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["table1"]}
    mock_meta_client = MagicMock()

    mock_config_class.load_for_default_environment_notebook.return_value = mock_config
    mock_meta_class.configure.return_value.get_client.return_value = mock_meta_client
    mock_meta_client.start_publishing.side_effect = Exception("start fail")

    with pytest.raises(Exception, match="start fail"):
        common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)
