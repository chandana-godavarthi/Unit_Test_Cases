import pytest
from unittest.mock import MagicMock, patch
import common

def test_cdl_publishing_success():
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["table1", "table2"]}

    mock_meta_client = MagicMock()

    with patch("common.Configuration.load_for_default_environment_notebook", return_value=mock_config), \
         patch("common.MetaPSClient.configure") as mock_configure:

        mock_configure.return_value.get_client.return_value = mock_meta_client

        common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)

        # assert publish_table called for each table in config
        assert mock_meta_client.publish_table.call_count == len(mock_config["tables"])

        # assert start_publishing called once
        mock_meta_client.start_publishing.assert_called_once()


def test_cdl_publishing_empty_tables():
    mock_dbutils = MagicMock()
    mock_config = {"tables": []}

    mock_meta_client = MagicMock()

    with patch("common.Configuration.load_for_default_environment_notebook", return_value=mock_config), \
         patch("common.MetaPSClient.configure") as mock_configure:

        mock_configure.return_value.get_client.return_value = mock_meta_client

        common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)

        # assert publish_table never called
        mock_meta_client.publish_table.assert_not_called()

        # start_publishing still called
        mock_meta_client.start_publishing.assert_called_once()


def test_cdl_publishing_config_load_failure():
    mock_dbutils = MagicMock()

    with patch("common.Configuration.load_for_default_environment_notebook", side_effect=Exception("config error")), \
         patch("common.MetaPSClient.configure") as mock_configure:

        with pytest.raises(Exception, match="config error"):
            common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)

        mock_configure.assert_not_called()


def test_cdl_publishing_publish_table_failure():
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["table1"]}

    mock_meta_client = MagicMock()
    mock_meta_client.publish_table.side_effect = Exception("publish error")

    with patch("common.Configuration.load_for_default_environment_notebook", return_value=mock_config), \
         patch("common.MetaPSClient.configure") as mock_configure:

        mock_configure.return_value.get_client.return_value = mock_meta_client

        with pytest.raises(Exception, match="publish error"):
            common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)


def test_cdl_publishing_start_publishing_failure():
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["table1"]}

    mock_meta_client = MagicMock()
    mock_meta_client.start_publishing.side_effect = Exception("start error")

    with patch("common.Configuration.load_for_default_environment_notebook", return_value=mock_config), \
         patch("common.MetaPSClient.configure") as mock_configure:

        mock_configure.return_value.get_client.return_value = mock_meta_client

        with pytest.raises(Exception, match="start error"):
            common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)

