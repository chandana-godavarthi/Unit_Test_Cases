import pytest
from unittest.mock import patch, MagicMock
import common


def test_cdl_publishing_success():
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["table1", "table2"]}
    mock_meta_client = MagicMock()

    with patch.object(common.Configuration, 'load_for_default_environment_notebook', return_value=mock_config), \
         patch.object(common.MetaPSClient, 'configure') as mock_configure:

        mock_configure.return_value.get_client.return_value = mock_meta_client

        # Call the function
        common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)

        # Assert publish_table called for each table
        assert mock_meta_client.publish_table.call_count == len(mock_config["tables"])

        # Assert start_publishing called once
        mock_meta_client.start_publishing.assert_called_once()


def test_cdl_publishing_empty_tables():
    mock_dbutils = MagicMock()
    mock_config = {"tables": []}
    mock_meta_client = MagicMock()

    with patch.object(common.Configuration, 'load_for_default_environment_notebook', return_value=mock_config), \
         patch.object(common.MetaPSClient, 'configure') as mock_configure:

        mock_configure.return_value.get_client.return_value = mock_meta_client

        common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)

        # Should not call publish_table if no tables
        mock_meta_client.publish_table.assert_not_called()

        # But start_publishing still called once
        mock_meta_client.start_publishing.assert_called_once()


def test_cdl_publishing_config_load_failure():
    mock_dbutils = MagicMock()

    with patch.object(common.Configuration, 'load_for_default_environment_notebook', side_effect=Exception("load fail")), \
         patch.object(common.MetaPSClient, 'configure') as mock_configure:

        with pytest.raises(Exception, match="load fail"):
            common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)

        mock_configure.assert_not_called()


def test_cdl_publishing_publish_table_failure():
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["table1"]}
    mock_meta_client = MagicMock()

    # Make publish_table raise an Exception
    mock_meta_client.publish_table.side_effect = Exception("publish fail")

    with patch.object(common.Configuration, 'load_for_default_environment_notebook', return_value=mock_config), \
         patch.object(common.MetaPSClient, 'configure') as mock_configure:

        mock_configure.return_value.get_client.return_value = mock_meta_client

        with pytest.raises(Exception, match="publish fail"):
            common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)


def test_cdl_publishing_start_publishing_failure():
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["table1"]}
    mock_meta_client = MagicMock()

    # Make start_publishing raise an Exception
    mock_meta_client.start_publishing.side_effect = Exception("start fail")

    with patch.object(common.Configuration, 'load_for_default_environment_notebook', return_value=mock_config), \
         patch.object(common.MetaPSClient, 'configure') as mock_configure:

        mock_configure.return_value.get_client.return_value = mock_meta_client

        with pytest.raises(Exception, match="start fail"):
            common.cdl_publishing("logical", "physical", "unity", "partition", mock_dbutils)
