import pytest
from unittest.mock import MagicMock, patch
import common
import types

# Patch missing attributes into common for test context
if not hasattr(common, "Configuration"):
    common.Configuration = types.SimpleNamespace()

if not hasattr(common, "MetaPSClient"):
    common.MetaPSClient = types.SimpleNamespace()


def test_cdl_publishing_success(monkeypatch):
    mock_dbutils = MagicMock()

    mock_config = {"tables": ["table1", "table2"]}
    mock_meta_client = MagicMock()

    mock_publish = MagicMock()
    mock_start = MagicMock()

    # Mock Configuration.load_for_default_environment_notebook
    monkeypatch.setattr(common.Configuration, "load_for_default_environment_notebook", lambda dbutils: mock_config)

    # Mock MetaPSClient.configure().get_client()
    monkeypatch.setattr(common.MetaPSClient, "configure", lambda config: MagicMock(get_client=lambda: mock_meta_client))

    mock_meta_client.mode.return_value.publish_table = mock_publish
    mock_meta_client.start_publishing = mock_start

    common.cdl_publishing("logical", "physical", "unity_catalog", "partition", mock_dbutils)

    # Verify publish_table called twice (for 2 tables)
    assert mock_publish.call_count == 2
    mock_start.assert_called_once()


def test_cdl_publishing_empty_tables(monkeypatch):
    mock_dbutils = MagicMock()

    mock_config = {"tables": []}
    mock_meta_client = MagicMock()

    monkeypatch.setattr(common.Configuration, "load_for_default_environment_notebook", lambda dbutils: mock_config)
    monkeypatch.setattr(common.MetaPSClient, "configure", lambda config: MagicMock(get_client=lambda: mock_meta_client))

    mock_meta_client.mode.return_value.publish_table = MagicMock()
    mock_meta_client.start_publishing = MagicMock()

    common.cdl_publishing("logical", "physical", "unity_catalog", "partition", mock_dbutils)

    # No tables to publish
    mock_meta_client.mode.return_value.publish_table.assert_not_called()
    mock_meta_client.start_publishing.assert_called_once()


def test_cdl_publishing_config_load_failure(monkeypatch):
    mock_dbutils = MagicMock()

    def raise_exception(dbutils):
        raise Exception("load fail")

    monkeypatch.setattr(common.Configuration, "load_for_default_environment_notebook", raise_exception)

    with pytest.raises(Exception, match="load fail"):
        common.cdl_publishing("logical", "physical", "unity_catalog", "partition", mock_dbutils)


def test_cdl_publishing_publish_table_failure(monkeypatch):
    mock_dbutils = MagicMock()

    mock_config = {"tables": ["table1"]}
    mock_meta_client = MagicMock()

    def raise_publish(*args, **kwargs):
        raise Exception("publish fail")

    monkeypatch.setattr(common.Configuration, "load_for_default_environment_notebook", lambda dbutils: mock_config)
    monkeypatch.setattr(common.MetaPSClient, "configure", lambda config: MagicMock(get_client=lambda: mock_meta_client))

    mock_meta_client.mode.return_value.publish_table.side_effect = raise_publish

    with pytest.raises(Exception, match="publish fail"):
        common.cdl_publishing("logical", "physical", "unity_catalog", "partition", mock_dbutils)


def test_cdl_publishing_start_publishing_failure(monkeypatch):
    mock_dbutils = MagicMock()

    mock_config = {"tables": ["table1"]}
    mock_meta_client = MagicMock()

    def raise_start():
        raise Exception("start fail")

    monkeypatch.setattr(common.Configuration, "load_for_default_environment_notebook", lambda dbutils: mock_config)
    monkeypatch.setattr(common.MetaPSClient, "configure", lambda config: MagicMock(get_client=lambda: mock_meta_client))

    mock_meta_client.mode.return_value.publish_table = MagicMock()
    mock_meta_client.start_publishing.side_effect = raise_start

    with pytest.raises(Exception, match="start fail"):
        common.cdl_publishing("logical", "physical", "unity_catalog", "partition", mock_dbutils)
