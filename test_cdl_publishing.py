import pytest
from unittest.mock import MagicMock, patch
import common


# Mock Configuration and MetaPSClient classes since they aren't implemented in common.py
class MockConfiguration:
    @staticmethod
    def load_for_default_environment_notebook(dbutils):
        pass


class MockMetaPSClient:
    @staticmethod
    def configure(config):
        pass


# Patch these into the common module
common.Configuration = MockConfiguration
common.MetaPSClient = MockMetaPSClient


def test_cdl_publishing_success(monkeypatch):
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["TP_WK_FCT"]}
    mock_meta_client = MagicMock()

    # Patch Configuration.load_for_default_environment_notebook to return mock_config
    monkeypatch.setattr(
        common.Configuration,
        "load_for_default_environment_notebook",
        lambda dbutils: mock_config
    )

    # Patch MetaPSClient.configure to return an object with get_client method returning mock_meta_client
    monkeypatch.setattr(
        common.MetaPSClient,
        "configure",
        lambda config: MagicMock(get_client=lambda: mock_meta_client)
    )

    common.cdl_publishing(
        logical_table_name="TP_WK_FCT",
        physical_table_name="TP_WK_FCT",
        unity_catalog_table_name="TP_WK_FCT",
        partition_definition_value="2024-01-01",
        dbutils=mock_dbutils
    )

    mock_meta_client.publish_table.assert_called_once()
    mock_meta_client.start_publishing.assert_called_once()


def test_cdl_publishing_empty_tables(monkeypatch):
    mock_dbutils = MagicMock()
    mock_config = {"tables": []}
    mock_meta_client = MagicMock()

    monkeypatch.setattr(
        common.Configuration,
        "load_for_default_environment_notebook",
        lambda dbutils: mock_config
    )

    monkeypatch.setattr(
        common.MetaPSClient,
        "configure",
        lambda config: MagicMock(get_client=lambda: mock_meta_client)
    )

    common.cdl_publishing(
        logical_table_name="TP_WK_FCT",
        physical_table_name="TP_WK_FCT",
        unity_catalog_table_name="TP_WK_FCT",
        partition_definition_value="2024-01-01",
        dbutils=mock_dbutils
    )

    mock_meta_client.publish_table.assert_not_called()
    mock_meta_client.start_publishing.assert_called_once()


def test_cdl_publishing_config_load_failure(monkeypatch):
    mock_dbutils = MagicMock()

    monkeypatch.setattr(
        common.Configuration,
        "load_for_default_environment_notebook",
        lambda dbutils: (_ for _ in ()).throw(Exception("load fail"))
    )

    with pytest.raises(Exception, match="load fail"):
        common.cdl_publishing(
            logical_table_name="TP_WK_FCT",
            physical_table_name="TP_WK_FCT",
            unity_catalog_table_name="TP_WK_FCT",
            partition_definition_value="2024-01-01",
            dbutils=mock_dbutils
        )


def test_cdl_publishing_publish_table_failure(monkeypatch):
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["TP_WK_FCT"]}

    monkeypatch.setattr(
        common.Configuration,
        "load_for_default_environment_notebook",
        lambda dbutils: mock_config
    )

    class MockMetaClient:
        def publish_table(self, *args, **kwargs):
            raise Exception("publish fail")

        def start_publishing(self):
            pass

    monkeypatch.setattr(
        common.MetaPSClient,
        "configure",
        lambda config: MagicMock(get_client=lambda: MockMetaClient())
    )

    with pytest.raises(Exception, match="publish fail"):
        common.cdl_publishing(
            logical_table_name="TP_WK_FCT",
            physical_table_name="TP_WK_FCT",
            unity_catalog_table_name="TP_WK_FCT",
            partition_definition_value="2024-01-01",
            dbutils=mock_dbutils
        )


def test_cdl_publishing_start_publishing_failure(monkeypatch):
    mock_dbutils = MagicMock()
    mock_config = {"tables": ["TP_WK_FCT"]}

    monkeypatch.setattr(
        common.Configuration,
        "load_for_default_environment_notebook",
        lambda dbutils: mock_config
    )

    class MockMetaClient:
        def publish_table(self, *args, **kwargs):
            pass

        def start_publishing(self):
            raise Exception("start fail")

    monkeypatch.setattr(
        common.MetaPSClient,
        "configure",
        lambda config: MagicMock(get_client=lambda: MockMetaClient())
    )

    with pytest.raises(Exception, match="start fail"):
        common.cdl_publishing(
            logical_table_name="TP_WK_FCT",
            physical_table_name="TP_WK_FCT",
            unity_catalog_table_name="TP_WK_FCT",
            partition_definition_value="2024-01-01",
            dbutils=mock_dbutils
        )
