import pytest
import types
import common


@pytest.fixture
def setup_mocks(monkeypatch):
    # Mock Configuration
    mock_config_class = types.SimpleNamespace()
    mock_config_class.load_for_default_environment_notebook = lambda dbutils: {"tables": ["mock_table"]}

    # Mock MetaPSClient
    class MockMetaPSClient:
        @staticmethod
        def configure(config):
            return MockMetaPSClient()
        def get_client(self):
            return self
        def mode(self, publish_mode):
            return self
        def publish_table(self, **kwargs):
            pass
        def start_publishing(self):
            pass

    # Patch into common
    monkeypatch.setattr(common, "Configuration", mock_config_class)
    monkeypatch.setattr(common, "MetaPSClient", MockMetaPSClient)


def test_cdl_publishing_success(setup_mocks):
    mock_dbutils = object()
    common.cdl_publishing(
        logical_table_name="logical_table",
        physical_table_name="physical_table",
        unity_catalog_table_name="catalog_table",
        partition_definition_value="2024-01-01",
        dbutils=mock_dbutils
    )


def test_cdl_publishing_empty_tables(monkeypatch):
    # Mock Configuration returning empty tables
    mock_config_class = types.SimpleNamespace()
    mock_config_class.load_for_default_environment_notebook = lambda dbutils: {"tables": []}

    class MockMetaPSClient:
        @staticmethod
        def configure(config):
            return MockMetaPSClient()
        def get_client(self):
            return self
        def mode(self, publish_mode):
            return self
        def publish_table(self, **kwargs):
            pass
        def start_publishing(self):
            pass

    monkeypatch.setattr(common, "Configuration", mock_config_class)
    monkeypatch.setattr(common, "MetaPSClient", MockMetaPSClient)

    mock_dbutils = object()

    # Should run without errors even with no tables
    common.cdl_publishing(
        logical_table_name="logical_table",
        physical_table_name="physical_table",
        unity_catalog_table_name="catalog_table",
        partition_definition_value="2024-01-01",
        dbutils=mock_dbutils
    )


def test_cdl_publishing_config_load_failure(monkeypatch):
    class MockConfig:
        @staticmethod
        def load_for_default_environment_notebook(dbutils):
            raise Exception("load fail")

    monkeypatch.setattr(common, "Configuration", MockConfig)

    mock_dbutils = object()

    with pytest.raises(Exception, match="load fail"):
        common.cdl_publishing(
            logical_table_name="logical_table",
            physical_table_name="physical_table",
            unity_catalog_table_name="catalog_table",
            partition_definition_value="2024-01-01",
            dbutils=mock_dbutils
        )


def test_cdl_publishing_publish_table_failure(monkeypatch):
    mock_config_class = types.SimpleNamespace()
    mock_config_class.load_for_default_environment_notebook = lambda dbutils: {"tables": ["mock_table"]}

    class MockMetaPSClient:
        @staticmethod
        def configure(config):
            return MockMetaPSClient()
        def get_client(self):
            return self
        def mode(self, publish_mode):
            return self
        def publish_table(self, **kwargs):
            raise Exception("publish fail")
        def start_publishing(self):
            pass

    monkeypatch.setattr(common, "Configuration", mock_config_class)
    monkeypatch.setattr(common, "MetaPSClient", MockMetaPSClient)

    mock_dbutils = object()

    with pytest.raises(Exception, match="publish fail"):
        common.cdl_publishing(
            logical_table_name="logical_table",
            physical_table_name="physical_table",
            unity_catalog_table_name="catalog_table",
            partition_definition_value="2024-01-01",
            dbutils=mock_dbutils
        )


def test_cdl_publishing_start_publishing_failure(monkeypatch):
    mock_config_class = types.SimpleNamespace()
    mock_config_class.load_for_default_environment_notebook = lambda dbutils: {"tables": ["mock_table"]}

    class MockMetaPSClient:
        @staticmethod
        def configure(config):
            return MockMetaPSClient()
        def get_client(self):
            return self
        def mode(self, publish_mode):
            return self
        def publish_table(self, **kwargs):
            pass
        def start_publishing(self):
            raise Exception("start fail")

    monkeypatch.setattr(common, "Configuration", mock_config_class)
    monkeypatch.setattr(common, "MetaPSClient", MockMetaPSClient)

    mock_dbutils = object()

    with pytest.raises(Exception, match="start fail"):
        common.cdl_publishing(
            logical_table_name="logical_table",
            physical_table_name="physical_table",
            unity_catalog_table_name="catalog_table",
            partition_definition_value="2024-01-01",
            dbutils=mock_dbutils
        )
