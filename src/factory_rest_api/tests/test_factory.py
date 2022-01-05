from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from src.configuration_service import ConfigurationService
from src.factory_service import FactoryService
from src.models import (
    ColumnDelimiter,
    Dataset,
    EscapeCharacter,
    PipelineRequest,
    QuoteCharacter,
)


def get_configuration_service() -> ConfigurationService:
    """Getting a single instance of the LogService"""
    return ConfigurationService()


@pytest.fixture(scope="function")
def factory_service():
    return FactoryService(
        subscription_id=get_configuration_service().get_subscription_id(),
        resource_group_name=get_configuration_service().get_datafactory_resource_group_name(),
        datafactory_name=get_configuration_service().get_datafactory_account_name(),
        source_linked_service=get_configuration_service().get_datafactory_source_linked_service(),
        sink_linked_service=get_configuration_service().get_datafactory_sink_linked_service(),
    )


def test_create_pipeline(client: TestClient):
    with patch(
        "src.factory_service.FactoryService.initialize_azure_clients"
    ) as mock_initialize_azure_clients:
        source = Dataset(
            resource_group_name="RESOURCE_GROUP",
            storage_account_name="STORAGE_ACCOUNT_NAME",
            container_name="SOURCE_CONTAINER",
            folder_path="SOURCE_BLOB_FOLDER",
            file_pattern_or_name="SOURCE_BLOB_FILE",
            first_row_as_header=True,
            column_delimiter=ColumnDelimiter.SEMICOLON.value,
            quote_char=QuoteCharacter.DOUBLE_QUOTE.value,
            escape_char=EscapeCharacter.DOUBLE_QUOTE.value,
        )
        join = Dataset(
            resource_group_name="RESOURCE_GROUP",
            storage_account_name="STORAGE_ACCOUNT_NAME",
            container_name="SOURCE_CONTAINER",
            folder_path="JOIN_BLOB_FOLDER",
            file_pattern_or_name="JOIN_BLOB_FILE",
            first_row_as_header=True,
            column_delimiter=ColumnDelimiter.SEMICOLON.value,
            quote_char=QuoteCharacter.DOUBLE_QUOTE.value,
            escape_char=EscapeCharacter.DOUBLE_QUOTE.value,
        )
        sink = Dataset(
            resource_group_name="RESOURCE_GROUP",
            storage_account_name="STORAGE_ACCOUNT_NAME",
            container_name="SINK_CONTAINER",
            folder_path="SINK_BLOB_FOLDER",
            file_pattern_or_name="SINK_BLOB_FILE",
            first_row_as_header=True,
            column_delimiter=ColumnDelimiter.SEMICOLON.value,
            quote_char=QuoteCharacter.DOUBLE_QUOTE.value,
            escape_char=EscapeCharacter.DOUBLE_QUOTE.value,
        )
        columns = ["key", "phone", "email"]
        pipeline_request = PipelineRequest(
            source=source, join=join, columns=columns, sink=sink
        )
        mock_initialize_azure_clients.return_value = True
        pipeline_response = client.post(
            url="/pipeline",
            json=pipeline_request.dict(),
            headers={"accept": "application/json", "Content-Type": "application/json"},
        )
        assert pipeline_response.status_code == 200


def test_get_pipeline(client: TestClient):
    with patch(
        "src.factory_service.FactoryService.initialize_azure_clients"
    ) as mock_initialize_azure_clients:
        pipeline_name = "Pipeline0000000"
        mock_initialize_azure_clients.return_value = True
        pipeline_response = client.get(
            url=f"/pipeline/{pipeline_name}",
            headers={"accept": "application/json", "Content-Type": "application/json"},
        )
        assert pipeline_response.status_code == 200


def test_launch_pipeline_run(client: TestClient):
    with patch(
        "src.factory_service.FactoryService.initialize_azure_clients"
    ) as mock_initialize_azure_clients:
        pipeline_name = "Pipeline0000000"
        mock_initialize_azure_clients.return_value = True
        pipeline_response = client.post(
            url=f"/pipeline/{pipeline_name}/run",
            headers={"accept": "application/json", "Content-Type": "application/json"},
        )
        assert pipeline_response.status_code == 200


def test_get_pipeline_run(client: TestClient):
    with patch(
        "src.factory_service.FactoryService.initialize_azure_clients"
    ) as mock_initialize_azure_clients:
        pipeline_name = "Pipeline0000000"
        run_id = "00000000000"
        mock_initialize_azure_clients.return_value = True
        pipeline_response = client.get(
            url=f"/pipeline/{pipeline_name}/run/{run_id}",
            headers={"accept": "application/json", "Content-Type": "application/json"},
        )
        assert pipeline_response.status_code == 200


def test_factory_service_get_folder_file_from_script(factory_service):
    with patch(
        "src.factory_service.FactoryService.initialize_azure_clients"
    ) as mock_initialize_azure_clients:
        mock_initialize_azure_clients.return_value = True
        folder, file = factory_service.get_folder_file_from_script("")
        assert folder == ""
        assert file == ""


def test_factory_service_get_sink_file_from_script(factory_service):
    with patch(
        "src.factory_service.FactoryService.initialize_azure_clients"
    ) as mock_initialize_azure_clients:
        mock_initialize_azure_clients.return_value = True
        file = factory_service.get_sink_file_from_script("")
        assert file == ""


def test_factory_service_get_column_list_from_script(factory_service):
    with patch(
        "src.factory_service.FactoryService.initialize_azure_clients"
    ) as mock_initialize_azure_clients:
        mock_initialize_azure_clients.return_value = True
        columnlist = factory_service.get_column_list_from_script("")
        assert columnlist == []


def test_factory_service_get_storage_account_name_from_endpoint(factory_service):
    with patch(
        "src.factory_service.FactoryService.initialize_azure_clients"
    ) as mock_initialize_azure_clients:
        mock_initialize_azure_clients.return_value = True
        storage = factory_service.get_storage_account_name_from_endpoint("")
        assert storage == ""
