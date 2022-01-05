import json
import os
import tempfile
import time

import pytest
import requests
from fastapi import HTTPException

from .configuration import Configuration
from .models import (
    ColumnDelimiter,
    Dataset,
    EscapeCharacter,
    PipelineRequest,
    PipelineResponse,
    QuoteCharacter,
    RunResponse,
    Status,
)
from .test_common import (
    compare_files,
    download_file_from_azure_blob,
    upload_file_to_azure_blob,
)


@pytest.fixture(scope="module")
def configuration():
    return Configuration()


def test_copy_input_files(
    configuration: Configuration,
):
    print(
        f"TEST DATA FACTORY SCENARIO: through Web App  {configuration.WEB_APP_SERVER} "
    )
    configuration.SOURCE_BLOB_PATH = f"{configuration.SOURCE_BLOB_FOLDER}/{configuration.SOURCE_BLOB_FILE}"  # noqa: E501
    res = upload_file_to_azure_blob(
        local_file_path=configuration.SOURCE_LOCAL_PATH,
        account_name=configuration.STORAGE_ACCOUNT_NAME,
        container_name=configuration.SOURCE_CONTAINER,
        blob_path=configuration.SOURCE_BLOB_PATH,
    )
    assert res is True
    configuration.JOIN_BLOB_PATH = (
        f"{configuration.JOIN_BLOB_FOLDER}/{configuration.JOIN_BLOB_FILE}"  # noqa: E501
    )
    res = upload_file_to_azure_blob(
        local_file_path=configuration.JOIN_LOCAL_PATH,
        account_name=configuration.STORAGE_ACCOUNT_NAME,
        container_name=configuration.SOURCE_CONTAINER,
        blob_path=configuration.JOIN_BLOB_PATH,
    )
    assert res is True
    return


def test_create_pipeline(
    configuration: Configuration, depends=["test_copy_input_files"]
):
    try:
        pipeline_url = f"https://{configuration.WEB_APP_SERVER}/pipeline"
        headers = {
            "Content-Type": "application/json",
        }
        source = Dataset(
            resource_group_name=configuration.RESOURCE_GROUP,
            storage_account_name=configuration.STORAGE_ACCOUNT_NAME,
            container_name=configuration.SOURCE_CONTAINER,
            folder_path=configuration.SOURCE_BLOB_FOLDER,
            file_pattern_or_name=configuration.SOURCE_BLOB_FILE,
            first_row_as_header=True,
            column_delimiter=ColumnDelimiter.SEMICOLON.value,
            quote_char=QuoteCharacter.DOUBLE_QUOTE.value,
            escape_char=EscapeCharacter.DOUBLE_QUOTE.value,
        )
        join = Dataset(
            resource_group_name=configuration.RESOURCE_GROUP,
            storage_account_name=configuration.STORAGE_ACCOUNT_NAME,
            container_name=configuration.SOURCE_CONTAINER,
            folder_path=configuration.JOIN_BLOB_FOLDER,
            file_pattern_or_name=configuration.JOIN_BLOB_FILE,
            first_row_as_header=True,
            column_delimiter=ColumnDelimiter.SEMICOLON.value,
            quote_char=QuoteCharacter.DOUBLE_QUOTE.value,
            escape_char=EscapeCharacter.DOUBLE_QUOTE.value,
        )
        sink = Dataset(
            resource_group_name=configuration.RESOURCE_GROUP,
            storage_account_name=configuration.STORAGE_ACCOUNT_NAME,
            container_name=configuration.SINK_CONTAINER,
            folder_path=configuration.SINK_BLOB_FOLDER,
            file_pattern_or_name=configuration.SINK_BLOB_FILE,
            first_row_as_header=True,
            column_delimiter=ColumnDelimiter.SEMICOLON.value,
            quote_char=QuoteCharacter.DOUBLE_QUOTE.value,
            escape_char=EscapeCharacter.DOUBLE_QUOTE.value,
        )
        columns = json.loads(configuration.SELECTED_COLUMNS)
        pipeline_request = PipelineRequest(
            source=source, join=join, columns=columns, sink=sink
        )
        pipeline_response = requests.post(
            url=pipeline_url,
            json=pipeline_request.dict(),
            headers=headers,
        )
        pipeline_response.raise_for_status()
        response: PipelineResponse = json.loads(pipeline_response.text)
        pipeline_name = response["pipeline_name"]
        print(f"Pipeline created: name: {pipeline_name}")
        configuration.PIPELINE_NAME = pipeline_name
        result = True
    except HTTPException as e:
        print(f"HTTPException while calling {pipeline_url}: {repr(e)}")
        result = False
    except Exception as ex:
        print(f"Exception while calling {pipeline_url}: {repr(ex)}")
        result = False
    assert result is True


def test_get_pipeline(configuration: Configuration, depends=["test_create_pipeline"]):
    try:
        pipeline_url = f"https://{configuration.WEB_APP_SERVER}/pipeline/{configuration.PIPELINE_NAME}"
        headers = {
            "Content-Type": "application/json",
        }
        pipeline_response = requests.get(
            url=pipeline_url,
            headers=headers,
        )
        pipeline_response.raise_for_status()
        response: PipelineResponse = json.loads(pipeline_response.text)
        pipeline_name = response["pipeline_name"]
        print(f"Get Pipeline: name: {pipeline_name}")
        configuration.PIPELINE_NAME = pipeline_name
        result = True
    except HTTPException as e:
        print(f"HTTPException while calling {pipeline_url}: {repr(e)}")
        result = False
    except Exception as ex:
        print(f"Exception while calling {pipeline_url}: {repr(ex)}")
        result = False

    assert result is True


def test_trigger_run(configuration: Configuration, depends=["test_get_share_status"]):
    try:
        run_url = f"https://{configuration.WEB_APP_SERVER}/pipeline/{configuration.PIPELINE_NAME}/run"
        headers = {
            "Content-Type": "application/json",
        }
        run_response = requests.post(
            url=run_url,
            headers=headers,
        )
        run_response.raise_for_status()
        response: RunResponse = json.loads(run_response.text)
        run_id = response["run_id"]
        print(f"Run launched, run_id: {run_id}")
        configuration.RUN_ID = run_id
        result = True
    except HTTPException as e:
        print(f"HTTPException while calling {run_url}: {repr(e)}")
        result = False
    except Exception as ex:
        print(f"Exception while calling {run_url}: {repr(ex)}")
        result = False
    assert result is True


def test_wait_for_run_completion(
    configuration: Configuration, depends=["test_trigger_run"]
):
    try:
        result = False
        status = Status.IN_PROGRESS
        while status != Status.FAILED and status != Status.SUCCEEDED:
            run_url = f"https://{configuration.WEB_APP_SERVER}/pipeline/{configuration.PIPELINE_NAME}/run/{configuration.RUN_ID}"
            headers = {
                "Content-Type": "application/json",
            }
            run_response = requests.get(
                url=run_url,
                headers=headers,
            )
            run_response.raise_for_status()
            response: RunResponse = json.loads(run_response.text)
            assert response["run_id"] == configuration.RUN_ID
            assert response["pipeline_name"] == configuration.PIPELINE_NAME
            status = response["status"]["status"]
            if status == Status.FAILED:
                message = response["error"]["message"]
                print(
                    f"Run Status for pipeline '{configuration.PIPELINE_NAME}' run_id '{configuration.RUN_ID}' message: {message}"
                )
            else:
                if status == Status.SUCCEEDED:
                    print(
                        f"Run Status for pipeline '{configuration.PIPELINE_NAME}' run_id '{configuration.RUN_ID}' successful. Dataset in file: {configuration.SINK_BLOB_FOLDER}/{configuration.SINK_BLOB_FILE} "
                    )
                    result = True
                else:
                    print(
                        f"Run Status for pipeline '{configuration.PIPELINE_NAME}' run_id '{configuration.RUN_ID}' status: '{status}'"
                    )
            time.sleep(20)

    except HTTPException as e:
        print(f"HTTPException while calling {run_url}: {repr(e)}")
        result = False
    except Exception as ex:
        print(f"Exception while calling {run_url}: {repr(ex)}")
        result = False
    assert result is True


def test_check_received_file(
    configuration: Configuration, depends=["test_wait_for_run_completion"]
):
    # create temporary directory
    temp_dir = tempfile.TemporaryDirectory()
    result_local_path = f"{temp_dir.name}/{configuration.SINK_BLOB_FILE}"

    #
    # Download the result file
    #
    # print(f"download from storage: {configuration.STORAGE_ACCOUNT_NAME} container: {configuration.SINK_CONTAINER} file: {configuration.SINK_BLOB_FOLDER}/{configuration.SINK_BLOB_FILE} ")
    # print(f"local file: {result_local_path} ")
    result = download_file_from_azure_blob(
        local_file_path=result_local_path,
        account_name=configuration.STORAGE_ACCOUNT_NAME,
        container_name=configuration.SINK_CONTAINER,
        blob_path=f"{configuration.SINK_BLOB_FOLDER}/{configuration.SINK_BLOB_FILE}",
    )
    assert result is True
    #
    # Check if the result file is correct
    #
    result = compare_files(
        first_file_path=configuration.SINK_LOCAL_PATH,
        second_file_path=result_local_path,
    )
    assert result is True

    if os.path.exists(result_local_path):
        os.remove(result_local_path)
    # use temp_dir, and when done:
    temp_dir.cleanup()

    print(
        f"TEST DATA FACTORY SCENARIO SUCCESSFUL: through Web App  {configuration.WEB_APP_SERVER} "
    )
