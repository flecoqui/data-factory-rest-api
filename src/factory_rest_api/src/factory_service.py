import hashlib
import json
import os
from datetime import datetime
from enum import Enum
from typing import Any, List

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    AzureBlobStorageLocation,
    DataFlowReference,
    DataFlowResource,
    DataFlowSink,
    DataFlowSource,
    DatasetReference,
    DatasetResource,
    DelimitedTextDataset,
    ExecuteDataFlowActivity,
    LinkedServiceReference,
    MappingDataFlow,
    PipelineResource,
    Transformation,
)
from fastapi import HTTPException
from src.configuration_service import ConfigurationService
from src.log_service import LogService
from src.models import (
    ColumnDelimiter,
    Dataset,
    Error,
    EscapeCharacter,
    PipelineRequest,
    PipelineResponse,
    QuoteCharacter,
    RunResponse,
    Status,
    StatusDetails,
)


class FactoryServiceError(int, Enum):
    NO_ERROR = 0
    DATA_FACTORY_ERROR = 1
    PIPELINE_CREATION_ERROR = 2
    DATAFLOW_CREATION_ERROR = 3
    RUN_PIPELINE_ERROR = 4
    RUN_PIPELINE_EXCEPTION = 5
    PIPELINE_ID_NOT_FOUND = 6
    PIPELINE_GET_EXCEPTION = 7


def get_log_service() -> LogService:
    """Getting a single instance of the LogService"""
    return LogService()


def get_configuration_service() -> ConfigurationService:
    """Getting a single instance of the ConfigurationService"""
    return ConfigurationService()


class FactoryService:
    """Class used to implement the datafactory service"""

    PIPELINE_PREFIX = "Pipeline"
    PIPELINE_ID = "Pipeline_id"
    SOURCE_DATASET = "SourceDataset"
    JOIN_DATASET = "JoinDataset"
    SINK_DATASET = "SinkDataset"
    DATA_FLOW = "DataFlow-"
    JOIN_FLOW = "JoinFlow"
    SELECT_FLOW = "SelectFlow"
    ACTIVITY = "Activity"

    def __init__(
        self,
        subscription_id: str,
        resource_group_name: str,
        datafactory_name: str,
        source_linked_service: str,
        sink_linked_service: str,
    ):  # pragma: no cover
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.datafactory_name = datafactory_name
        self.source_linked_service = source_linked_service
        self.sink_linked_service = sink_linked_service
        if not self.initialize_azure_clients():
            raise HTTPException(status_code=500, detail="Internal server error.")

    def initialize_azure_clients(self) -> bool:  # pragma: no cover
        try:
            credentials = DefaultAzureCredential()

            self.adf_client = DataFactoryManagementClient(
                credentials, self.subscription_id
            )
        except Exception:
            return False
        return True

    def set_env_value(self, variable: str, value: str) -> str:
        """set environment variable value (string type)"""
        if not os.environ.get(variable):
            os.environ.setdefault(variable, value)
        else:
            os.environ[variable] = value
        return value

    def get_env_value(self, variable: str, default_value: str) -> str:
        """get environment variable value (string type)"""
        if not os.environ.get(variable):
            os.environ.setdefault(variable, default_value)
            return default_value
        else:
            return os.environ[variable]

    def serialize(self, o):
        """serialize object"""
        if isinstance(o, dict):
            return {k: self.serialize(v) for k, v in o.items()}
        if isinstance(o, list):
            return [self.serialize(e) for e in o]
        if isinstance(o, bytes):
            return o.decode("utf-8")
        if isinstance(o, datetime):
            return o.isoformat()
        return o

    def set_memory_persistent_node_list(self, list: Any):
        """store list in environment variable (string)"""
        o = self.serialize(list)
        self.set_env_value("SHARE_NODE_LIST", json.dumps(o))

    def raise_http_exception(self, code: int, message: str, detail: str):
        """raise HTTP exception"""
        if not detail:
            get_log_service().log_error(
                f"HTTP EXCEPTION code: {code} message: {message}"
            )
        else:
            get_log_service().log_error(
                f"HTTP EXCEPTION code: {code} message: {message} detail: {detail}"
            )
        error = Error(
            code=code, message=message, source="shareservice", date=datetime.utcnow()
        )
        raise HTTPException(
            status_code=code, detail=json.dumps(self.serialize(error.dict()))
        )

    def pipeline(self, pipeline: PipelineRequest) -> PipelineResponse:
        """
        Create Pipeline
        with the following parameters:
            PipelineRequest
        """
        try:
            pipelineresponse = self.create_data_flow(input=pipeline)
            return pipelineresponse
        except Exception as ex:
            get_log_service().log_error(f"EXCEPTION in pipeline: {ex}")
            return None

    def pipeline_status(self, pipeline_name: str) -> PipelineResponse:
        """
        Return Pipeline
        with the following parameters:
            pipeline name
        """
        pipelineresponse = self.get_data_flow(pipeline_name=pipeline_name)
        return pipelineresponse

    def run(self, pipeline_name: str) -> RunResponse:
        """
        Create Run
        with the following parameters:
            RunRequest
        """
        runresponse = self.run_data_flow(pipeline_name)
        return runresponse

    def run_status(self, pipeline_name: str, run_id=str) -> RunResponse:
        """
        Create Pipeline
        with the following parameters:
            RunRequest
        """
        try:
            runresponse = self.get_run_data_flow_status(pipeline_name, run_id)
            return runresponse
        except Exception as ex:
            get_log_service().log_error(f"EXCEPTION in run_status: {ex}")
            return None

    def get_hash(
        self,
        input: PipelineRequest,
    ) -> str:
        """
        Return the hash associated with the blob storage where the dataset
        is stored
        """
        text = f"{input.source.resource_group_name}-\
{input.source.storage_account_name}-\
{input.source.container_name}-\
{input.source.folder_path}-\
{input.source.file_pattern_or_name}-\
{input.join.resource_group_name}-\
{input.join.storage_account_name}-\
{input.join.container_name}-\
{input.join.folder_path}-\
{input.join.file_pattern_or_name}-\
{input.sink.resource_group_name}-\
{input.sink.storage_account_name}-\
{input.sink.container_name}-\
{input.sink.folder_path}-\
{input.sink.file_pattern_or_name}-\
{'-'.join(input.columns)}"
        hash_object = hashlib.md5(text.encode())
        return hash_object.hexdigest()

    def create_data_flow(
        self,
        input: PipelineRequest,
    ) -> PipelineResponse:  # pragma: no cover
        pipeline_id = self.get_hash(input)
        # Data factory constants used to create data flow and pipeline

        source_dataset_name = f"{FactoryService.SOURCE_DATASET}{pipeline_id}"
        join_dataset_name = f"{FactoryService.JOIN_DATASET}{pipeline_id}"
        sink_dataset_name = f"{FactoryService.SINK_DATASET}{pipeline_id}"
        join_flow_name = f"{FactoryService.JOIN_FLOW}{pipeline_id}"
        select_flow_name = f"{FactoryService.SELECT_FLOW}{pipeline_id}"
        dataflow_name = f"{FactoryService.DATA_FLOW}{pipeline_id}"
        activity_name = f"{FactoryService.ACTIVITY}"
        pipeline_name = f"{FactoryService.PIPELINE_PREFIX}{pipeline_id}"

        sink_linked_service = LinkedServiceReference(
            reference_name=self.sink_linked_service
        )
        source_linked_service = LinkedServiceReference(
            reference_name=self.source_linked_service
        )

        # Folder path and file name not set
        # in source definition
        # will be defined in ADF script
        source_location = AzureBlobStorageLocation(
            container=input.source.container_name,
            folder_path="",
            file_name="",
        )
        join_location = AzureBlobStorageLocation(
            container=input.join.container_name,
            folder_path=input.join.folder_path,
            file_name=input.join.file_pattern_or_name,
        )
        sink_location = AzureBlobStorageLocation(
            container=input.sink.container_name,
            folder_path=input.sink.folder_path,
            file_name="",
        )

        source_dataset_resource = DatasetResource(
            properties=DelimitedTextDataset(
                linked_service_name=source_linked_service,
                location=source_location,
                first_row_as_header=input.source.first_row_as_header,
                column_delimiter=input.source.column_delimiter,
                quote_char=input.source.quote_char,
                escape_char=input.source.escape_char,
            )
        )
        join_dataset_resource = DatasetResource(
            properties=DelimitedTextDataset(
                linked_service_name=source_linked_service,
                location=join_location,
                first_row_as_header=input.join.first_row_as_header,
                column_delimiter=input.join.column_delimiter,
                quote_char=input.join.quote_char,
                escape_char=input.join.escape_char,
            )
        )
        sink_dataset_resource = DatasetResource(
            properties=DelimitedTextDataset(
                linked_service_name=sink_linked_service,
                location=sink_location,
                first_row_as_header=input.sink.first_row_as_header,
                column_delimiter=input.sink.column_delimiter,
                quote_char=input.sink.quote_char,
                escape_char=input.sink.escape_char,
            )
        )

        source_dataset = self.adf_client.datasets.create_or_update(
            self.resource_group_name,
            self.datafactory_name,
            source_dataset_name,
            source_dataset_resource,
        )
        if source_dataset is None:
            raise HTTPException(status_code=500, detail="Internal server error.")

        join_dataset = self.adf_client.datasets.create_or_update(
            self.resource_group_name,
            self.datafactory_name,
            join_dataset_name,
            join_dataset_resource,
        )
        if join_dataset is None:
            raise HTTPException(status_code=500, detail="Internal server error.")

        sink_dataset = self.adf_client.datasets.create_or_update(
            self.resource_group_name,
            self.datafactory_name,
            sink_dataset_name,
            sink_dataset_resource,
        )
        if sink_dataset is None:
            raise HTTPException(status_code=500, detail="Internal server error.")

        source_data_flow_source = DataFlowSource(
            name=source_dataset_name,
            dataset=DatasetReference(reference_name=source_dataset_name),
        )
        join_data_flow_source = DataFlowSource(
            name=join_dataset_name,
            dataset=DatasetReference(reference_name=join_dataset_name),
        )
        sink_data_flow_sink = DataFlowSink(
            name=sink_dataset_name,
            dataset=DatasetReference(reference_name=sink_dataset_name),
        )

        # create transformations
        join_data_flow = Transformation(name=join_flow_name)
        select_data_flow = Transformation(name=select_flow_name)

        # if input.sink.file_pattern_or_name contains "-00001"
        # remove this substring as it will be automatically added
        # by data factory
        sink_pattern = input.sink.file_pattern_or_name.replace("-00001.", ".")
        data_flow = MappingDataFlow(
            description="Prepare Data Flow",
            sources=[source_data_flow_source, join_data_flow_source],
            sinks=[sink_data_flow_sink],
            transformations=[join_data_flow, select_data_flow],
            script=self.get_data_flow_script(
                source_data_source_name=source_dataset_name,
                join_data_source_name=join_dataset_name,
                sink_data_source_name=sink_dataset_name,
                join_data_flow_name=join_flow_name,
                select_data_flow_name=select_flow_name,
                output_file_name=sink_pattern,
                columns=input.columns,
                source_folder_path=input.source.folder_path,
                source_file_name=input.source.file_pattern_or_name,
            ),
        )

        data_flow_resource = DataFlowResource(properties=data_flow)

        data_flow_created = self.adf_client.data_flows.create_or_update(
            resource_group_name=self.resource_group_name,
            factory_name=self.datafactory_name,
            data_flow_name=dataflow_name,
            data_flow=data_flow_resource,
        )
        if data_flow_created is None:
            pipeline_response = self.create_pipeline_response(
                pipeline_request=input,
                pipeline_name=pipeline_name,
                error_code=FactoryServiceError.DATAFLOW_CREATION_ERROR,
                error_message=f"Pipeline Data Flow creation failed for {dataflow_name}",
            )
            return pipeline_response

        data_flow_ref = DataFlowReference(reference_name=dataflow_name)
        data_flow_activity = ExecuteDataFlowActivity(
            name=activity_name, data_flow=data_flow_ref
        )

        # Create a pipeline with the copy activity
        params_for_pipeline = {f"{FactoryService.PIPELINE_ID}": pipeline_id}
        tags_for_pipeline = [
            "{PIPELINE_ID}:{id}".format(
                PIPELINE_ID=FactoryService.PIPELINE_ID, id=pipeline_id
            )
        ]
        p_obj = PipelineResource(
            activities=[data_flow_activity],
            parameters={},
            annotations=tags_for_pipeline,
            additional_properties=params_for_pipeline,
        )
        p = self.adf_client.pipelines.create_or_update(
            self.resource_group_name, self.datafactory_name, pipeline_name, p_obj
        )
        if p is None:
            pipeline_response = self.create_pipeline_response(
                pipeline_request=input,
                pipeline_name=pipeline_name,
                error_code=FactoryServiceError.PIPELINE_CREATION_ERROR,
                error_message=f"Pipeline creation failed for {pipeline_name}",
            )
            return pipeline_response

        pipeline_response = self.create_pipeline_response(
            pipeline_request=input,
            pipeline_name=pipeline_name,
            error_code=FactoryServiceError.NO_ERROR,
            error_message="",
        )
        return pipeline_response

    def get_data_flow(
        self,
        pipeline_name: str,
    ) -> PipelineResponse:  # pragma: no cover

        try:
            # Get pipeline from pipeline name
            pipeline_resource = self.adf_client.pipelines.get(
                self.resource_group_name, self.datafactory_name, pipeline_name
            )
            if pipeline_resource is not None:
                pipeline_response = self.get_pipeline_response(
                    pipeline_name=pipeline_name,
                    error_code=FactoryServiceError.NO_ERROR,
                    error_message="",
                )
                return pipeline_response
        except Exception as ex:
            if hasattr(ex, "reason") and ex.reason == "Not Found":
                raise HTTPException(status_code=ex.status_code, detail=ex.message)

            pipeline_response = self.create_pipeline_response(
                pipeline_request=None,
                pipeline_name=pipeline_name,
                error_code=FactoryServiceError.PIPELINE_GET_EXCEPTION,
                error_message=f"Exception while getting pipeline {pipeline_name}: {ex}",
            )
            return pipeline_response

    def run_data_flow(
        self,
        pipeline_name: str,
    ) -> RunResponse:  # pragma: no cover

        try:
            pipeline_id = pipeline_name.replace(FactoryService.PIPELINE_PREFIX, "")

            # Create a pipeline run
            create_run_response = self.adf_client.pipelines.create_run(
                self.resource_group_name,
                self.datafactory_name,
                pipeline_name,
                parameters={f"{FactoryService.PIPELINE_ID}": pipeline_id},
            )
        except Exception as ex:
            run_response = self.create_run_response(
                run_id="",
                pipeline_name=pipeline_name,
                status=Status.FAILED,
                start=datetime.utcnow(),
                end=datetime.utcnow(),
                duration_in_ms=0,
                error_code=FactoryServiceError.RUN_PIPELINE_EXCEPTION,
                error_message=f"Run pipeline exception: {ex}",
            )
            return run_response

        if create_run_response is not None and create_run_response.run_id is not None:
            run_response = self.create_run_response(
                run_id=create_run_response.run_id,
                pipeline_name=pipeline_name,
                status=Status.IN_PROGRESS,
                start=datetime.utcnow(),
                end=datetime.utcnow(),
                duration_in_ms=0,
                error_code=FactoryServiceError.NO_ERROR,
                error_message="",
            )
        else:
            run_response = self.create_run_response(
                run_id="",
                pipeline_name=pipeline_name,
                status=Status.FAILED,
                start=datetime.utcnow(),
                end=datetime.utcnow(),
                duration_in_ms=0,
                error_code=FactoryServiceError.RUN_PIPELINE_ERROR,
                error_message="Run pipeline error: create_run() return None",
            )
        return run_response

    def get_run_data_flow_status(
        self, pipeline_name: str, run_id: str
    ) -> RunResponse:  # pragma: no cover

        try:
            pipeline_id = pipeline_name.replace(FactoryService.PIPELINE_PREFIX, "")

            pipeline_run = self.adf_client.pipeline_runs.get(
                self.resource_group_name, self.datafactory_name, run_id
            )
            run_pipeline_id = ""
            if pipeline_run.additional_properties["annotations"] is not None:
                for x in pipeline_run.additional_properties["annotations"]:
                    if x.startswith(f"{FactoryService.PIPELINE_ID}:"):
                        run_pipeline_id = x[12:]
                        break

            if pipeline_id == run_pipeline_id:
                run_response = self.create_run_response(
                    run_id=run_id,
                    pipeline_name=pipeline_name,
                    status=pipeline_run.status,
                    start=datetime.utcnow()
                    if pipeline_run.run_start is None
                    else pipeline_run.run_start,
                    end=datetime.utcnow()
                    if pipeline_run.run_end is None
                    else pipeline_run.run_end,
                    duration_in_ms=0
                    if pipeline_run.duration_in_ms is None
                    else pipeline_run.duration_in_ms,
                    error_code=FactoryServiceError.NO_ERROR,
                    error_message=""
                    if pipeline_run.message is None
                    else pipeline_run.message,
                )
            else:
                run_response = self.create_run_response(
                    run_id=run_id,
                    pipeline_name=pipeline_name,
                    status=Status.FAILED,
                    start=datetime.utcnow(),
                    end=datetime.utcnow(),
                    duration_in_ms=0,
                    error_code=FactoryServiceError.PIPELINE_ID_NOT_FOUND,
                    error_message=f"Pipeline id '{pipeline_id}' not found",
                )

        except Exception as ex:
            run_response = self.create_run_response(
                run_id=run_id,
                pipeline_name=pipeline_name,
                status=Status.FAILED,
                start=datetime.utcnow(),
                end=datetime.utcnow(),
                duration_in_ms=0,
                error_code=FactoryServiceError.RUN_PIPELINE_EXCEPTION,
                error_message=f"Run pipeline exception: {ex}",
            )
            return run_response

        return run_response

    def get_data_flow_script(
        self,
        source_data_source_name: str,
        join_data_source_name: str,
        sink_data_source_name: str,
        join_data_flow_name: str,
        select_data_flow_name: str,
        output_file_name: str,
        columns: List[str],
        source_folder_path: str,
        source_file_name: str,
    ) -> str:  # pragma: no cover
        typed_columns_list = ""
        columns_list = ""
        source_key_name = ""
        # shared_key_name = ""

        # This loop below will initialize 2 strings
        # - typed_columns_list = ""
        # - columns_list = ""
        # Those two line will contain json list which will be integrated in the
        # data flow script.
        # Those two lines will contain the list of selected columns.
        # The first column will be the key used for the inner join

        if columns:
            # source_key_name = columns[0]
            # shared_key_name = columns[1]
            # typed_columns_list = (
            #    f"\n\t\t{{{source_key_name}}} as string,\n\t\t"
            #    + ",\n\t\t".join(f"{col} as string" for col in columns[2:])
            # )
            # columns_list = f"\n\t\t{shared_key_name},\n\t\t" + ",\n\t\t".join(
            #    columns[2:]
            # )
            source_key_name = columns[0]
            typed_columns_list = (
                f"\n\t\t{{{source_key_name}}} as string,\n\t\t"
                + ",\n\t\t".join(f"{col} as string" for col in columns[1:])
            )
            columns_list = f"\n\t\t{source_key_name},\n\t\t" + ",\n\t\t".join(
                columns[1:]
            )
        else:
            source_key_name = ""
            # shared_key_name = ""
            typed_columns_list = ""
            columns_list = ""

        script = """source(output({typed_columns}),
        allowSchemaDrift: true,
        validateSchema: false,
        ignoreNoFilesFound: false,
        wildcardPaths:['{source_folder_path}/{source_file_name}']) ~> {source_ds}
    source(output(
                    {{{key}}} as string
        ),
        allowSchemaDrift: true,
        validateSchema: false,
        ignoreNoFilesFound: false) ~> {join_ds}
    {source_ds}, {join_ds} join({source_ds}@{{{key}}} == {join_ds}@{{{key}}},
        joinType:'inner',
        broadcast: 'auto') ~> {join_df}
    {join_df} select(mapColumn(
                {columns}
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> {select_df}
    {select_df} sink(allowSchemaDrift: true,
        validateSchema: false,
        filePattern:'{file_name}') ~> {sink_ds}""".format(
            typed_columns=typed_columns_list,
            columns=columns_list,
            source_ds=source_data_source_name,
            key=source_key_name,
            join_ds=join_data_source_name,
            join_df=join_data_flow_name,
            select_df=select_data_flow_name,
            sink_ds=sink_data_source_name,
            file_name=output_file_name,
            source_folder_path=source_folder_path,
            source_file_name=source_file_name,
        )

        return script

    def get_storage_account_name_from_endpoint(self, end_point: str) -> str:
        try:
            start = "https://"
            end = ".blob"
            result = end_point[
                end_point.index(start) + len(start): end_point.index(end)
            ]
        except Exception:
            result = ""
        return result

    def get_column_list_from_script(self, script: str) -> List[str]:
        try:
            start = "select(mapColumn(\n                "
            end = "\n        ),\n        skipDuplicateMapInputs:"
            result = script[script.index(start) + len(start): script.index(end)]
            result = result.replace("\n\t\t", "")
            li = result.split(",")
        except Exception:
            li = []
        return li

    def get_folder_file_from_script(self, script: str):
        try:
            start = "wildcardPaths:['"
            end = "']) ~> Source"
            result = script[script.index(start) + len(start): script.index(end)]
            file = result[result.rfind("/") + 1:]
            folder = result[0: result.rfind("/")]
            return folder, file
        except Exception:
            folder = ""
            file = ""
        return folder, file

    def get_sink_file_from_script(self, script: str) -> str:
        try:
            start = "filePattern:'"
            end = "') ~> Sink"
            file = script[script.index(start) + len(start): script.index(end)]
            return file
        except Exception:
            file = ""
        return file

    def get_pipeline_response(
        self,
        pipeline_name: str,
        error_code: int,
        error_message: str,
    ) -> PipelineResponse:
        """
        Get a PipelineResponse using the pipeline name
        """

        error = Error(
            code=error_code,
            message=error_message,
            source="factory_rest_api",
            date=datetime.utcnow(),
        )
        pipeline_id = pipeline_name.replace(f"{FactoryService.PIPELINE_PREFIX}", "")

        source_dataset_name = f"{FactoryService.SOURCE_DATASET}{pipeline_id}"
        join_dataset_name = f"{FactoryService.JOIN_DATASET}{pipeline_id}"
        sink_dataset_name = f"{FactoryService.SINK_DATASET}{pipeline_id}"
        dataflow_name = f"{FactoryService.DATA_FLOW}{pipeline_id}"

        source_dataset = self.adf_client.datasets.get(
            self.resource_group_name,
            self.datafactory_name,
            source_dataset_name,
        )
        if source_dataset is not None:
            location = source_dataset.properties.location
            linked_service_name = (
                source_dataset.properties.linked_service_name.reference_name
            )
            service = self.adf_client.linked_services.get(
                self.resource_group_name, self.datafactory_name, linked_service_name
            )
            if service is not None:
                dataset_source = Dataset(
                    resource_group_name=self.resource_group_name,
                    storage_account_name=self.get_storage_account_name_from_endpoint(
                        service.properties.service_endpoint
                    ),
                    container_name=location.container,
                    folder_path=location.folder_path,
                    file_pattern_or_name=location.file_name,
                    first_row_as_header=True
                    if source_dataset.properties.first_row_as_header is None
                    else source_dataset.properties.first_row_as_header,
                    column_delimiter=ColumnDelimiter.SEMICOLON.value
                    if source_dataset.properties.column_delimiter is None
                    else source_dataset.properties.column_delimiter,
                    quote_char=QuoteCharacter.DOUBLE_QUOTE.value
                    if source_dataset.properties.quote_char is None
                    else source_dataset.properties.quote_char,
                    escape_char=EscapeCharacter.DOUBLE_QUOTE.value
                    if source_dataset.properties.escape_char is None
                    else source_dataset.properties.escape_char,
                )

        join_dataset = self.adf_client.datasets.get(
            self.resource_group_name,
            self.datafactory_name,
            join_dataset_name,
        )
        if join_dataset is not None:
            location = join_dataset.properties.location
            linked_service_name = (
                join_dataset.properties.linked_service_name.reference_name
            )
            service = self.adf_client.linked_services.get(
                self.resource_group_name, self.datafactory_name, linked_service_name
            )
            if service is not None:
                dataset_join = Dataset(
                    resource_group_name=self.resource_group_name,
                    storage_account_name=self.get_storage_account_name_from_endpoint(
                        service.properties.service_endpoint
                    ),
                    container_name=location.container,
                    folder_path=location.folder_path,
                    file_pattern_or_name=location.file_name,
                    first_row_as_header=True
                    if join_dataset.properties.first_row_as_header is None
                    else join_dataset.properties.first_row_as_header,
                    column_delimiter=ColumnDelimiter.SEMICOLON.value
                    if join_dataset.properties.column_delimiter is None
                    else join_dataset.properties.column_delimiter,
                    quote_char=QuoteCharacter.DOUBLE_QUOTE.value
                    if join_dataset.properties.quote_char is None
                    else join_dataset.properties.quote_char,
                    escape_char=EscapeCharacter.DOUBLE_QUOTE.value
                    if join_dataset.properties.escape_char is None
                    else join_dataset.properties.escape_char,
                )

        sink_dataset = self.adf_client.datasets.get(
            self.resource_group_name, self.datafactory_name, sink_dataset_name
        )
        if sink_dataset is not None:
            location = sink_dataset.properties.location
            linked_service_name = (
                sink_dataset.properties.linked_service_name.reference_name
            )
            service = self.adf_client.linked_services.get(
                self.resource_group_name, self.datafactory_name, linked_service_name
            )
            if service is not None:
                dataset_sink = Dataset(
                    resource_group_name=self.resource_group_name,
                    storage_account_name=self.get_storage_account_name_from_endpoint(
                        service.properties.service_endpoint
                    ),
                    container_name=location.container,
                    folder_path=location.folder_path,
                    file_pattern_or_name=location.file_name,
                    first_row_as_header=True
                    if sink_dataset.properties.first_row_as_header is None
                    else sink_dataset.properties.first_row_as_header,
                    column_delimiter=ColumnDelimiter.SEMICOLON.value
                    if sink_dataset.properties.column_delimiter is None
                    else sink_dataset.properties.column_delimiter,
                    quote_char=QuoteCharacter.DOUBLE_QUOTE.value
                    if sink_dataset.properties.quote_char is None
                    else sink_dataset.properties.quote_char,
                    escape_char=EscapeCharacter.DOUBLE_QUOTE.value
                    if sink_dataset.properties.escape_char is None
                    else sink_dataset.properties.escape_char,
                )

        data_flow = self.adf_client.data_flows.get(
            resource_group_name=self.resource_group_name,
            factory_name=self.datafactory_name,
            data_flow_name=dataflow_name,
        )

        if data_flow is not None:
            script = data_flow.properties.script
            column_list = self.get_column_list_from_script(script)
            # get source folder path and source file name from script
            folder, file = self.get_folder_file_from_script(script)
            dataset_source.folder_path = folder
            dataset_source.file_pattern_or_name = file
            # get sink file name from script
            file = self.get_sink_file_from_script(script)
            dataset_sink.file_pattern_or_name = file

        pipeline_response = PipelineResponse(
            source=dataset_source,
            join=dataset_join,
            columns=column_list,
            sink=dataset_sink,
            pipeline_name=pipeline_name,
            error=error,
        )
        return pipeline_response

    def create_pipeline_response(
        self,
        pipeline_request: PipelineRequest,
        pipeline_name: str,
        error_code: int,
        error_message: str,
    ) -> PipelineResponse:
        """
        Create a PipelineResponse using the input parameters
        """

        error = Error(
            code=error_code,
            message=error_message,
            source="factory_rest_api",
            date=datetime.utcnow(),
        )
        if pipeline_request is not None:
            pipeline_response = PipelineResponse(
                source=pipeline_request.source,
                join=pipeline_request.join,
                columns=pipeline_request.columns,
                sink=pipeline_request.sink,
                pipeline_name=pipeline_name,
                error=error,
            )
        else:
            dataset = Dataset(
                resource_group_name=self.resource_group_name,
                storage_account_name="",
                container_name="",
                folder_path="",
                file_pattern_or_name="",
                first_row_as_header=True,
                column_delimiter=ColumnDelimiter.SEMICOLON.value,
                quote_char=QuoteCharacter.DOUBLE_QUOTE.value,
                escape_char=EscapeCharacter.DOUBLE_QUOTE.value,
            )
            pipeline_response = PipelineResponse(
                source=dataset,
                join=dataset,
                columns=[],
                sink=dataset,
                pipeline_name=pipeline_name,
                error=error,
            )

        return pipeline_response

    def create_run_response(
        self,
        run_id: str,
        pipeline_name: str,
        status: Status,
        start: datetime,
        end: datetime,
        duration_in_ms: int,
        error_code: int,
        error_message: str,
    ) -> PipelineResponse:
        """
        Create a PipelineResponse using the input parameters
        """
        status_detail = StatusDetails(
            status=status,
            start=start,
            end=end,
            duration=duration_in_ms,
        )
        error = Error(
            code=error_code,
            message=error_message,
            source="factory_rest_api",
            date=datetime.utcnow(),
        )
        run_response = RunResponse(
            run_id=run_id,
            pipeline_name=pipeline_name,
            status=status_detail,
            error=error,
        )

        return run_response
