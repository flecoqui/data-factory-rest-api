import os
from datetime import datetime

from fastapi import APIRouter, Body, FastAPI
from fastapi.params import Depends
from src.configuration_service import ConfigurationService
from src.factory_service import FactoryService
from src.log_service import LogService
from src.models import PipelineRequest, PipelineResponse, RunResponse
from starlette.requests import Request

router = APIRouter(prefix="")

app_version = os.getenv("APP_VERSION", "1.0.0.1")

app = FastAPI(
    title="factory REST API",
    description="Sample factory REST API.",
    version=app_version,
)


def get_log_service() -> LogService:
    """Getting a single instance of the LogService"""
    return LogService()


def get_factory_service() -> FactoryService:
    """Getting a single instance of the FactoryService"""
    return FactoryService(
        subscription_id=get_configuration_service().get_subscription_id(),
        resource_group_name=get_configuration_service().get_datafactory_resource_group_name(),
        datafactory_name=get_configuration_service().get_datafactory_account_name(),
        source_linked_service=get_configuration_service().get_datafactory_source_linked_service(),
        sink_linked_service=get_configuration_service().get_datafactory_sink_linked_service(),
    )


def get_configuration_service() -> ConfigurationService:
    """Getting a single instance of the LogService"""
    return ConfigurationService()


@router.get(
    "/version",
    responses={
        200: {"description": "Get version."},
    },
    summary="Returns the current version.",
)
def get_version(
    request: Request,
) -> str:
    """Get version using GET /version"""
    app_version = os.getenv("APP_VERSION", "1.0.0.1")
    return app_version


@router.get(
    "/time",
    responses={
        200: {"description": "Get current time."},
    },
    summary="Returns the current time.",
)
def get_time(
    request: Request,
) -> str:
    """Get utc time using GET /time"""
    now = datetime.utcnow()
    return now.strftime("%Y/%m/%d-%H:%M:%S")


@router.post(
    "/pipeline",
    responses={
        200: {
            "description": "return shareresponse  (PipelineResponse)\
 status with params: {PipelineRequest}"
        },
    },
    summary="Create pipeline with Body: {PipelineRequest}",
    response_model=PipelineResponse,
)
def pipeline(
    request: Request,
    body: PipelineRequest = Body(...),
    factory_service: FactoryService = Depends(get_factory_service),
) -> PipelineResponse:
    """Create pipeline using POST /pipeline BODY: PipelineRequest \
RESPONSE: PipelineResponse"""
    get_log_service().log_information(f"HTTP REQUEST POST /pipeline BODY: {body}")
    pipelineresponse = factory_service.pipeline(body)
    get_log_service().log_information(
        f"HTTP REQUEST POST /pipeline BODY: {body} RESPONSE: {pipelineresponse}"
    )
    return pipelineresponse


@router.get(
    "/pipeline/{pipeline_name}",
    responses={
        200: {
            "description": "return pipelineresponse  (PipelineResponse)\
 status with params: {pipeline_name}"
        },
    },
    summary="Get pipeline PipelineResponse with: {pipeline_name}",
    response_model=PipelineResponse,
)
def pipeline_status(
    request: Request,
    pipeline_name: str,
    factory_service: FactoryService = Depends(get_factory_service),
) -> PipelineResponse:
    """Get factory status using GET /pipeline/{pipeline_name} RESPONSE PipelineResponse"""
    get_log_service().log_information(
        f"HTTP REQUEST GET /pipeline PARAMS: {pipeline_name}"
    )
    pipelineresponse = factory_service.pipeline_status(
        pipeline_name=pipeline_name,
    )
    get_log_service().log_information(
        f"HTTP REQUEST GET /pipeline PARAMS: {pipeline_name}\
... RESPONSE: {pipelineresponse}"
    )
    return pipelineresponse


@router.post(
    "/pipeline/{pipeline_name}/run",
    responses={
        200: {
            "description": "return runresponse  (RunResponse)\
 status with params: {pipeline_name}"
        },
    },
    summary="Launch pipeline run with: {pipeline_name}",
    response_model=RunResponse,
)
def run(
    request: Request,
    pipeline_name: str,
    factory_service: FactoryService = Depends(get_factory_service),
) -> RunResponse:
    """Launch pipeline run using POST /pipeline BODY: RunRequest \
RESPONSE: RunResponse"""
    get_log_service().log_information(
        f"HTTP REQUEST POST /pipeline/{pipeline_name}/run parameter: {pipeline_name}"
    )
    runresponse = factory_service.run(pipeline_name)
    get_log_service().log_information(
        f"HTTP REQUEST POST /pipeline/{pipeline_name}/run parameter: {pipeline_name} RESPONSE: {runresponse}"
    )
    return runresponse


@router.get(
    "/pipeline/{pipeline_name}/run/{run_id}",
    responses={
        200: {
            "description": "return runresponse  (RunResponse)\
 status with parameters {pipeline_name} and {run_id}"
        },
    },
    summary="Get pipeline RunResponse with: {pipeline_name} and {run_id}",
    response_model=RunResponse,
)
def pipeline_run_status(
    request: Request,
    pipeline_name: str,
    run_id: str,
    factory_service: FactoryService = Depends(get_factory_service),
) -> RunResponse:
    """Get factory status using GET /pipeline/{pipeline_name}/run/{run_id} RESPONSE RunResponse"""
    get_log_service().log_information(
        f"HTTP REQUEST GET /pipeline/{pipeline_name}/run/{run_id} PARAMS: {pipeline_name} and {run_id}"
    )
    runresponse = factory_service.run_status(pipeline_name=pipeline_name, run_id=run_id)
    get_log_service().log_information(
        f"HTTP REQUEST GET /pipeline /pipeline/{pipeline_name}/run/{run_id} PARAMS: {pipeline_name} and {run_id}\
... RESPONSE: {runresponse}"
    )
    return runresponse


app.include_router(router, prefix="")
