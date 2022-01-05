import os

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from src.app import app as application  # pragma: no cover # NOQA: E402

os.environ["AZURE_TENANT_ID"] = "02020202-aaaa-erty-olki-020202020202"
os.environ["AZURE_SUBSCRIPTION_ID"] = "03030303-aaaa-yuio-bbbb-030303030303"


os.environ["APP_VERSION"] = "1.0.0.0"
os.environ["PORT_HTTP"] = "5000"
os.environ["WEBSITES_HTTP"] = "5000"
os.environ["DATAFACTORY_ACCOUNT_NAME"] = "datafactory-account"
os.environ["DATAFACTORY_RESOURCE_GROUP_NAME"] = "datafactory-rg"
os.environ["DATAFACTORY_SOURCE_LINKED_SERVICE"] = "datafactory-source-ls"
os.environ["DATAFACTORY_SINK_LINKED_SERVICE"] = "datafactory-sink-ls"


@pytest.fixture
def app() -> FastAPI:
    application.dependency_overrides = {}
    return application


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app)
