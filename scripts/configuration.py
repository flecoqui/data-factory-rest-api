import os
from datetime import datetime


class Configuration:
    # Static variable storing current date
    DATE = datetime.utcnow()

    def __init__(self) -> None:
        self.AZURE_SUBSCRIPTION_ID = os.environ["AZURE_SUBSCRIPTION_ID"]
        self.AZURE_TENANT_ID = os.environ["AZURE_TENANT_ID"]
        self.WEB_APP_SERVER = os.environ["WEB_APP_SERVER"]
        self.RESOURCE_GROUP = os.environ["RESOURCE_GROUP"]
        self.STORAGE_ACCOUNT_NAME = os.environ["DATAFACTORY_STORAGE_ACCOUNT_NAME"]
        self.SOURCE_CONTAINER = os.environ["DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME"]
        self.SINK_CONTAINER = os.environ["DATAFACTORY_STORAGE_SINK_CONTAINER_NAME"]
        self.SOURCE_FOLDER_FORMAT = os.environ["SOURCE_FOLDER_FORMAT"]
        self.JOIN_FOLDER_FORMAT = os.environ["JOIN_FOLDER_FORMAT"]
        self.SINK_FOLDER_FORMAT = os.environ["SINK_FOLDER_FORMAT"]

        self.SOURCE_LOCAL_RELATIVE_PATH = os.environ["SOURCE_LOCAL_RELATIVE_PATH"]
        self.SOURCE_LOCAL_PATH = f"{os.path.dirname(os.path.abspath(__file__))}/{self.SOURCE_LOCAL_RELATIVE_PATH}"
        self.SOURCE_BLOB_FILE = os.environ["SOURCE_BLOB_FILE"]
        self.SOURCE_BLOB_FOLDER = self.SOURCE_FOLDER_FORMAT.replace(
            "{date}", self.DATE.strftime("%Y-%m-%d")
        ).replace("{time}", self.DATE.strftime("%Y-%m-%d-%H-%M-%S"))

        self.JOIN_LOCAL_RELATIVE_PATH = os.environ["JOIN_LOCAL_RELATIVE_PATH"]
        self.JOIN_LOCAL_PATH = f"{os.path.dirname(os.path.abspath(__file__))}/{self.JOIN_LOCAL_RELATIVE_PATH}"
        self.JOIN_BLOB_FILE = os.environ["JOIN_BLOB_FILE"]
        self.JOIN_BLOB_FOLDER = self.JOIN_FOLDER_FORMAT.replace(
            "{date}", self.DATE.strftime("%Y-%m-%d")
        ).replace("{time}", self.DATE.strftime("%Y-%m-%d-%H-%M-%S"))

        self.SINK_LOCAL_RELATIVE_PATH = os.environ["SINK_LOCAL_RELATIVE_PATH"]
        self.SINK_LOCAL_PATH = f"{os.path.dirname(os.path.abspath(__file__))}/{self.SINK_LOCAL_RELATIVE_PATH}"
        self.SINK_BLOB_FILE = os.environ["SINK_BLOB_FILE"]
        self.SINK_BLOB_FOLDER = self.SINK_FOLDER_FORMAT.replace(
            "{date}", self.DATE.strftime("%Y-%m-%d")
        ).replace("{time}", self.DATE.strftime("%Y-%m-%d-%H-%M-%S"))

        self.SELECTED_COLUMNS = os.environ["SELECTED_COLUMNS"]

        self.SOURCE_BLOB_PATH = ""
        self.JOIN_BLOB_PATH = ""
        self.PIPELINE_NAME = ""
        self.RUN_ID = ""
