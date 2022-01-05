import os


class ConfigurationService:
    """Class used to read and write application service configuration"""

    """ Below the existing configuration """
    """{ "name":"APP_VERSION", "value":"${APP_VERSION}"}, """
    """{ "name":"PORT_HTTP", "value":"${APP_PORT}"},"""
    """{ "name":"WEBSITES_PORT", "value":"${APP_PORT}"}, """
    """{ "name":"DATAFACTORY_ACCOUNT_NAME", "value":"${DATAFACTORY_ACCOUNT_NAME}"},"""
    """{ "name":"DATAFACTORY_RESOURCE_GROUP_NAME", "value":"${RESOURCE_GROUP}"},"""
    """{ "name":"DATAFACTORY_STORAGE_RESOURCE_GROUP_NAME", "value":"${RESOURCE_GROUP}"},"""
    """{ "name":"DATAFACTORY_STORAGE_ACCOUNT_NAME", "value":"${STORAGE_ACCOUNT_NAME}"},"""
    """{ "name":"DATAFACTORY_STORAGE_SINK_CONTAINER_NAME", "value":"${SINK_CONTAINER}"},"""
    """{ "name":"DATAFACTORY_STORAGE_SINK_FILE_NAME_FORMAT", "value":"output-00001.csv"},"""
    """{ "name":"DATAFACTORY_STORAGE_SINK_FOLDER_FORMAT", "value":"consume/{node_id}/dataset-{date}"},"""
    """{ "name":"DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME", "value":"${SOURCE_CONTAINER}"},"""
    """{ "name":"DATAFACTORY_STORAGE_SOURCE_FILE_NAME_FORMAT", "value":"input-00001.csv"},"""
    """{ "name":"DATAFACTORY_STORAGE_SOURCE_FOLDER_FORMAT", "value":"factory/{node_id}/dataset-{date}"},"""
    """{ "name":"DATAFACTORY_SOURCE_LINKED_SERVICE", "value":"source_linked_service"},"""
    """{ "name":"DATAFACTORY_SINK_LINKED_SERVICE", "value":"sink_linked_service"},"""

    def set_env_value(self, variable: str, value: str) -> str:
        if not os.environ.get(variable):
            os.environ.setdefault(variable, value)
        else:
            os.environ[variable] = value
        return value

    def get_env_value(self, variable: str, default_value: str) -> str:
        if not os.environ.get(variable):
            os.environ.setdefault(variable, default_value)
            return default_value
        else:
            return os.environ[variable]

    def get_app_version(self) -> str:
        return self.get_env_value("APP_VERSION", "1.0.0.0")

    def get_http_port(self) -> int:
        return int(self.get_env_value("PORT_HTTP", "5000"))

    def get_websites_port(self) -> int:
        return int(self.get_env_value("WEBSITES_HTTP", "5000"))

    def get_datafactory_account_name(self) -> str:
        return self.get_env_value("DATAFACTORY_ACCOUNT_NAME", "")

    def get_datafactory_resource_group_name(self) -> str:
        return self.get_env_value("DATAFACTORY_RESOURCE_GROUP_NAME", "")

    def get_datafactory_source_linked_service(self) -> str:
        return self.get_env_value("DATAFACTORY_SOURCE_LINKED_SERVICE", "")

    def get_datafactory_sink_linked_service(self) -> str:
        return self.get_env_value("DATAFACTORY_SINK_LINKED_SERVICE", "")

    def get_subscription_id(self) -> str:
        return self.get_env_value("AZURE_SUBSCRIPTION_ID", "")

    def get_tenant_id(self) -> str:
        return self.get_env_value("AZURE_TENANT_ID", "")
