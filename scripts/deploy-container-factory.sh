#!/bin/bash
#
# executable
#

set -e
# Read variables in configuration file
parent_path=$(
    cd "$(dirname "${BASH_SOURCE[0]}")/../"
    pwd -P
)
SCRIPTS_DIRECTORY=`dirname $0`
source "$SCRIPTS_DIRECTORY"/common.sh

env_path=$1
if [[ -z $env_path ]]; then
    env_path="$(dirname "${BASH_SOURCE[0]}")/../configuration/.default.env"
fi

if [[ $env_path ]]; then
    if [ ! -f "$env_path" ]; then
        printError "$env_path does not exist."
        exit 1
    fi
    set -o allexport
    source "$env_path"
    set +o allexport
else
    printWarning "No env. file specified. Using environment variables."
fi


# Check Variables
checkVariables
checkError

# Check Azure connection
printMessage "Check Azure connection for subscription: '$AZURE_SUBSCRIPTION_ID'"
azLogin
checkError

RESOURCE_GROUP="rg${APP_NAME}"
DEPLOYMENT_NAME=$(az deployment group list -g $RESOURCE_GROUP --output json | jq -r '.[0].name')

# Retrieve deployment outputs
REGISTRY_URL=$2
ACR_LOGIN_SERVER=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.acrLoginServer.value')
WEB_APP_SERVER=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppServer.value')
ACR_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.acrName.value')
WEB_APP_TENANT_ID=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppTenantId.value')
WEB_APP_OBJECT_ID=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppObjectId.value')
DATAFACTORY_STORAGE_ACCOUNT_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.storageAccountName.value')
DATAFACTORY_STORAGE_SINK_CONTAINER_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.storageSinkContainerName.value')
DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.storageSourceContainerName.value')
DATAFACTORY_ACCOUNT_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.dataFactoryAccountName.value')
DATAFACTORY_SOURCE_LINKED_SERVICE=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.dataFactorySourceLinkedServiceName.value')
DATAFACTORY_SINK_LINKED_SERVICE=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.dataFactorySinkLinkedServiceName.value')

printMessage "Registry url: ${REGISTRY_URL}"
printMessage "Azure Resource Group: ${RESOURCE_GROUP}"
printMessage "Azure Container Registry DNS name: ${ACR_LOGIN_SERVER}"
printMessage "Azure Web App Url: ${WEB_APP_SERVER}"
printMessage "Azure Data Factory: ${DATAFACTORY_ACCOUNT_NAME}"
printMessage "Azure Storage: ${DATAFACTORY_STORAGE_ACCOUNT_NAME}"
printMessage "Azure Source Container: ${DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME}"
printMessage "Azure Sink Container: ${DATAFACTORY_STORAGE_SINK_CONTAINER_NAME}"
printMessage "Data Factory Source Linked Service: ${DATAFACTORY_SOURCE_LINKED_SERVICE}"
printMessage "Data Factory Sink Linked Service: ${DATAFACTORY_SINK_LINKED_SERVICE}"
printMessage "Azure Tenant Id: ${WEB_APP_TENANT_ID}"
printMessage "Azure App Service Identity: ${WEB_APP_OBJECT_ID}"

# Deploy factory_rest_api
printMessage "Deploy containers from Azure Container Registry ${ACR_LOGIN_SERVER}"
tmp_dir=$(mktemp -d -t env-XXXXXXXXXX)
cat << EOF > ${tmp_dir}/factory-settings.conf
[
{ "name":"AZURE_SUBSCRIPTION_ID", "value":"${AZURE_SUBSCRIPTION_ID}"}, 
{ "name":"AZURE_TENANT_ID", "value":"${AZURE_TENANT_ID}"}, 
{ "name":"APP_VERSION", "value":"${APP_VERSION}"}, 
{ "name":"PORT_HTTP", "value":"${APP_PORT}"},
{ "name":"WEBSITES_PORT", "value":"${APP_PORT}"}, 
{ "name":"NODE_IDENTITY", "value":"${WEB_APP_OBJECT_ID}"},
{ "name":"DATAFACTORY_ACCOUNT_NAME", "value":"${DATAFACTORY_ACCOUNT_NAME}"},
{ "name":"DATAFACTORY_RESOURCE_GROUP_NAME", "value":"${RESOURCE_GROUP}"},
{ "name":"DATAFACTORY_STORAGE_RESOURCE_GROUP_NAME", "value":"${RESOURCE_GROUP}"},
{ "name":"DATAFACTORY_STORAGE_ACCOUNT_NAME", "value":"${STORAGE_ACCOUNT_NAME}"},
{ "name":"DATAFACTORY_STORAGE_SINK_CONTAINER_NAME", "value":"${DATAFACTORY_STORAGE_SINK_CONTAINER_NAME}"},
{ "name":"DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME", "value":"${DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME}"},
{ "name":"DATAFACTORY_SOURCE_LINKED_SERVICE", "value":"${DATAFACTORY_SOURCE_LINKED_SERVICE}"},
{ "name":"DATAFACTORY_SINK_LINKED_SERVICE", "value":"${DATAFACTORY_SINK_LINKED_SERVICE}"},

{ "name":"DATAFACTORY_STORAGE_SHARE_FILE_NAME_FORMAT", "value":"input-00001.csv"},
{ "name":"DATAFACTORY_STORAGE_SHARE_FOLDER_FORMAT", "value":"factory/{node_id}/dataset-{date}"},
{ "name":"DATAFACTORY_STORAGE_CONSUME_FILE_NAME_FORMAT", "value":"output-00001.csv"},
{ "name":"DATAFACTORY_STORAGE_CONSUME_FOLDER_FORMAT", "value":"consume/{node_id}/{invitation_id}/dataset-{date}"},

]
EOF
deployWebAppContainerConfigFromFile "${AZURE_SUBSCRIPTION_ID}" "${APP_NAME}" "${ACR_LOGIN_SERVER}" "${ACR_NAME}"  "factory_rest_api" "latest" "${tmp_dir}/factory-settings.conf"
checkError

# Test factory_rest_api
share_rest_api_url="https://${WEB_APP_SERVER}/version"
datetime=$(date +"%y/%m/%d-%H:%M:%S")
printMessage "Testing factory_rest_api url: $share_rest_api_url at $datetime expected version: ${APP_VERSION}"
result=$(checkUrl "${share_rest_api_url}" "${APP_VERSION}" 420)
if [[ "${result}" != "true" ]]; then
    printError "Error while testing factory_rest_api"
else
    printMessage "Testing factory_rest_api successful"
fi

printMessage "Deployment successfully done"