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
# force APP_VERSION value
APP_VERSION=$(date +"%y%m%d.%H%M%S")

# Retrieve deployment outputs
ACR_LOGIN_SERVER=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.acrLoginServer.value')
WEB_APP_SERVER=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppServer.value')
ACR_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.acrName.value')
WEB_APP_TENANT_ID=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppTenantId.value')
WEB_APP_OBJECT_ID=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppObjectId.value')

printMessage "Azure Container Registry DNS name: ${ACR_LOGIN_SERVER}"
printMessage "Azure Web App Url: ${WEB_APP_SERVER}"


# Build factory-api docker image
printMessage "Building factory_rest_api container version:${APP_VERSION} port: ${APP_PORT}"
buildWebAppContainer "${ACR_LOGIN_SERVER}" "./src/factory_rest_api" "factory_rest_api" "${APP_VERSION}" "latest" ${APP_PORT}
checkError

# update env file
tmp_dir=$(mktemp -d -t env-XXXXXXXXXX)
cat ${env_path} | grep -v APP_VERSION > ${tmp_dir}/.env
echo "APP_VERSION=${APP_VERSION}" >> ${tmp_dir}/.env
cp ${tmp_dir}/.env ${env_path}

printMessage "Build successfully done"