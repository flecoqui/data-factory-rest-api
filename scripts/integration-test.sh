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

printMessage "Integration tests with this envrionment file ${env_path}"

printMessage "Deploy Factory infrastructure"
"$SCRIPTS_DIRECTORY"/deploy-factory.sh  "${env_path}"

printMessage "Build and Deploy Factory REST API"
"$SCRIPTS_DIRECTORY"/build-container-factory.sh  "${env_path}"
"$SCRIPTS_DIRECTORY"/deploy-container-factory.sh  "${env_path}"

printMessage "Getting the input parameters to test the REST APIs"
# Retrieve parameters for the tests 
export $(cat "${env_path}" | grep  APP_NAME | sed "s/\"//g")
export $(cat "${env_path}" | grep  APP_PREFIX | sed "s/\"//g")
export $(cat "${env_path}" | grep  AZURE_SUBSCRIPTION_ID | sed "s/\"//g")
export $(cat "${env_path}" | grep  AZURE_TENANT_ID | sed "s/\"//g")
RESOURCE_GROUP="rg${APP_NAME}"
DEPLOYMENT_NAME=$(az deployment group list -g $RESOURCE_GROUP --subscription $AZURE_SUBSCRIPTION_ID  --output json | jq -r '.[0].name')
WEB_APP_SERVER=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppServer.value')
DATAFACTORY_STORAGE_ACCOUNT_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.storageAccountName.value')
DATAFACTORY_STORAGE_SINK_CONTAINER_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.storageSinkContainerName.value')
DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.storageSourceContainerName.value')
DATAFACTORY_ACCOUNT_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.dataFactoryAccountName.value')
DATAFACTORY_SOURCE_LINKED_SERVICE=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.dataFactorySourceLinkedServiceName.value')
DATAFACTORY_SINK_LINKED_SERVICE=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.dataFactorySinkLinkedServiceName.value')




printProgress  "Checking role for current user on container ${DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME} in storage ${DATAFACTORY_STORAGE_ACCOUNT_NAME}..."
currentObjectId=$(az ad signed-in-user show --query "objectId" --output tsv)
roleAssignmentCount=$(az role assignment list --assignee ${currentObjectId} --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${DATAFACTORY_STORAGE_ACCOUNT_NAME}/blobServices/default/containers/${DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME}" | jq -r 'select(.[].roleDefinitionName=="Storage Blob Data Contributor") | length')
if [ "${roleAssignmentCount}" != "1" ];
then
    printProgress  "Assigning 'Storage Blob Data Contributor' role assignment on container ${DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME} in storage ${DATAFACTORY_STORAGE_ACCOUNT_NAME}..."
    printWarning  "It can sometimes take up to 30 minutes to take into account the new role assignment"
    cmd="az role assignment create --assignee-object-id \"${currentObjectId}\" --assignee-principal-type ServicePrincipal --scope \"/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${DATAFACTORY_STORAGE_ACCOUNT_NAME}/blobServices/default/containers/${DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME}\" --role \"Storage Blob Data Contributor\" --output none"
    printProgress "$cmd"
    eval "$cmd"
    # Wait 60 seconds for the role assignment propagation
    sleep 60
fi
printProgress  "Checking role for current user on container ${DATAFACTORY_STORAGE_SINK_CONTAINER_NAME} in storage ${DATAFACTORY_STORAGE_ACCOUNT_NAME}..."
currentObjectId=$(az ad signed-in-user show --query "objectId" --output tsv)
roleAssignmentCount=$(az role assignment list --assignee ${currentObjectId} --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${DATAFACTORY_STORAGE_ACCOUNT_NAME}/blobServices/default/containers/${DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME}" | jq -r 'select(.[].roleDefinitionName=="Storage Blob Data Reader") | length')
if [ "${roleAssignmentCount}" != "1" ];
then
    printProgress  "Assigning 'Storage Blob Data Reader' role assignment on container ${DATAFACTORY_STORAGE_SINK_CONTAINER_NAME} in storage ${DATAFACTORY_STORAGE_ACCOUNT_NAME}..."
    printWarning  "It can sometimes take up to 30 minutes to take into account the new role assignment"
    cmd="az role assignment create --assignee-object-id \"${currentObjectId}\" --assignee-principal-type ServicePrincipal --scope \"/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${DATAFACTORY_STORAGE_ACCOUNT_NAME}/blobServices/default/containers/${DATAFACTORY_STORAGE_SINK_CONTAINER_NAME}\" --role \"Storage Blob Data Reader\" --output none"
    printProgress "$cmd"
    eval "$cmd"
    # Wait 60 seconds for the role assignment propagation
    sleep 60
fi


tmp_dir=$(mktemp -d -t env-XXXXXXXXXX)
cat << EOF > ${tmp_dir}/.test.env
AZURE_SUBSCRIPTION_ID=${AZURE_SUBSCRIPTION_ID}
AZURE_TENANT_ID=${AZURE_TENANT_ID}
APP_VERSION=${APP_VERSION}
WEB_APP_SERVER=${WEB_APP_SERVER}
RESOURCE_GROUP=${RESOURCE_GROUP}
DATAFACTORY_ACCOUNT_NAME=${DATAFACTORY_ACCOUNT_NAME}
DATAFACTORY_RESOURCE_GROUP_NAME=${RESOURCE_GROUP}
DATAFACTORY_STORAGE_RESOURCE_GROUP_NAME=${RESOURCE_GROUP}
DATAFACTORY_STORAGE_ACCOUNT_NAME=${DATAFACTORY_STORAGE_ACCOUNT_NAME}
DATAFACTORY_STORAGE_SINK_CONTAINER_NAME=${DATAFACTORY_STORAGE_SINK_CONTAINER_NAME}
DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME=${DATAFACTORY_STORAGE_SOURCE_CONTAINER_NAME}
DATAFACTORY_SOURCE_LINKED_SERVICE=${DATAFACTORY_SOURCE_LINKED_SERVICE}
DATAFACTORY_SINK_LINKED_SERVICE=${DATAFACTORY_SINK_LINKED_SERVICE}
SOURCE_FOLDER_FORMAT="source/{time}"
JOIN_FOLDER_FORMAT="join/{time}"
SINK_FOLDER_FORMAT="sink/{time}"
SOURCE_LOCAL_RELATIVE_PATH="data/sourcedata.csv"
JOIN_LOCAL_RELATIVE_PATH="data/joindata.csv"
SINK_LOCAL_RELATIVE_PATH="data/sinkdata.csv"
SELECTED_COLUMNS="[\"key\", \"phone\", \"email\"]"
SOURCE_BLOB_FILE="sourcedata.csv"
JOIN_BLOB_FILE="joindata.csv"
SINK_BLOB_FILE="sinkdata-00001.csv"

EOF
printMessage "Input parameters in file: '${tmp_dir}/.test.env'"

printMessage "Launch the tests of the REST APIs"
"$SCRIPTS_DIRECTORY"/launch-test-datafactory.sh  "${tmp_dir}/.test.env" 

printMessage "Undeploy Factory infrastructure"
"$SCRIPTS_DIRECTORY"/undeploy-factory.sh  "${env_path}"

printMessage "Integration tests successfully done"