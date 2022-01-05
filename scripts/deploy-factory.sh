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

# Deploy infrastructure image
printMessage "Deploy infrastructure subscription: '$AZURE_SUBSCRIPTION_ID' region: '$AZURE_REGION' prefix: '$APP_NAME' sku: 'B2'"
deployAzureInfrastructure "arm-factory-template.json" $AZURE_SUBSCRIPTION_ID $AZURE_REGION $APP_NAME "B2"

# Retrieve deployment outputs
ACR_LOGIN_SERVER=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.acrLoginServer.value')
WEB_APP_SERVER=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppServer.value')
WEB_APP_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppName.value')
ACR_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.acrName.value')
WEB_APP_TENANT_ID=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppTenantId.value')
WEB_APP_OBJECT_ID=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.webAppObjectId.value')
STORAGE_ACCOUNT_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.storageAccountName.value')
OUTPUT_CONTAINER=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.outputContainerName.value')
INPUT_CONTAINER=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.inputContainerName.value')
DATAFACTORY_ACCOUNT_NAME=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.dataFactoryAccountName.value')
DATAFACTORY_INPUT_LINKED_SERVICE=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.dataFactorySourceLinkedServiceName.value')
DATAFACTORY_OUTPUT_LINKED_SERVICE=$(az deployment group show --resource-group $RESOURCE_GROUP -n $DEPLOYMENT_NAME | jq -r '.properties.outputs.dataFactorySinkLinkedServiceName.value')

printMessage "Azure Resource Group: ${RESOURCE_GROUP}"
printMessage "Azure Container Registry DNS name: ${ACR_LOGIN_SERVER}"
printMessage "Azure Web App Url: ${WEB_APP_SERVER}"
printMessage "Azure Data Factory: ${DATAFACTORY_ACCOUNT_NAME}"
printMessage "Azure Storage: ${STORAGE_ACCOUNT_NAME}"
printMessage "Azure Input Container: ${INPUT_CONTAINER}"
printMessage "Azure Output Container: ${OUTPUT_CONTAINER}"
printMessage "Azure Tenant Id: ${WEB_APP_TENANT_ID}"
printMessage "Azure App Service Identity: ${WEB_APP_OBJECT_ID}"
printMessage "Data Factory Input Linked Service: ${DATAFACTORY_INPUT_LINKED_SERVICE}"
printMessage "Data Factory Output Linked Service: ${DATAFACTORY_OUTPUT_LINKED_SERVICE}"

printProgress  "Checking role 'Contributor' for App Service ${WEB_APP_SERVER} on datafactory ${DATAFACTORY_ACCOUNT_NAME}..."
appServicePrincipalId=$(az webapp identity show --name "${WEB_APP_NAME}" --resource-group "${RESOURCE_GROUP}" --query "principalId" --output tsv)
roleAssignmentCount=$(az role assignment list --assignee ${appServicePrincipalId} --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DATAFACTORY_ACCOUNT_NAME}" | jq -r 'select(.[].roleDefinitionName=="Contributor") | length')
if [ "${roleAssignmentCount}" != "1" ];
then
    printProgress  "Assigning 'Storage Blob Data Reader' role for App Service ${WEB_APP_SERVER} on datafactory ${DATAFACTORY_ACCOUNT_NAME}..."
    printWarning  "It can sometimes take up to 30 minutes to take into account the new role assignment"
    cmd="az role assignment create --assignee-object-id \"${appServicePrincipalId}\" --assignee-principal-type ServicePrincipal --scope \"/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/${DATAFACTORY_ACCOUNT_NAME}\" --role \"Contributor\" --output none"
    printProgress "$cmd"
    eval "$cmd"
fi

printProgress  "Checking role 'Storage Account Contributor' for App Service ${WEB_APP_SERVER} on storage ${STORAGE_ACCOUNT_NAME}..."
appServicePrincipalId=$(az webapp identity show --name "${WEB_APP_NAME}" --resource-group "${RESOURCE_GROUP}" --query "principalId" --output tsv)
roleAssignmentCount=$(az role assignment list --assignee ${appServicePrincipalId} --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT_NAME}" | jq -r 'select(.[].roleDefinitionName=="Storage Account Contributor") | length')
if [ "${roleAssignmentCount}" != "1" ];
then
    printProgress  "Assigning 'Storage Account Contributor' role for App Service ${WEB_APP_SERVER} on storage ${STORAGE_ACCOUNT_NAME}..."
    printWarning  "It can sometimes take up to 30 minutes to take into account the new role assignment"
    cmd="az role assignment create --assignee-object-id \"${appServicePrincipalId}\" --assignee-principal-type ServicePrincipal --scope \"/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT_NAME}\" --role \"Storage Account Contributor\" --output none"
    printProgress "$cmd"
    eval "$cmd"
fi

printProgress  "Checking role 'Storage Blob Data Contributor' for Data Factory Account ${DATAFACTORY_ACCOUNT_NAME} on storage ${STORAGE_ACCOUNT_NAME}..."
datafactoryPrincipalId=$(az datafactory show --resource-group "${RESOURCE_GROUP}" --name "${DATAFACTORY_ACCOUNT_NAME}" --subscription "${AZURE_SUBSCRIPTION_ID}" --query "identity.principalId" --output tsv)
roleAssignmentCount=$(az role assignment list --assignee ${datafactoryPrincipalId} --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT_NAME}" | jq -r 'select(.[].roleDefinitionName=="Storage Blob Data Contributor") | length')
if [ "${roleAssignmentCount}" != "1" ];
then
    printProgress  "Assigning 'Storage Blob Data Contributor' role for Data Factory Account ${DATAFACTORY_ACCOUNT_NAME} on storage ${STORAGE_ACCOUNT_NAME}..."
    printWarning  "It can sometimes take up to 30 minutes to take into account the new role assignment"
    cmd="az role assignment create --assignee-object-id \"${datafactoryPrincipalId}\" --assignee-principal-type ServicePrincipal --scope \"/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT_NAME}\" --role \"Storage Blob Data Contributor\" --output none"
    printProgress "$cmd"
    eval "$cmd"
fi


printMessage "Deployment successfully done"