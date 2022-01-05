#!/bin/bash
#
# executable
#

##############################################################################
# colors for formatting the ouput
##############################################################################
# shellcheck disable=SC2034
{
YELLOW='\033[1;33m'
GREEN='\033[1;32m'
RED='\033[0;31m'
BLUE='\033[1;34m'
NC='\033[0m' # No Color
}
##############################################################################
#- function used to check whether an error occured
##############################################################################
function checkError() {
    # shellcheck disable=SC2181
    if [ $? -ne 0 ]; then
        echo -e "${RED}\nAn error occured exiting from the current bash${NC}"
        exit 1
    fi
}

##############################################################################
#- print functions
##############################################################################
function printMessage(){
    echo -e "${GREEN}$1${NC}" 
}
function printWarning(){
    echo -e "${YELLOW}$1${NC}" 
}
function printError(){
    echo -e "${RED}$1${NC}" 
}
function printProgress(){
    echo -e "${BLUE}$1${NC}" 
}
##############################################################################
#- azure Login 
##############################################################################
function azLogin() {
    # Check if current process's user is logged on Azure
    # If no, then triggers az login
    azOk=true
    az account set -s "$AZURE_SUBSCRIPTION_ID" 2>/dev/null || azOk=false
    if [[ ${azOk} == false ]]; then
        echo -e "need to az login"
        az login --tenant "$AZURE_TENANT_ID"
    fi

    azOk=true
    az account set -s "$AZURE_SUBSCRIPTION_ID"   || azOk=false
    if [[ ${azOk} == false ]]; then
        echo -e "unknown error"
        exit 1
    fi
}
##############################################################################
#- check is Url is ready returning 200 and the expected response
##############################################################################
function checkUrl() {
    httpCode="404"
    apiUrl="$1"
    expectedResponse="$2"
    timeOut="$3"
    response=""
    count=0
    while [[ "$httpCode" != "200" ]] || [[ "$response" != "$expectedResponse" ]] && [[ $count -lt ${timeOut} ]]
    do
        SECONDS=0
        httpCode=$(curl -s -o /dev/null -L -w '%{http_code}' "$apiUrl") || true
        if [[ $httpCode == "200" ]]; then
            response=$(curl -s  "$apiUrl") || true
            response=${response//\"/}
        fi
        #echo "count=${count} code: ${httpCode} response: ${response} "
        sleep 10
        ((count=count+SECONDS))
    done
    if [ $httpCode == "200" ] && [ "${response}" == "${expectedResponse}" ]; then
        echo "true"
        return
    fi
    echo "false"
    return
}

##############################################################################
#- get localhost
##############################################################################
function get_local_host() {
CONTAINER_NAME="$1"
DEV_CONTAINER_ROOT="/dcworkspace"
DEV_CONTAINER_NETWORK=$(docker inspect $(hostname) | jq -r '.[0].HostConfig.NetworkMode')
FULL_PATH=$(cd $(dirname ""); pwd -P /$(basename ""))
    if [[ $FULL_PATH =~ ^$DEV_CONTAINER_ROOT.* ]] && [[ -n $DEV_CONTAINER_NETWORK ]]; then
        # running in dev container
        # connect devcontainer network to container
        if [[ $(docker container inspect "${CONTAINER_NAME}" | jq -r ".[].NetworkSettings.Networks.\"$DEV_CONTAINER_NETWORK\"") == null ]]; then 
            docker network connect ${DEV_CONTAINER_NETWORK} ${CONTAINER_NAME} 
        fi
        CONTAINER_IP=$(docker container inspect "${CONTAINER_NAME}" | jq -r ".[].NetworkSettings.Networks.\"$DEV_CONTAINER_NETWORK\".IPAddress")
        echo "$CONTAINER_IP"
    else
        echo "127.0.0.1"
    fi
}

##############################################################################
#- checkVariables: check if the variables are set
##############################################################################
function checkVariables() {
    if [[ -z ${AZURE_SUBSCRIPTION_ID} ]]; then
        printError "'AZURE_SUBSCRIPTION_ID' environment variable not set"
        exit 1
    fi
    if [[ -z ${AZURE_TENANT_ID} ]]; then
        printError "'AZURE_TENANT_ID' environment variable not set"
        exit 1
    fi
    if [[ -z ${AZURE_REGION} ]]; then
        export AZURE_REGION="eastus2"
    fi
    if [[ -z ${APP_PREFIX} ]]; then
        printError "'APP_PREFIX' environment variable not set"
        exit 1
    fi
    if [[ -z ${APP_NAME} ]]; then
        # Set APP_NAME to include a 4 char random string 
        export APP_NAME="${APP_PREFIX}$(echo $RANDOM | md5sum | head -c 4)"
    fi
    if [[ -z ${APP_VERSION} ]]; then
        export APP_VERSION=$(date +"%y%m%d.%H%M%S")
    fi
    if [[ -z ${APP_PORT} ]]; then
        export APP_PORT=5000
    fi
}
##############################################################################
#- deployAzureInfrastructure: deploy infrastructure with ARM Template
#  arg 1: ARM template file name
#  arg 2: Subscription
#  arg 3: Azure region for the deployment
#  arg 4: prefix used for naming Azure resources
#  arg 5: web app sku 
##############################################################################
function deployAzureInfrastructure(){
    TEMPLATE=$1
    SUBSCRIPTION=$2
    REGION=$3
    PREFIX=$4
    SKU=$5
    DEPLOYMENT_NAME=$(date +"%y%M%d-%H%M%S")
    RESOURCE_GROUP="rg${PREFIX}"

    cmd="az group create  --subscription $SUBSCRIPTION --location $REGION --name $RESOURCE_GROUP --output none "
    printProgress "$cmd"
    eval "$cmd"

    checkError
    cmd="az deployment group create \
        --name $DEPLOYMENT_NAME \
        --resource-group $RESOURCE_GROUP \
        --subscription $SUBSCRIPTION \
        --template-file $SCRIPTS_DIRECTORY/$TEMPLATE \
        --output none \
        --parameters \
        name=$PREFIX sku=$SKU location=$REGION"
    printProgress "$cmd"
    eval "$cmd"
    checkError
}

##############################################################################
#- undeployAzureInfrastructure: delete resource group
#  arg 1: subscription
#  arg 2: prefix used for naming Azure resources
##############################################################################
function undeployAzureInfrastructure(){
    subscription=$1
    prefix=$2
    resourcegroup="rg${prefix}"

    cmd="az group delete  --subscription $subscription  --name $resourcegroup -y --output none "
    printProgress "$cmd"
    eval "$cmd"
}
##############################################################################
#- buildWebAppContainer: build webapp container
#  arg 1: Azure Container Registry Name
#  arg 2: API name: for instance factory_rest_api, registry-rest-api
#  arg 3: Image name: for instance factory_rest_api, registry-rest-api
#  arg 4: Image Tag
#  arg 5: Image Latest Tag: for instance latest
#  arg 6: HTTP Port
##############################################################################
function buildWebAppContainer() {
    ContainerRegistryName="$1"
    apiModule="$2"
    imageName="$3"
    imageTag="$4"
    imageLatestTag="$5"
    portHttp="$6"

    targetDirectory="$(dirname "${BASH_SOURCE[0]}")/../$apiModule"

    if [ ! -d "$targetDirectory" ]; then
            echo "Directory '$targetDirectory' does not exist."
            exit 1
    fi

    echo "Building and uploading the docker image for '$apiModule'"

    # Navigate to API module folder
    pushd "$targetDirectory" > /dev/null

    # Build the image
    echo "Building the docker image for '$imageName:$imageTag'"
    cmd="az acr build --registry $ContainerRegistryName --image ${imageName}:${imageTag} --image ${imageName}:${imageLatestTag} -f Dockerfile --build-arg APP_VERSION=${imageTag} --build-arg ARG_PORT_HTTP=${portHttp} . --output none"
    printProgress "$cmd"
    eval "$cmd"

    
    popd > /dev/null

}

##############################################################################
#- deployWebAppContainer: build webapp container
#  arg 1: Azure Susbcription
#  arg 2: prefix used for naming Azure resources
#  arg 3: Azure Container Registry Url
#  arg 4: Azure Container Registry Name
#  arg 5: Image name: for instance factory_rest_api, registry-rest-api
#  arg 6: Image Tag
#  arg 7: Application Version: usually = Image Tag
#  arg 8: HTTP Port
##############################################################################
function deployWebAppContainer(){
    SUBSCRIPTION_ID="$1"
    prefix="$2"
    ContainerRegistryUrl="$3"
    ContainerRegistryName="$4"
    imageName="$5"
    imageTag="$6"
    appVersion="$7"
    portHTTP="$8"

    resourcegroup="rg${prefix}"
    webapp="webapp${prefix}"

    # When deployed, WebApps get automatically a managed identity. Ensuring this MSI has AcrPull rights
    printProgress  "Ensure ${webapp} has AcrPull role assignment on ${ContainerRegistryName}..."
    WebAppMsiPrincipalId=$(az webapp show -n "$webapp" -g "$resourcegroup" -o json | jq -r .identity.principalId)
    WebAppMsiAcrPullAssignmentCount=$(az role assignment list --assignee "$WebAppMsiPrincipalId" --scope /subscriptions/"${SUBSCRIPTION_ID}"/resourceGroups/"${resourcegroup}"/providers/Microsoft.ContainerRegistry/registries/"${ContainerRegistryName}" | jq -r 'select(.[].roleDefinitionName=="AcrPull") | length')

    if [ "$WebAppMsiAcrPullAssignmentCount" != "1" ];
    then
        printProgress  "Assigning AcrPull role assignment on scope ${ContainerRegistryName}..."
        az role assignment create --assignee-object-id "$WebAppMsiPrincipalId" --assignee-principal-type ServicePrincipal --scope /subscriptions/"${SUBSCRIPTION_ID}"/resourceGroups/"${resourcegroup}"/providers/Microsoft.ContainerRegistry/registries/"${ContainerRegistryName}" --role "AcrPull"
    fi

    printProgress  "Check if WebApp ${webapp} use Managed Identity for the access to ACR ${ContainerRegistryName}..."
    WebAppAcrConfigAcrEnabled=$(az resource show --ids /subscriptions/"${SUBSCRIPTION_ID}"/resourceGroups/"${resourcegroup}"/providers/Microsoft.Web/sites/"${webapp}"/config/web | jq -r ".properties.acrUseManagedIdentityCreds")
    if [ "$WebAppAcrConfigAcrEnabled" = false ];
    then
        printProgress "Enabling Acr on ${webapp}..."
        az resource update --ids /subscriptions/"${SUBSCRIPTION_ID}"/resourceGroups/"${resourcegroup}"/providers/Microsoft.Web/sites/"${webapp}"/config/web --set properties.acrUseManagedIdentityCreds=True
    fi


    printProgress "Create Containers"
    FX_Version="Docker|$ContainerRegistryUrl/$imageName:$imageTag"

    #Configure the ACR, Image and Tag to pull
    cmd="az resource update --ids /subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${resourcegroup}/providers/Microsoft.Web/sites/${webapp}/config/web --set properties.linuxFxVersion=\"$FX_Version\" -o none --force-string"
    printProgress "$cmd"
    eval "$cmd"

    printProgress "Create Config"
    cmd="az webapp config appsettings set -g "$resourcegroup" -n "$webapp" \
    --settings APP_VERSION=${appVersion} PORT_HTTP=${portHTTP} WEBSITES_PORT=${portHTTP} --output none"
    printProgress "$cmd"
    eval "$cmd"

    printProgress "Restart Web App "
    cmd="az webapp restart --name $webapp --resource-group $resourcegroup"
    printProgress "$cmd"
    eval "$cmd"
}

##############################################################################
#- deployWebAppContainer: build webapp container
#  arg 1: Azure Susbcription
#  arg 2: prefix used for naming Azure resources
#  arg 3: Azure Container Registry Url
#  arg 4: Azure Container Registry Name
#  arg 5: Image name: for instance factory_rest_api, registry-rest-api
#  arg 6: Image Tag
#  arg 7: Configuration file path
##############################################################################
function deployWebAppContainerConfigFromFile(){
    SUBSCRIPTION_ID="$1"
    prefix="$2"
    ContainerRegistryUrl="$3"
    ContainerRegistryName="$4"
    imageName="$5"
    imageTag="$6"
    configFile="$7"

    resourcegroup="rg${prefix}"
    webapp="webapp${prefix}"

    # When deployed, WebApps get automatically a managed identity. Ensuring this MSI has AcrPull rights
    printProgress  "Ensure ${webapp} has AcrPull role assignment on ${ContainerRegistryName}..."
    WebAppMsiPrincipalId=$(az webapp show -n "$webapp" -g "$resourcegroup" -o json | jq -r .identity.principalId)
    WebAppMsiAcrPullAssignmentCount=$(az role assignment list --assignee "$WebAppMsiPrincipalId" --scope /subscriptions/"${SUBSCRIPTION_ID}"/resourceGroups/"${resourcegroup}"/providers/Microsoft.ContainerRegistry/registries/"${ContainerRegistryName}" | jq -r 'select(.[].roleDefinitionName=="AcrPull") | length')

    if [ "$WebAppMsiAcrPullAssignmentCount" != "1" ];
    then
        printProgress  "Assigning AcrPull role assignment on scope ${ContainerRegistryName}..."
        az role assignment create --assignee-object-id "$WebAppMsiPrincipalId" --assignee-principal-type ServicePrincipal --scope /subscriptions/"${SUBSCRIPTION_ID}"/resourceGroups/"${resourcegroup}"/providers/Microsoft.ContainerRegistry/registries/"${ContainerRegistryName}" --role "AcrPull"
    fi

    printProgress  "Check if WebApp ${webapp} use Managed Identity for the access to ACR ${ContainerRegistryName}..."
    WebAppAcrConfigAcrEnabled=$(az resource show --ids /subscriptions/"${SUBSCRIPTION_ID}"/resourceGroups/"${resourcegroup}"/providers/Microsoft.Web/sites/"${webapp}"/config/web | jq -r ".properties.acrUseManagedIdentityCreds")
    if [ "$WebAppAcrConfigAcrEnabled" = false ];
    then
        printProgress "Enabling Acr on ${webapp}..."
        az resource update --ids /subscriptions/"${SUBSCRIPTION_ID}"/resourceGroups/"${resourcegroup}"/providers/Microsoft.Web/sites/"${webapp}"/config/web --set properties.acrUseManagedIdentityCreds=True
    fi


    printProgress "Create Containers"
    FX_Version="Docker|$ContainerRegistryUrl/$imageName:$imageTag"

    #Configure the ACR, Image and Tag to pull
    cmd="az resource update --ids /subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${resourcegroup}/providers/Microsoft.Web/sites/${webapp}/config/web --set properties.linuxFxVersion=\"$FX_Version\" -o none --force-string"
    printProgress "$cmd"
    eval "$cmd"

    printProgress "Create Config"
    cmd="az webapp config appsettings set -g "$resourcegroup" -n "$webapp" \
    --settings @${configFile} --output none"
    printProgress "$cmd"
    eval "$cmd"

    printProgress "Restart Web App "
    cmd="az webapp restart --name $webapp --resource-group $resourcegroup"
    printProgress "$cmd"
    eval "$cmd"
}
