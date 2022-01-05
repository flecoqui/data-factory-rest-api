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

pushd "$(dirname "${BASH_SOURCE[0]}")/../src/factory_rest_api" > /dev/null
# Run all the tests
pip install -r requirements.txt --quiet
PYTHONPATH=./src python3 -m pytest ./tests --doctest-modules --cov=./src --junitxml=pytest-results.xml --cov-report term --cov-report=xml --capture=tee-sys
popd > /dev/null


printMessage "Factory unit-tests completed"