#!/bin/bash
set -e
export IMAGE_NAME="factory_rest_api"

result=$(docker images --filter=reference="${IMAGE_NAME}:*" -q | head -n 1)
while [[ -n ${result} ]]
do
    docker rmi -f ${result}
    result=$(docker images --filter=reference="${IMAGE_NAME}:*" -q | head -n 1)
done