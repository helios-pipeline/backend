#!/bin/bash

. scripts/development/global-vars.sh

sh scripts/development/kill-docker.sh

docker buildx build --platform=linux/amd64 -t helios-clickhouse-amd database/.
docker login
docker tag helios-clickhouse-amd ${USERNAME}/helios-clickhouse-amd:dev
docker push ${USERNAME}/helios-clickhouse-amd:dev


