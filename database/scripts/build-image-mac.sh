#!/bin/bash

source database/scripts/global-vars.sh

sh database/scripts/kill-docker.sh

docker buildx build --platform=linux/arm64 -t helios-clickhouse-arm database/.
docker login
docker tag helios-clickhouse-arm ${USERNAME}/helios-clickhouse-arm:dev
docker push ${USERNAME}/helios-clickhouse-arm:dev