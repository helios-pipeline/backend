#!/bin/bash
. scripts/development/global-vars.sh

docker stop helios-clickhouse-container
docker rm helios-clickhouse-container
docker rmi ${USERNAME}/helios-clickhouse-arm:dev || true
docker rmi ${USERNAME}/helios-clickhouse-amd:dev || true