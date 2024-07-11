#!/bin/bash

username="gcochran"

sh database/scripts/kill-docker.sh

docker build -t my-clickhouse-image database/.
docker login
docker tag my-clickhouse-image ${username}/my-clickhouse-image:dev
docker push ${username}/my-clickhouse-image:dev
