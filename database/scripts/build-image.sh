#!/bin/bash

username="gcochran"

sh ./scripts/kill-docker.sh
docker build -t my-clickhouse-image .
docker login
docker tag my-clickhouse-image ${username}/my-clickhouse-image:dev
docker push ${username}/my-clickhouse-image:dev