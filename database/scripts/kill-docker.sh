#!/bin/bash
username="gcochran"

docker stop my-clickhouse-container
docker rm my-clickhouse-container
docker rmi ${username}/my-clickhouse-image:dev