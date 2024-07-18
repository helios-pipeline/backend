#!/bin/bash

. /scripts/global-vars.sh

docker buildx build --platform=linux/amd64 -t helios-flask-amd ./
docker login
docker tag helios-flask-amd ${USERNAME}/helios-flask-amd:dev
docker push ${USERNAME}/helios-flask-amd:dev


