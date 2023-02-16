#!/bin/bash

CWD="$(dirname $0)"

docker build -f DockerfileTangle -t tangled . && \
docker build -f DockerfileTester -t tester . && \
docker-compose -p tangled-tests -f ${CWD}/docker-compose.yml run tester poetry run pytest $*

docker-compose -p tangled-tests -f ${CWD}/docker-compose.yml rm -fsv
