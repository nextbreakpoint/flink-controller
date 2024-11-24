#!/usr/bin/env sh

set -x
set -e

eval "$(minikube docker-env)"

docker build -t integration/flinkctl:1.5.0 .
docker build -t integration/flink:1.20.0 integration/flink --build-arg flink_version=1.20.0 --build-arg scala_version=2.12
docker build -t integration/jobs:1.20.0 integration/jobs --build-arg image=integration/flinkctl:1.5.0 --build-arg flink_version=1.20.0 --build-arg scala_version=2.12
