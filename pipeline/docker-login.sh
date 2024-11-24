#!/usr/bin/env sh

set -e

eval "$(minikube docker-env)"

echo "$DOCKER_PASSWORD" | docker login --username="$DOCKER_USERNAME" --password-stdin
