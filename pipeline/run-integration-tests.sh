#!/usr/bin/env sh

set -x
set -e

OPERATOR_HOST="$(minikube ip)"

export OPERATOR_HOST

mvn verify

