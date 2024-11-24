#!/usr/bin/env sh

set -x
set -e

OPERATOR_HOST="$(minikube ip)"

export OPERATOR_HOST

./gradlew integrationTest --info --tests="com.nextbreakpoint.flink.integration.${1}Test"

