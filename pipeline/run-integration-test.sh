#!/usr/bin/env sh

set -x
set -e

OPERATOR_HOST="$(minikube ip)"

export OPERATOR_HOST

mvn verify -Dtest=com.nextbreakpoint.flink.control.integration.EnsureBehaviourIT

