#!/usr/bin/env sh

set -x
set -e

OPERATOR_HOST="$(minikube ip)"

export OPERATOR_HOST

mvn -t toolchains-pipeline.xml verify -Dtest=com.nextbreakpoint.flink.controller.integration.EnsureBehaviourIT

