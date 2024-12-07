#!/bin/sh

source .utils

java -cp $(classpath) com.nextbreakpoint.flink.cli.Main supervisor run --namespace=flink --flink-hostname=$(minikubeIp) --cluster-name=test --kube-config=$(kubeConfig) --dry-run
