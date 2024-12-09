#!/bin/sh

source .utils

java -cp $(classpath) com.nextbreakpoint.flink.control.cli.Main operator run --namespace=flink --port=4444 --flink-hostname=$(minikubeIp) --kube-config=$(kubeConfig)  --keystore-path=secrets/keystore-operator-api.jks --truststore-path=secrets/truststore-operator-api.jks --keystore-secret=keystore-password --truststore-secret=truststore-password --dry-run
