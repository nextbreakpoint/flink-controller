#!/bin/sh

minikubeIp() {
   minikube ip
}

kubeConfig() {
   echo ~/.kube/config
}

java --module-path com.nextbreakpoint.flink.control-1.5.0-shaded.jar --module com.nextbreakpoint.flink.control/com.nextbreakpoint.flink.control.cli.Main \
  operator run --dry-run --namespace=flink --port=4444 --flink-hostname="$(minikubeIp)" --kube-config="$(kubeConfig)" \
  --keystore-path=secrets/keystore-operator-api.jks --truststore-path=secrets/truststore-operator-api.jks \
  --keystore-secret=keystore-password --truststore-secret=truststore-password

