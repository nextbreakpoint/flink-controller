#!/bin/sh

minikubeIp() {
   minikube ip
}

kubeConfig() {
   echo ~/.kube/config
}

java --module-path com.nextbreakpoint.flink.control-1.5.0-shaded.jar --module com.nextbreakpoint.flink.control/com.nextbreakpoint.flink.control.cli.Main \
  supervisor run --dry-run --namespace=flink --flink-hostname="$(minikubeIp)" --kube-config="$(kubeConfig)" --cluster-name=cluster-1
