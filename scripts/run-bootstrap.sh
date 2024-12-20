#!/bin/sh

minikubeIp() {
   minikube ip
}

kubeConfig() {
   echo ~/.kube/config
}

java --module-path com.nextbreakpoint.flink.control-1.5.0-shaded.jar --module com.nextbreakpoint.flink.control/com.nextbreakpoint.flink.controller.cli.Main \
  bootstrap run --namespace=flink-jobs --flink-hostname="$(minikubeIp)" --kube-config="$(kubeConfig)"  --cluster-name=cluster-1 --job-name=job-1 \
  --jar-path=com.nextbreakpoint.flink.dummies.jar --class-name=com.nextbreakpoint.flink.dummies.TemperatureMeasurementMain --parallelism=1
