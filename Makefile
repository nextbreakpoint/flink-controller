.PHONY: build
build:
	mvn package

.PHONY: clean
clean:
	mvn clean

.PHONY: test
test:
	mvn test

.PHONY: verify
verify:
	#eval $$(minikube docker-env) && docker build -t integration/flinkctl:1.5.0 .
	BUILD_IMAGES=true OPERATOR_HOST=$$(minikube ip) mvn verify -Dtest=com.nextbreakpoint.flink.control.integration.EnsureBehaviourIT

.PHONY: verify-skip-image
verify-skip-image:
	BUILD_IMAGES=false OPERATOR_HOST=$$(minikube ip) mvn verify -Dtest=com.nextbreakpoint.flink.control.integration.EnsureBehaviourIT

.PHONY: package
package:
	mvn package -DskipTests=true

.PHONY: install
install:
	mvn install -DskipTests=true

.PHONY: package-distribution
package-distribution:
	mvn package -DskipTests=true -Pdistribution

.PHONY: build-image
build-image:
	eval $$(minikube docker-env) && docker build -t nextbreakpoint/flinkctl:1.5.0 .

.PHONY: install-operator
install-operator:
	helm upgrade --install flink-k8s-toolbox-crd helm/flink-k8s-toolbox-crd
	helm upgrade --install flink-k8s-toolbox-roles helm/flink-k8s-toolbox-roles --namespace flink-operator --set targetNamespace=flink-jobs
	helm upgrade --install flink-k8s-toolbox-operator helm/flink-k8s-toolbox-operator --namespace flink-operator --set targetNamespace=flink-jobs --set secretName=flink-operator-ssl --set image.pullPolicy=Never

.PHONY: uninstall-operator
uninstall-operator:
	helm uninstall flink-k8s-toolbox-operator --namespace flink-operator
	helm uninstall flink-k8s-toolbox-roles --namespace flink-operator
	helm uninstall flink-k8s-toolbox-crd

.PHONY: minikube-start
minikube-start:
	minikube start --cpus=4 --memory=8gb --kubernetes-version v1.31.0 --disk-size 80GB --driver=hyperkit

.PHONY: minikube-stop
minikube-stop:
	minikube stop

.PHONY: minikube-delete
minikube-delete:
	minikube delete

.PHONY: release
release:
	mvn -Dchannel=ossrh clean deploy
	mvn -Dchannel=github de.jutzig:github-release-plugin:release

.PHONY: version
version:
	@echo $(shell mvn -q help:evaluate -Dexpression=project.version -DforceStdout)
