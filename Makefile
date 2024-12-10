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
	BUILD_IMAGES=true mvn verify -Dtest=com.nextbreakpoint.flink.control.integration.EnsureBehaviourIT

.PHONY: verify-skip-image
verify-skip-image:
	BUILD_IMAGES=false mvn verify -Dtest=com.nextbreakpoint.flink.control.integration.EnsureBehaviourIT

.PHONY: package
package:
	mvn package -DskipTests=true

.PHONY: install
install:
	mvn install -DskipTests=true

.PHONY: package-distribution
package-distribution:
	mvn package -DskipTests=true -Pdistribution

.PHONY: minikube-start
minikube-start:
	minikube start --cpus=4 --memory=8gb --kubernetes-version v1.31.0 --disk-size 20GB --driver=hyperkit

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
