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
	BUILD_IMAGES=true OPERATOR_HOST=$$(minikube ip) mvn verify -Dtest=com.nextbreakpoint.flink.controller.integration.EnsureBehaviourIT

.PHONY: verify-skip-image
verify-skip-image:
	BUILD_IMAGES=false OPERATOR_HOST=$$(minikube ip) mvn verify -Dtest=com.nextbreakpoint.flink.controller.integration.EnsureBehaviourIT

.PHONY: package
package:
	mvn package -DskipTests=true

.PHONY: install
install:
	mvn install -DskipTests=true

.PHONY: package-shaded
package-shaded:
	mvn package -DskipTests=true -Pshaded

.PHONY: package-distribution
package-distribution:
	mvn package -DskipTests=true -Pdistribution

.PHONY: build-image
build-image:
	eval $$(minikube docker-env) && docker build -t nextbreakpoint/flinkctl:1.5.0 .

.PHONY: release
release:
	#mvn -Dchannel=ossrh clean deploy
	mvn clean package -DskipTests=true -Pshaded
	mvn -Dchannel=github de.jutzig:github-release-plugin:release

.PHONY: version
version:
	@echo $(shell mvn -q help:evaluate -Dexpression=project.version -DforceStdout)
