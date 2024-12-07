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
	mvn verify

.PHONY: package
package:
	mvn package -DskipTests=true

.PHONY: install
install:
	mvn install -DskipTests=true

.PHONY: minikube-start
minikube-start:
	minikube start --cpus=4 --memory=8gb --kubernetes-version v1.31.0 --driver=docker

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
