# Flink Controller

Flink Controller implements a Kubernetes Custom Controller (aka Kubernetes Operator) for Apache Flink.
That means you can operate Flink and manage applications using Kubernetes native tooling like kubectl.  

## License

Flink Controller is distributed under the terms of BSD 3-Clause License.

    Copyright (c) 2019-2024, Andrea Medeghini
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this
      list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.

    * Neither the name of the copyright holder nor the names of its
      contributors may be used to endorse or promote products derived from
      this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
    FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
    SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

## Features

This is the list of the main features implemented in Flink Controller:
- Manage deployment of clusters and applications (aka jobs) 
- Dynamically associate jobs to clusters
- Provide resilient and self-healing infrastructure 
- Deploy separate supervisor for each cluster
- Automatically restart clusters or jobs when resources are modified
- Automatically create a savepoint before stopping a job
- Automatically recover from latest savepoint when restarting a job
- Support periodic savepoints  
- Support batch and stream jobs
- Ability to rescale clusters and jobs via Kubernetes scale interface
- Support autoscaling based on custom metrics (compatible with HPA)
- Allow init containers and side containers in JobManager and TaskManager pods
- Allow configuration of user annotations   
- Allow configuration of container resources, environment variables, ports, and volumes 
- Allow configuration of security context, service account, affinity, and tolerations 
- Support pull secrets and private registries
- Support for public Flink images or custom images
- Provide CLI and REST interface to support operations
- Provide metrics compatible with Prometheus

## Overview

Flink Controller is implemented as single command line tool, called flinkctl, but the tool has 4 main functions:   
- execute the Operator process
- execute the Supervisor process
- execute the Bootstrap process
- provide a command line interface for interacting with the operator

Each function is activated by passing the relevant arguments to the flinkctl command.

Flink Controller supports the following custom resources:
- FlinkDeployment: it represents a cluster deployment, and it provides the configuration of the cluster with an optional list of jobs.
- FlinkCluster: it represents a cluster, and it stores the configuration and the status of the cluster.
- FlinkJob: it represents a job, and it stores the configuration and the status of the job.

The FlinkDeployment, FlinkCluster and FlinkJob resources are called primary resources.

The primary resources can be created and modified using Kubernetes native tooling like kubectl or helm.

Once installed, the operator will observe a given namespace and reacts to any change in the primary resources in that namespace.
Please note that it is recommended to install the operator in a separate namespace, different from the one observed. 
Also, multiple operators are required to observe multiple namespaces.

For instance, when a deployment resource is created, the operator detects the resource and creates a supervisor, 
then the supervisor creates the JobManager and TaskManagers, and bootstraps the jobs. Once the jobs are running,
the supervisor will make sure they are still running, and it will do the best effort to recover from temporary issues.  

Both operator and supervisor detect changes in the primary resources, and eventually creates,
updates or deletes one or more secondary resources, such as Pods, Services and BatchJobs, however
the operator is responsible for reconciling the status of all supervisors (one for each cluster), 
and each supervisor is responsible for reconciling the status of a single cluster and its jobs.

The status of the cluster and jobs is persisted in the primary resources, and it can be inspected with flinkctl or kubectl.

The supervisor can perform several tasks automatically, such as creating savepoints when a job is restarted,
or restarting the cluster when the specification has changed, making it easier to operate Flink on Kubernetes.  

The deployment resource is convenient for defining a cluster with multiple jobs as single resource,
however, clusters and jobs can be created independently. Jobs can be added or removed to an existing
cluster either updating a deployment resource or directly creating or deleting new job resources.

A job can be created independently of a cluster, but it can only be executed when there is a cluster for executing the job.
A job can be associated to a cluster based on the name of the resources. The rule is that the name of the FlinkJob resource 
must start with the name of the corresponding FlinkCluster resource, like clustername-jobname.

The dependencies between resources are represented in the following graph:

![Resource dependencies](/graphs/flink-operator.png "Resource dependencies")

## Get started

Download the Docker image with flinkctl command from Docker Hub:

    docker pull nextbreakpoint/flinkctl:1.5.0

Execute flinkctl as Docker container:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 --help

Check out the quickstart example:

https://github.com/nextbreakpoint/flink-controller/blob/master/example/README.md

## Installation

Flink Controller requires Kubernetes 1.31, and it supports Apache Flink 1.20.

### Generate SSL certificates

Flink Controller provides client and server components. The client component communicates to the server component over HTTP.
To ensure that the communication is secure, flinkctl can use HTTPS and SSL certificates for authentication if enabled.

Execute the script secrets.sh to generate the required keystores and truststores with self-signed certificates:

    ./secrets.sh flink-operator key-password keystore-password truststore-password

The keystores and truststores will be created in the directory secrets.

### Install

Create a namespace flink-operator for the operator:

    kubectl create namespace flink-operator

The name of the namespace can be anything you like.

Create a namespace flink-jobs for executing Flink:

    kubectl create namespace flink-jobs

The name of the namespace can be anything you like.

Create a secret which contains the keystore and truststore files (required to enable SSL):

    kubectl -n flink-operator create secret generic flink-operator-ssl \
        --from-file=keystore.jks=secrets/keystore-operator-api.jks \
        --from-file=truststore.jks=secrets/truststore-operator-api.jks \
        --from-literal=keystore-secret=keystore-password \
        --from-literal=truststore-secret=truststore-password

The name of the secret can be anything you like.

Install the CRDs (Custom Resource Definitions) with command:

    helm install flink-controller-crd helm/flink-controller-crd

Install the default roles with command:

    helm install flink-controller-roles helm/flink-controller-roles --namespace flink-operator --set targetNamespace=flink-jobs

Install the operator and enable SSL with command:

    helm install flink-controller-operator helm/flink-controller-operator --namespace flink-operator --set targetNamespace=flink-jobs --set secretName=flink-operator-ssl

Remove "--set secretName=flink-operator-ssl" if you don't want to enable SSL.

Scale the operator with command:

    kubectl -n flink-operator scale deployment flink-operator --replicas=1

Increase the number of replicas to enable HA (High Availability).

Alternatively, you can add the argument "--set replicas=1" when installing the operator with Helm.

### Uninstall 

Delete all FlinkDeployment resources:

    kubectl -n flink-jobs delete fd --all

and wait until the resources are deleted.

Delete all FlinkCluster resource:

    kubectl -n flink-jobs delete fc --all

and wait until the resources are deleted.

Delete all FlinkJob resource:

    kubectl -n flink-jobs delete fj --all

and wait until the resources are deleted.

Stop the operator with command:

    kubectl -n flink-operator scale deployment flink-operator --replicas=0

Remove the operator with command:

    helm uninstall flink-controller-operator --namespace flink-operator

Remove the default roles with command:

    helm uninstall flink-controller-roles --namespace flink-operator

Remove the CRDs with command:

    helm uninstall flink-controller-crd

Remove secret with command:

    kubectl -n flink-operator delete secret flink-operator-ssl

Remove flink-operator namespace with command:

    kubectl delete namespace flink-operator

Remove flink-jobs namespace with command:

    kubectl delete namespace flink-jobs

Please note that Kubernetes is not able to remove all resources until there are finalizers pending.
The operator and the supervisor are responsible for removing the finalizers but, in case of misconfiguration, 
they might not be able to properly remove the finalizers. If you are in such situation, you can always manually
remove the finalizers to allow Kubernetes to delete all resources.

### Upgrade

PLEASE NOTE THAT THE OPERATOR IS STILL EXPERIMENTAL, THEREFORE EACH RELEASE MIGHT INTRODUCE BREAKING CHANGES.

Before upgrading to a new release, you must cancel all jobs creating a savepoint into a durable storage location (for instance AWS S3).

Create a copy of your FlinkDeployment resources:

    kubectl -n flink-operator get fd -o yaml > deployments-backup.yaml

Create a copy of your FlinkCluster resources:

    kubectl -n flink-operator get fc -o yaml > clusters-backup.yaml

Create a copy of your FlinkJob resources:

    kubectl -n flink-operator get fj -o yaml > jobs-backup.yaml

Upgrade the default roles with command:

    helm upgrade flink-controller-roles --install helm/flink-controller-roles --namespace flink-operator --set targetNamespace=flink-jobs

Upgrade the CRDs with command:

    helm upgrade flink-controller-crd --install helm/flink-controller-crd

After installing the new CRDs, you can recreate all the custom resources. However, the old resources might not be compatible 
with the new CRDs. If that is the case, you have to fix the resource editing the yaml file and then recreate the resource. 
If you want to restore the latest savepoint of a job, copy the savepoint path from the backup into the new resource.

Finally, upgrade and restart the operator with command:

    helm upgrade flink-controller-operator --install helm/flink-controller-operator --namespace flink-operator --set targetNamespace=flink-jobs --set secretName=flink-operator-ssl --set replicas=1

## User Manual

See user manual for detailed instructions about how to use Flink Control:

https://github.com/nextbreakpoint/flink-controller/blob/master/MANUAL.md


## Contribute

Visit the project on GitHub:

https://github.com/nextbreakpoint/flink-controller

Report an issue or request a feature:

https://github.com/nextbreakpoint/flink-controller/issues
