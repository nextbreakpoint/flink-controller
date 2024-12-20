# Example of Flink deployment on Minikube   

Follow the instructions below to install Flink Controller on Minikube and deploy a Flink application.

## Prepare environment

Install Minikube 1.34. Install kubectl, Helm 3, and AWS CLI.

Start Minikube with at least 8Gb of memory and Kubernetes 1.31:

    minikube start --cpus=4 --memory=8gb --kubernetes-version v1.31.0 --disk-size 80GB --driver=<your-driver>

## Install the operator    

Create flink-operator namespace:

    kubectl create namespace flink-operator

Create flink-jobs namespace:

    kubectl create namespace flink-jobs

Install operator resources:

    helm install flink-controller-crd helm/flink-controller-crd
    helm install flink-controller-roles helm/flink-controller-roles --namespace flink-operator --set targetNamespace=flink-jobs
    helm install flink-controller-operator helm/flink-controller-operator --namespace flink-operator --set targetNamespace=flink-jobs

Start the operator:

    kubectl scale deployment -n flink-operator flink-operator --replicas=1

Tail the logs of the operator:

    kubectl -n flink-operator logs -f --tail=-1 -l app=flink-operator

## Prepare Docker image

Configure Docker environment:

    eval $(minikube docker-env)

Build Flink jobs image:

    docker build -t example/docker:latest example/jobs

## Install Minio

Minio provides a S3 compatible API which can be used instead of the actual Amazon S3 API. Minio will be used as distributed persistent storage.

Create minio namespace:

    kubectl create namespace minio

Install Minio with Helm:

    helm install minio example/helm/minio --namespace minio --set persistence.enabled=true,persistence.size=10G,minio.accessKey=minioaccesskey,minio.secretKey=miniosecretkey

Expose Minio port to host:

    kubectl -n minio expose service minio --name=minio-external --type=LoadBalancer --external-ip=$(minikube ip) --port=9000 --target-port=9000
    kubectl -n minio expose service-console minio --name=minio-console-external --type=LoadBalancer --external-ip=$(minikube ip) --port=9001 --target-port=9001

Create the bucket with AWS CLI:

    export AWS_ACCESS_KEY_ID=minioaccesskey  
    export AWS_SECRET_ACCESS_KEY=miniosecretkey
    aws s3 mb s3://nextbreakpoint-example --endpoint-url http://$(minikube ip):9000

or create the bucket with Minio client:

    kubectl -n minio run minio-client --image=minio/mc:latest --restart=Never --command=true -- sh -c "mc config host add minio http://minio-headless:9000 minioaccesskey miniosecretkey && mc mb --region=eu-west-1 minio/nextbreakpoint-example"

## Install Flink deployment

Install deployment resource to create cluster and jobs:

    helm upgrade --install example example/helm/example -n flink-jobs --set s3Endpoint=http://minio-headless.minio:9000,s3AccessKey=minioaccesskey,s3SecretKey=miniosecretkey,s3BucketName=nextbreakpoint-example,jobs.pullPolicy=Never

List deployment resources:

    kubectl -n flink-jobs get fd

Get deployment resource:

    kubectl -n flink-jobs get fd example -o json | jq

Watch cluster resources:

    kubectl -n flink-jobs get fc --watch

Watch job resources:

    kubectl -n flink-jobs get fj --watch

Watch pod resources:

    kubectl -n flink-jobs get pods --watch

Tail the logs of the supervisor:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=supervisor

Tail the logs of the JobManager:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=jobmanager -c jobmanager

Tail the logs of the TaskManager:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=taskmanager -c taskmanager

Expose the JobManager web console:

    kubectl -n flink-jobs port-forward service/jobmanager-example 8081

then open a browser at http://localhost:8081

Check the content of the bucket:

    aws --endpoint-url http://$(minikube ip):9000 s3 ls s3://nextbreakpoint-example/output/

the jobs should produce output periodically.

## Patch a deployment resource to update cluster and jobs

Patch pullPolicy:

    kubectl -n flink-jobs patch fd example --type=json -p '[{"op":"replace","path":"/spec/cluster/runtime/pullPolicy","value":"Always"}]'

Patch serviceMode:

    kubectl -n flink-jobs patch fd example --type=json -p '[{"op":"replace","path":"/spec/cluster/jobManager/serviceMode","value":"NodePort"}]'

Patch savepointInterval:

    kubectl -n flink-jobs patch fd example --type=json -p '[{"op":"replace","path":"/spec/jobs/0/spec/savepoint/savepointInterval","value":60}]'

Patch supervisor replicas:

    kubectl -n flink-jobs patch fd example --type=json -p '[{"op":"replace","path":"/spec/cluster/supervisor/replicas","value":2}]'

## Inspect cluster and jobs

Inspect status of the deployment:

    kubectl -n flink-jobs get fd example -o json | jq '.status'

Inspect status of the cluster:

    kubectl -n flink-jobs get fc example -o json | jq '.status'

Inspect status of a job:

    kubectl -n flink-jobs get fj example-measurement -o json | jq '.status'

## Scale jobs with kubectl

Ensure that rescale policy is JobParallelism:

    kubectl -n flink-jobs patch fd example --type=json -p '[{"op":"replace","path":"/spec/cluster/supervisor/rescalePolicy","value":"JobParallelism"}]'

Scale the job parallelism and the cluster will rescale automatically:

    kubectl -n flink-jobs scale fj example-measurement --replicas=2

## Scale cluster with kubectl

Ensure that rescale policy is None:

    kubectl -n flink-jobs patch fd example --type=json -p '[{"op":"replace","path":"/spec/cluster/supervisor/rescalePolicy","value":"None"}]'

Wait until the supervisor restarts, then scale the cluster:

    kubectl -n flink-jobs scale fc example --replicas=4

## Control cluster and jobs with annotations

Annotate the cluster to stop it:

    kubectl -n flink-jobs annotate fc example --overwrite operator.nextbreakpoint.com/requested-action=STOP

Annotate the cluster to start it:

    kubectl -n flink-jobs annotate fc example --overwrite operator.nextbreakpoint.com/requested-action=START

Annotate a job to stop it:

    kubectl -n flink-jobs annotate fj example-measurement --overwrite operator.nextbreakpoint.com/requested-action=STOP

Annotate a job to start it:

    kubectl -n flink-jobs annotate fj example-measurement --overwrite operator.nextbreakpoint.com/requested-action=START

##  Control cluster and jobs with curl

Expose operator control interface:

    kubectl -n flink-operator port-forward service/flink-operator 4444

Get status of the deployment:

    curl http://localhost:4444/api/v1/deployments/example/status | jq -r '.output' | jq

Get status of the cluster:

    curl http://localhost:4444/api/v1/clusters/example/status | jq -r '.output' | jq

Get status of a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/status | jq -r '.output' | jq

Stop the cluster:

    curl http://localhost:4444/api/v1/clusters/example/stop -XPUT -d'{"withoutSavepoint":false}'

Start the cluster:

    curl http://localhost:4444/api/v1/clusters/example/start -XPUT -d'{"withoutSavepoint":false}'

Stop a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/stop -XPUT -d'{"withoutSavepoint":false}'

Start a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/start -XPUT -d'{"withoutSavepoint":false}'

## Control cluster and jobs with flinkctl

Expose the operator using an external address:

    kubectl -n flink-operator expose service flink-operator --name=flink-operator-external --port=4444 --target-port=4444 --external-ip=$(minikube ip)

Configure Docker environment:

    eval $(minikube docker-env)

Get status of the deployment:

     docker run --rm -it nextbreakpoint/flinkctl:1.5.0 deployment status --host=$(minikube ip) --deployment-name=example | jq -r '.output' | jq

Get status of the cluster:

     docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster status --host=$(minikube ip) --cluster-name=example | jq -r '.output' | jq

Get status of a job:

     docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job status --host=$(minikube ip) --cluster-name=example --job-name=measurement | jq -r '.output' | jq

Stop the cluster:

     docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster stop --host=$(minikube ip) --cluster-name=example

Start the cluster:

     docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster start --host=$(minikube ip) --cluster-name=example

Stop a job:

     docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job stop --host=$(minikube ip) --cluster-name=example --job-name=measurement

Start a job:

     docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job start --host=$(minikube ip) --cluster-name=example --job-name=measurement

## Remove all resources

Delete the Flink deployment:

    helm uninstall example -n flink-jobs

Wait until the operator has removed everything, then remove the operator.

Stop the operator:

    kubectl scale deployment -n flink-operator flink-operator --replicas=0

Uninstall the operator resources:

    helm uninstall flink-controller-operator --namespace flink-operator
    helm uninstall flink-controller-roles --namespace flink-operator
    helm uninstall flink-controller-crd
