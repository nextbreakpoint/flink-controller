# Flink Controller - User Manual

This document describes how to use Flink Controller. The manual provides instruction for Minikube, but they can be adapted to any flavor of Kubernetes.   

## Setup test environment

Install Minikube 1.34. Install kubectl, Helm 3, and AWS CLI.

Start Minikube with at least 8Gb of memory and Kubernetes 1.31:

    minikube start --cpus=4 --memory=8gb --kubernetes-version v1.31.0 --disk-size 80GB --driver=<your-driver>

## Install the operator

Create a namespace for the operator:

    kubectl create namespace flink-operator

Create a namespace for executing Flink:

    kubectl create namespace flink-jobs

Generate the keystores and truststores required for enabling SSL:

    ./secrets.sh flink-operator key-password keystore-password truststore-password

Create a secret which contains the keystore and truststore files:

    kubectl -n flink-operator create secret generic flink-operator-ssl \
        --from-file=keystore.jks=secrets/keystore-operator-api.jks \
        --from-file=truststore.jks=secrets/truststore-operator-api.jks \
        --from-literal=keystore-secret=keystore-password \
        --from-literal=truststore-secret=truststore-password

Install the CRDs (Custom Resource Definitions):

    helm install flink-controller-crd helm/flink-controller-crd

Install the required roles:

    helm install flink-controller-roles helm/flink-controller-roles --namespace flink-operator --set targetNamespace=flink-jobs

Install the operator with SSL enabled:

    helm install flink-controller-operator helm/flink-controller-operator --namespace flink-operator --set targetNamespace=flink-jobs --set secretName=flink-operator-ssl

Scale the operator up (use 2 replicas to enable HA):

    kubectl -n flink-operator scale deployment flink-operator --replicas=1

## Create your first deployment

Pull the base Docker image:

    docker pull nextbreakpoint/flinkctl:1.5.0

Download Flink Dummies:

    curl https://github.com/nextbreakpoint/flink-dummies/releases/download/v0.0.6/com.nextbreakpoint.flink.dummies-0.0.6.jar -L -o com.nextbreakpoint.flink.dummies.jar

Create the Dockerfile for the bootstrap image: 

    FROM nextbreakpoint/flinkctl:1.5.0
    COPY /com.nextbreakpoint.flink.dummies.jar /

Build the Docker image:

    docker build -t example/jobs:latest .

Pull the Flink Docker image:

    docker pull apache/flink:1.20.0-scala_2.12-java11

Install Minio to simulate a distributed filesystem base on AWS S3 API.

    helm install minio --namespace minio --create-namespace oci://registry-1.docker.io/bitnamicharts/minio --set auth.rootUser=minioaccesskey --set auth.rootPassword=miniosecretkey 

Expose Minio port to host:

    kubectl -n minio expose service minio --name=minio-external --type=LoadBalancer --external-ip=$(minikube ip) --port=9000 --target-port=9000

Expose Minio console port to host (optional):

    kubectl -n minio expose service minio --name=minio-console-external --type=LoadBalancer --external-ip=$(minikube ip) --port=9001 --target-port=9001

Create the bucket with AWS CLI:

    export AWS_ACCESS_KEY_ID=minioaccesskey  
    export AWS_SECRET_ACCESS_KEY=miniosecretkey
    aws s3 mb s3://nextbreakpoint-example --endpoint-url http://$(minikube ip):9000

Create a file flinkproperties.yaml for the JobManager and TaskManager configuration:

    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: demo-jobmanager-properties-v1
    data:
      FLINK_PROPERTIES: |
        heartbeat.timeout: 90000
        heartbeat.interval: 15000
        jobmanager.memory.jvm-overhead.min: 64mb
        jobmanager.memory.jvm-metaspace.size: 192mb
        jobmanager.memory.off-heap.size: 64mb
        jobmanager.memory.process.size: 500mb
        jobmanager.memory.flink.size: 200mb
        metrics.reporters: prometheus
        metrics.reporter.prometheus.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
        metrics.reporter.prometheus.port: 9250
        metrics.latency.granularity: operator
        state.backend.type: hashmap
        s3.endpoint: http://minio.minio:9000
        s3.path.style.access: true
        s3.connection.maximum: 200
        s3.access-key: minioaccesskey
        s3.secret-key: miniosecretkey
        execution.checkpointing.dir: s3p://nextbreakpoint-demo/checkpoints
        execution.checkpointing.savepoint-dir: s3p://nextbreakpoint-demo/savepoints
    ---
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: demo-taskmanager-properties-v1
    data:
      FLINK_PROPERTIES: |
        heartbeat.timeout: 90000
        heartbeat.interval: 15000
        taskmanager.memory.jvm-overhead.min: 64mb
        taskmanager.memory.jvm-metaspace.size: 192mb
        taskmanager.memory.framework.heap.size: 64mb
        taskmanager.memory.framework.off-heap.size: 64mb
        taskmanager.memory.process.size: 600mb
        taskmanager.memory.flink.size: 300mb
        taskmanager.memory.network.fraction: 0.1
        taskmanager.memory.managed.fraction: 0.1
        metrics.reporters: prometheus
        metrics.reporter.prometheus.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
        metrics.reporter.prometheus.port: 9250
        metrics.latency.granularity: operator
        state.backend.type: hashmap
        s3.endpoint: http://minio.minio:9000
        s3.path.style.access: true
        s3.connection.maximum: 200
        s3.access-key: minioaccesskey
        s3.secret-key: miniosecretkey
        execution.checkpointing.dir: s3p://nextbreakpoint-demo/checkpoints
        execution.checkpointing.savepoint-dir: s3p://nextbreakpoint-demo/savepoints

Deploy the resources:

    kubectl -n flink-jobs apply -f flinkproperties.yaml

Create a file jobproperties.yaml for the jobs configuration:

    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: demo-job-properties-v1
    data:
      measurement.properties: |
        job.name: temperature-measurement
        log.verbosity: 2
        watermark.strategy.max-out-of-orderness: 5000
        watermark.strategy.idle-timeout: 30000
        source.max-delay: 5000
        source.max-count: 0
        bucket.output-path: s3a://nextbreakpoint-demo/temperature-measurement
        bucket.check-interval: 30000
        bucket.rollover-interval: 300000
        bucket.inactivity-interval: 300000
        bucket.max-part-size-mb: 1024
        window.size: 60000
        window.slide: 60000
        flink.rest.bind-port: 8081
        flink.parallelism.default: 4
        flink.pipeline.max-parallelism: 128
        flink.pipeline.auto-watermark-interval: 10000
        flink.pipeline.operator-chaining.enabled: true
        flink.state.backend.type: hashmap
        flink.execution.checkpointing.interval: 300000
        flink.restart-strategy.type: disable
        #flink.restart-strategy.type: fixed-delay
        #flink.restart-strategy.fixed-delay.attempts: 3
        #flink.restart-strategy.fixed-delay.delay: 60000

Deploy the resources:

    kubectl -n flink-jobs apply -f jobproperties.yaml

Create a deployment.yaml file to describe cluster and jobs:

    apiVersion: "nextbreakpoint.com/v1"
    kind: FlinkDeployment
    metadata:
      name: "demo"
    spec:
      cluster:
        supervisor:
          pullPolicy: IfNotPresent
          image: nextbreakpoint/flinkctl:1.5.0
          serviceAccount: flink-supervisor
          taskTimeout: 180
          rescaleDelay: 10
          pollingInterval: 5
          rescalePolicy: JobParallelism
          replicas: 1
          resources:
            limits:
              cpu: '1'
              memory: 256Mi
            requests:
              cpu: '0.05'
              memory: 200Mi
        runtime:
          pullPolicy: IfNotPresent
          image: apache/flink:1.20.0-scala_2.12-java11
        jobManager:
          serviceMode: ClusterIP
          annotations:
            managedBy: "flinkctl"
          environment:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: "flink-s3-fs-hadoop-1.20.0.jar;flink-s3-fs-presto-1.20.0.jar"
          environmentFrom:
            - configMapRef:
                name: demo-jobmanager-properties-v1
          volumeMounts:
            - name: config-vol
              mountPath: /var/config/measurement.properties
              subPath: measurement.properties
          volumes:
            - name: config-vol
              configMap:
                name: demo-job-properties-v1
          extraPorts:
            - name: prometheus
              containerPort: 9250
              protocol: TCP
          resources:
            limits:
              cpu: '1'
              memory: 600Mi
            requests:
              cpu: '0.1'
              memory: 600Mi
        taskManager:
          taskSlots: 1
          annotations:
            managedBy: "flinkctl"
          environment:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: "flink-s3-fs-hadoop-1.20.0.jar;flink-s3-fs-presto-1.20.0.jar"
          environmentFrom:
            - configMapRef:
                name: demo-taskmanager-properties-v1
          extraPorts:
            - name: prometheus
              containerPort: 9250
              protocol: TCP
          resources:
            limits:
              cpu: '1'
              memory: 2200Mi
            requests:
              cpu: '0.05'
              memory: 2200Mi
      jobs:
        - name: measurement
          spec:
            jobParallelism: 1
            savepoint:
              savepointMode: Automatic
              savepointInterval: 600
              savepointTargetPath: s3p://nextbreakpoint-demo/savepoints
            restart:
              restartPolicy: Always
              restartDelay: 30
              restartTimeout: 120
            bootstrap:
              serviceAccount: flink-bootstrap
              pullPolicy: IfNotPresent
              image: demo/jobs:latest
              jarPath: /com.nextbreakpoint.flink.dummies.jar
              className: com.nextbreakpoint.flink.dummies.TemperatureMeasurementMain
              arguments:
                - --CONFIG_FILE
                - file:///var/config/measurement.properties
              resources:
                limits:
                  cpu: '1'
                  memory: 200Mi
                requests:
                  cpu: '0.01'
                  memory: 200Mi

Deploy the resource:

    kubectl -n flink-jobs apply -f deployment.yaml

The operator should create two derived resources: one FlinkCluster resource and one FlinkJob resource.

Wait few minutes until the operator creates the supervisor of the cluster, which will deploy the JobManager, create the required TaskManagers, and start the jobs. 

You can see what the operator is doing tailing the logs:

    kubectl -n flink-operator logs -f --tail=-1 -l app=flink-operator

    2024-12-20 13:38:57 INFO MonitoringVerticle - Monitor listening on port 8080
    2024-12-20 13:38:59 INFO OperatorVerticle - Operator listening on port 4444
    2024-12-20 13:39:01,472 [main] INFO io.kubernetes.client.extended.leaderelection.LeaderElector - Successfully acquired lease, became leader
    2024-12-20 13:39:01,473 [leader-elector-hook-worker-1] INFO io.kubernetes.client.extended.controller.LeaderElectingController - Lease acquired, starting controller..
    2024-12-20 13:43:21 INFO Operator - Add finalizer: deployment example
    2024-12-20 13:43:41 INFO Operator - Cluster example created
    2024-12-20 13:43:41 INFO Operator - Job example-measurement created
    2024-12-20 13:43:51 INFO Operator example - Add finalizer: cluster example
    2024-12-20 13:44:01 INFO Operator example - Supervisor not found. Creating supervisor deployment...
    2024-12-20 13:45:04 INFO Operator example - Supervisor deployment created
    ...

You can see what the supervisor is doing tailing the logs:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=supervisor

    2024-12-20 14:50:11 INFO MonitoringVerticle - Monitor listening on port 8080
    2024-12-20 14:50:11 INFO SupervisorVerticle - Supervisor listening on port 4445
    2024-12-20 14:50:31,458 [main] INFO io.kubernetes.client.extended.leaderelection.LeaderElector - Successfully acquired lease, became leader
    2024-12-20 14:50:31,459 [leader-elector-hook-worker-1] INFO io.kubernetes.client.extended.controller.LeaderElectingController - Lease acquired, starting controller..
    2024-12-20 14:50:31 INFO Supervisor example - Resource version: 6102
    2024-12-20 14:50:31 INFO Supervisor example - Cluster status: Unknown
    2024-12-20 14:50:31 INFO Supervisor example - Add finalizer
    2024-12-20 14:50:31 INFO Supervisor example-measurement - Resource version: 6089
    2024-12-20 14:50:31 INFO Supervisor example-measurement - Job status: Unknown
    2024-12-20 14:50:31 INFO Supervisor example-measurement - Add finalizer
    2024-12-20 14:50:36 INFO Supervisor example - Resource version: 6158
    2024-12-20 14:50:36 INFO Supervisor example - Cluster initialised
    2024-12-20 14:50:36 INFO Supervisor example-measurement - Resource version: 6160
    2024-12-20 14:50:41 INFO Supervisor example - Resource version: 6167
    2024-12-20 14:50:41 INFO Supervisor example - Cluster status: Starting
    2024-12-20 14:50:41 INFO Supervisor example - JobManager pod created
    2024-12-20 14:50:41 INFO Supervisor example-measurement - Resource version: 6169
    2024-12-20 14:50:41 INFO Supervisor example-measurement - Job status: Starting
    2024-12-20 14:50:46 INFO Supervisor example - Resource version: 6183
    2024-12-20 14:50:46 INFO Supervisor example - JobManager service created
    2024-12-20 14:50:51 INFO Supervisor example - Resource version: 6198
    2024-12-20 14:51:02 INFO Supervisor example - Cluster started
    2024-12-20 14:51:07 INFO Supervisor example - Resource version: 6218
    2024-12-20 14:51:07 INFO Supervisor example - Cluster status: Started
    2024-12-20 14:51:07 INFO Supervisor example - Detected change: TaskManagers (required: 1, declared: 0)
    2024-12-20 14:51:07 INFO Supervisor example-measurement - Resource version: 6219
    2024-12-20 14:51:12 INFO Supervisor example - Resource version: 6227
    2024-12-20 14:51:12 INFO Supervisor example - TaskManagers pod created (taskmanager-example-8lhmz)
    2024-12-20 14:51:17 INFO Supervisor example - Resource version: 6237
    2024-12-20 14:51:22 INFO Supervisor example - Resource version: 6248
    2024-12-20 14:51:32 WARNING Supervisor example-measurement - Job not ready yet
    2024-12-20 14:51:32 INFO Supervisor example-measurement - Bootstrap job created
    2024-12-20 14:51:37 INFO Supervisor example - Resource version: 6268
    2024-12-20 14:51:37 INFO Supervisor example-measurement - Resource version: 6270
    2024-12-20 14:51:37 WARNING Supervisor example-measurement - Job not ready yet
    2024-12-20 14:51:42 INFO Supervisor example-measurement - Resource version: 6292
    2024-12-20 14:51:42 INFO Supervisor example-measurement - Job started
    2024-12-20 14:51:47 INFO Supervisor example-measurement - Resource version: 6298
    2024-12-20 14:51:47 INFO Supervisor example-measurement - Job status: Started
    2024-12-20 14:51:53 INFO Supervisor example-measurement - Resource version: 6315
    ...

You can watch the FlinkDeployment resource:

    kubectl -n flink-jobs get fd --watch

    NAME      RESOURCE-STATUS   AGE
    example                     0s
    example   Updated           9s
    example   Updated           9s
    example   Updated           19s
    example   Updating          30s
    example   Updated           40s

You can watch the FlinkCluster resource:

    kubectl -n flink-jobs get fc --watch

    NAME      RESOURCE-STATUS   SUPERVISOR-STATUS   CLUSTER-HEALTH   TASK-MANAGERS   REQUESTED-TASK-MANAGERS   TASK-MANAGERS-REPLICAS   TOTAL-TASK-SLOTS   SERVICE-MODE   AGE
    example                                                                                                                                                               0s
    example                                                                                                                                                               11s
    example                                                          0                                         0                        0                                 49s
    example                                                          0                                         0                        0                                 49s
    example   Updating          Starting                             0                                         0                        0                  ClusterIP      54s
    example   Updating          Starting                             0                                         0                        0                  ClusterIP      54s
    example   Updating          Starting                             0                                         0                        0                  ClusterIP      59s
    example   Updating          Starting                             0                                         0                        0                  ClusterIP      64s
    example   Updating          Started                              0                                         0                        0                  ClusterIP      80s
    example   Updating          Started                              0                                         0                        0                  ClusterIP      80s
    example   Updating          Started                              0               1                         0                        0                  ClusterIP      85s
    example   Updating          Started             HEALTHY          0               1                         0                        0                  ClusterIP      85s
    example   Updating          Started             HEALTHY          1               1                         0                        0                  ClusterIP      90s
    example   Updating          Started             HEALTHY          1               1                         1                        1                  ClusterIP      95s
    example   Updated           Started             HEALTHY          1               1                         1                        1                  ClusterIP      110s

You can watch the FlinkJob resources:

    kubectl -n flink-jobs get fj --watch

    NAME                  RESOURCE-STATUS   SUPERVISOR-STATUS   CLUSTER-NAME   CLUSTER-HEALTH   JOB-STATUS   JOB-ID                             JOB-RESTART   BOOTSTRAP-ATTEMPTS   JOB-PARALLELISM   REQUESTED-JOB-PARALLELISM   SAVEPOINT-MODE   SAVEPOINT-PATH   SAVEPOINT-AGE   AGE
    example-measurement                                                                                                                                                                              1                                                                             0s
    example-measurement                                                                                                                         Always                                               1                           Automatic                                         49s
    example-measurement                                                                                                                         Always                                               1                           Automatic                                         49s
    example-measurement   Updating          Starting            example                                                                         Always                             1                 1                           Automatic                                         54s
    example-measurement   Updating          Starting            example                                                                         Always                             1                 1                           Automatic                                         54s
    example-measurement   Updating          Starting            example        HEALTHY                                                          Always                             1                 1                           Automatic                                         80s
    example-measurement   Updated           Starting            example        HEALTHY                                                          Always        1                    1                 1                           Automatic                                         110s
    example-measurement   Updated           Starting            example        HEALTHY                       877c37a95c69572195f29479fa85194d   Always        1                    1                 1                           Automatic                                         118s
    example-measurement   Updated           Started             example        HEALTHY                       877c37a95c69572195f29479fa85194d   Always        1                    1                 1                           Automatic                                         2m
    example-measurement   Updated           Started             example        HEALTHY          RUNNING      877c37a95c69572195f29479fa85194d   Always        1                    1                 1                           Automatic                                         2m5s
    
You can watch the pods:

    kubectl -n flink-jobs get pod --watch

    NAME                                  READY   STATUS              RESTARTS   AGE
    supervisor-example-66475fbd47-scmjv   0/1     Pending             0          0s
    supervisor-example-66475fbd47-scmjv   0/1     Pending             0          0s
    supervisor-example-66475fbd47-scmjv   0/1     ContainerCreating   0          0s
    supervisor-example-66475fbd47-scmjv   1/1     Running             0          2s
    jobmanager-example-xgrf6              0/1     Pending             0          0s
    jobmanager-example-xgrf6              0/1     Pending             0          0s
    jobmanager-example-xgrf6              0/1     ContainerCreating   0          0s
    jobmanager-example-xgrf6              1/1     Running             0          1s
    taskmanager-example-hc8st             0/1     Pending             0          0s
    taskmanager-example-hc8st             0/1     Pending             0          0s
    taskmanager-example-hc8st             0/1     ContainerCreating   0          0s
    taskmanager-example-hc8st             1/1     Running             0          1s
    bootstrap-example-measurement-nxh5p   0/1     Pending             0          0s
    bootstrap-example-measurement-nxh5p   0/1     Pending             0          0s
    bootstrap-example-measurement-nxh5p   0/1     ContainerCreating   0          0s
    bootstrap-example-measurement-nxh5p   1/1     Running             0          2s
    bootstrap-example-measurement-nxh5p   0/1     Completed           0          9s
    bootstrap-example-measurement-nxh5p   0/1     Completed           0          10s

You can inspect the FlinkDeployment resource:

    kubectl -n flink-jobs get fd example -o json | jq '.status'

    {
        "digest": {
            "cluster": {
                "jobManager": "/gG9YWPX41TVIHuHwDqDIg==",
                "runtime": "6wkNiz3ssaPywOH5hK6iXA==",
                "supervisor": "0WYzkm5+u3F6KQHPj6obYA==",
                "taskManager": "33fiUNBQoqnF2JV2B15gbg=="
            },
            "jobs": [
                {
                    "job": {
                        "bootstrap": "GoGNscPMPZ58tqTHG6p1gg==",
                        "restart": "+w3hQCjDXsex9v8jy+MxqQ==",
                        "savepoint": "YFAtz4KisaFnWrsHVyt8tA=="
                    },
                    "name": "measurement"
                }
            ]
        },
        "resourceStatus": "Updated",
        "timestamp": "2024-12-20T14:58:28.017000Z"
    }

You can inspect the FlinkCluster resource:

    kubectl -n flink-jobs get fc example -o json | jq '.status'

    {
        "clusterHealth": "HEALTHY",
        "digest": {
            "jobManager": "/gG9YWPX41TVIHuHwDqDIg==",
            "runtime": "6wkNiz3ssaPywOH5hK6iXA==",
            "supervisor": "0WYzkm5+u3F6KQHPj6obYA==",
            "taskManager": "33fiUNBQoqnF2JV2B15gbg=="
        },
        "labelSelector": "clusterName=example,owner=flink-operator,component=flink",
        "rescaleTimestamp": "2024-12-20T15:05:55.893000Z",
        "resourceStatus": "Updated",
        "serviceMode": "ClusterIP",
        "supervisorStatus": "Started",
        "taskManagerReplicas": 1,
        "taskManagers": 1,
        "taskSlots": 1,
        "timestamp": "2024-12-20T15:06:00.928000Z",
        "totalTaskSlots": 1
    }

You can inspect the FlinkJob resources:

    kubectl -n flink-jobs get fj example-measurement -o json | jq '.status'

    {
        "bootstrapAttempts": 2,
        "clusterHealth": "HEALTHY",
        "clusterName": "example",
        "digest": {
            "bootstrap": "GoGNscPMPZ58tqTHG6p1gg==",
            "restart": "+w3hQCjDXsex9v8jy+MxqQ==",
            "savepoint": "YFAtz4KisaFnWrsHVyt8tA=="
        },
        "jobId": "89d52e86a177b7e6ec3cd818b5bceb81",
        "jobParallelism": 1,
        "jobStatus": "RUNNING",
        "labelSelector": "clusterName=example,owner=flink-operator,component=flink",
        "resourceStatus": "Updated",
        "restartPolicy": "Always",
        "savepointJobId": "",
        "savepointMode": "Automatic",
        "savepointPath": "",
        "savepointRequestTimestamp": "2024-12-20T14:59:11.613000Z",
        "savepointTriggerId": "",
        "supervisorStatus": "Started",
        "timestamp": "2024-12-20T15:06:11.054000Z"
    }

You can tail the logs of the JobManager:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=jobmanager -c jobmanager

You can tail the logs of the TaskManager:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=taskmanager -c taskmanager

You can expose the JobManager web console:

    kubectl -n flink-jobs port-forward service/jobmanager-example 8081

then open a browser at http://localhost:8081.

## Control cluster and jobs with annotations

Annotate the cluster resource to stop the cluster:

    kubectl -n flink-jobs annotate fc example --overwrite operator.nextbreakpoint.com/requested-action=STOP

Annotate the cluster resource to restart the cluster:

    kubectl -n flink-jobs annotate fc example --overwrite operator.nextbreakpoint.com/requested-action=START

Annotate the job resource to stop the job (but not the cluster):

    kubectl -n flink-jobs annotate fj example-measurement --overwrite operator.nextbreakpoint.com/requested-action=STOP

Annotate the job resource to restart the job (but not the cluster):

    kubectl -n flink-jobs annotate fj example-measurement --overwrite operator.nextbreakpoint.com/requested-action=START

Annotate the job resource to trigger a savepoint:

    kubectl -n flink-jobs annotate fj example-measurement --overwrite operator.nextbreakpoint.com/requested-action=TRIGGER_SAVEPOINT

Annotate the job resource to forget a savepoint:

    kubectl -n flink-jobs annotate fj example-measurement --overwrite operator.nextbreakpoint.com/requested-action=FORGET_SAVEPOINT

## Control cluster and jobs using HTTP  

The operator exposes a control interface as REST API (port 4444 by default).

The control interface can be used to fetch status, details and metrics, and to submit commands which can be understood by the operator and the supervisor.

The following endpoints support GET requests:

    /deployments
    /deployments/<deploymentname>/status
    /clusters
    /cluster/<clustername>/status
    /cluster/<clustername>/jobs
    /cluster/<clustername>/jobs/<jobname>/status
    /cluster/<clustername>/jobs/<jobname>/details
    /cluster/<clustername>/jobs/<jobname>/metrics
    /cluster/<clustername>/jobmanager/metrics
    /cluster/<clustername>/taskmanagers
    /cluster/<clustername>/taskmanagers/<taskmanagerid>/details
    /cluster/<clustername>/taskmanagers/<taskmanagerid>/metrics

The following endpoints support POST requests:

    /deployments/<deploymentname>
    /clusters/<clustername>
    /clusters/<clustername>/jobs/<jobname>

The following endpoints support DELETE requests:

    /deployments/<deploymentname>
    /clusters/<clustername>
    /clusters/<clustername>/jobs/<jobname>
    /clusters/<clustername>/jobs/<jobname>/savepoint

The following endpoints support PUT requests:

    /deployments/<deploymentname>
    /clusters/<clustername>
    /clusters/<clustername>/jobs/<jobname>
    /clusters/<clustername>/start
    /clusters/<clustername>/stop
    /clusters/<clustername>/scale
    /clusters/<clustername>/jobs/<jobname>/start
    /clusters/<clustername>/jobs/<jobname>/stop
    /clusters/<clustername>/jobs/<jobname>/scale
    /clusters/<clustername>/jobs/<jobname>/savepoint

Reinstall the operator with SSL disabled:

    helm upgrade --install flink-controller-operator helm/flink-controller-operator --namespace flink-operator --set targetNamespace=flink-jobs --set replicas=1

Expose the control interface to your host:

    kubectl -n flink-operator port-forward service/flink-operator 4444

Get list of deployments:

    curl http://localhost:4444/api/v1/deployments

Get list of clusters:

    curl http://localhost:4444/api/v1/clusters

Get list of jobs:

    curl http://localhost:4444/api/v1/clusters/custer-1/jobs

Get status of a deployment:

    curl http://localhost:4444/api/v1/deployments/example/status

Get status of a cluster:

    curl http://localhost:4444/api/v1/clusters/example/status

Get status of a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/status

Get details of a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/details

Get metrics of a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/metrics

Get metrics of the JobManager:

    curl http://localhost:4444/api/v1/clusters/example/jobmanager/metrics

Get list of TaskManagers:

    curl http://localhost:4444/api/v1/clusters/example/taskmanagers

Get metrics of a TaskManager:

    curl http://localhost:4444/api/v1/clusters/example/taskmanagers/67761be7be3c93b44dd037632871c828/metrics

Get details of a TaskManager:

    curl http://localhost:4444/api/v1/clusters/example/taskmanagers/67761be7be3c93b44dd037632871c828/details

Create a deployment:

    curl http://localhost:4444/api/v1/deployments/example -XPOST -d@example/deployment-spec.json

Create a cluster:

    curl http://localhost:4444/api/v1/clusters/example -XPOST -d@example/cluster-spec.json

Create a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement -XPOST -d@example/job-spec-1.json

Delete a deployment:

    curl http://localhost:4444/api/v1/deployments/example -XDELETE

Delete a cluster:

    curl http://localhost:4444/api/v1/clusters/example -XDELETE

Delete a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement -XDELETE

Update a deployment:

    curl http://localhost:4444/api/v1/deployments/example -XPUT -d@example/deployment-spec.json

Update a cluster:

    curl http://localhost:4444/api/v1/clusters/example -XPUT -d@example/cluster-spec.json

Update a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement -XPUT -d@example/job-spec-1.json

Start a cluster:

    curl http://localhost:4444/api/v1/clusters/example/start -XPUT -d'{"withoutSavepoint":false}'

Stop a cluster:

    curl http://localhost:4444/api/v1/clusters/example/stop -XPUT -d'{"withoutSavepoint":false}'

Scale a cluster:

    curl http://localhost:4444/api/v1/clusters/example/scale -XPUT -d'{"taskManagers":4}'

Start a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/start -XPUT -d'{"withoutSavepoint":false}'

Stop a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/stop -XPUT -d'{"withoutSavepoint":false}'

Scale a job:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/scale -XPUT -d'{"parallelism":2}'

Trigger a savepoint:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/savepoint -XPUT -d' '

Forget a savepoint:

    curl http://localhost:4444/api/v1/clusters/example/jobs/measurement/savepoint -XDELETE

Please note that you must provide the SSL certificates when the operator API is secured with TLS:

    curl --cacert secrets/ca_cert.pem --cert secrets/operator-cli_cert.pem --key secrets/operator-cli_key.pem https://localhost:4444/api/v1/clusters/<name>/status

## Control cluster and jobs using flinkctl

Configure Docker environment if using Minikube:

    eval $(minikube docker-env)

Print the CLI usage:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 --help

The output should look like:

    Usage: flinkctl [OPTIONS] COMMAND [ARGS]...

    Options:
        -h, --help  Show this message and exit

    Commands:
        operator      Access operator subcommands
        supervisor    Access supervisor subcommands
        bootstrap     Access bootstrap subcommands
        deployments   Access deployments subcommands
        deployment    Access deployment subcommands
        clusters      Access clusters subcommands
        cluster       Access cluster subcommands
        savepoint     Access savepoint subcommands
        jobs          Access jobs subcommands
        job           Access job subcommands
        jobmanager    Access JobManager subcommands
        taskmanager   Access TaskManager subcommands
        taskmanagers  Access TaskManagers subcommands

You can see the options of each subcommand:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster create --help

    Usage: flinkctl cluster create [OPTIONS]

        Create a cluster

    Options:
        --host TEXT               The operator host
        --port INT                The operator port
        --keystore-path TEXT      The keystore path
        --keystore-secret TEXT    The keystore secret
        --truststore-path TEXT    The truststore path
        --truststore-secret TEXT  The truststore secret
        --cluster-name TEXT       The name of the Flink cluster
        --cluster-spec TEXT       The specification of the Flink cluster in JSON format
        -h, --help                Show this message and exit

Reinstall the operator with SSL disabled:

    helm upgrade --install flink-controller-operator helm/flink-controller-operator --namespace flink-operator --set targetNamespace=flink-jobs --set replicas=1

Expose the operator using an external address:

    kubectl -n flink-operator expose service flink-operator --name=flink-operator-external --port=4444 --target-port=4444 --external-ip=$(minikube ip)

Get the list of deployments:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 deployments list --host=$(minikube ip) | jq -r '.output' | jq

Get the list of clusters:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 clusters list --host=$(minikube ip) | jq -r '.output' | jq

Get the list of jobs:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 jobs list --cluster-name=example --host=$(minikube ip) | jq -r '.output' | jq

Get the status of a deployment:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 deployment status --deployment-name=example --host=$(minikube ip) | jq -r '.output' | jq

Get the status of a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster status --cluster-name=example --host=$(minikube ip) | jq -r '.output' | jq

Get the status of a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job status --cluster-name=example --job-name=measurement --host=$(minikube ip) | jq -r '.output' | jq

Delete a deployment:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 deployment delete --deployment-name=example --host=$(minikube ip)

Delete a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster delete --cluster-name=example --host=$(minikube ip)

Delete a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job delete --cluster-name=example --job-name=measurement --host=$(minikube ip)

Stop a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster stop --cluster-name=example --host=$(minikube ip)

Start a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster start --cluster-name=example --host=$(minikube ip)

Stop a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job stop --cluster-name=example --job-name=measurement --host=$(minikube ip)

Start a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job start --cluster-name=example --job-name=measurement --host=$(minikube ip)

Start a cluster without recovering from the savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster start --cluster-name=example --without-savepoint --host=$(minikube ip)

Stop a cluster without creating a new savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster stop --cluster-name=example --without-savepoint --host=$(minikube ip)

Start a job without recovering from the savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job start --cluster-name=example --job-name=measurement --without-savepoint --host=$(minikube ip)

Stop a job without creating a new savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job stop --cluster-name=example --job-name=measurement --without-savepoint --host=$(minikube ip)

Create a new savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 savepoint trigger --cluster-name=example --job-name=measurement --host=$(minikube ip)

Remove savepoint from job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 savepoint forget --cluster-name=example --job-name=measurement --host=$(minikube ip)

Rescale a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster scale --cluster-name=example --task-managers=4 --host=$(minikube ip)

Rescale a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job scale --cluster-name=example --job-name=measurement --parallelism=2 --host=$(minikube ip)

Get the details of the job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job details --cluster-name=example --job-name=measurement --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of the job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job metrics --cluster-name=example --job-name=measurement --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of the JobManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 jobmanager metrics --cluster-name=example --host=$(minikube ip) | jq -r '.output' | jq

Get a list of TaskManagers:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 taskmanagers list --cluster-name=example --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of a TaskManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 taskmanager metrics --cluster-name=example --taskmanager-id=67761be7be3c93b44dd037632871c828 --host=$(minikube ip) | jq -r '.output' | jq

Get the details of a TaskManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 taskmanager details --cluster-name=example --taskmanager-id=67761be7be3c93b44dd037632871c828 --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of a TaskManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 taskmanager metrics --cluster-name=example --taskmanager-id=67761be7be3c93b44dd037632871c828 --host=$(minikube ip) | jq -r '.output' | jq

Please note that you must provide the SSL certificates when the operator API is secured with TLS:

    docker run --rm -it -v /var/secrets:/secrets nextbreakpoint/flinkctl:1.5.0 deployments list --keystore-path=/secrets/keystore-operator-cli.jks --truststore-path=/secrets/truststore-operator-cli.jks --keystore-secret=keystore-password --truststore-secret=truststore-password --host=$(minikube ip) | jq -r '.output' | jq

When using minikube, the secrets can be mounted from host:

    minikube start --cpus=4 --memory=8gb -disk-size 80GB --kubernetes-version v1.31.0 --driver=<your_driver> --mount-string="$(pwd)/secrets:/var/secrets" --mount

## Server components

The command flinkctl implements both client and server components. The client commands are the ones used to operate on the resources.
The server commands instead are the ones used to run the processes which together implement the Kubernetes Operator pattern.

### Operator process

The operator process is required to orchestrate supervisors:

    flinkctl operator run --namespace=flink-jobs --task-timeout=180 --polling-interval=5

### Supervisor process

The supervisor process is required to reconcile the status of a cluster and its jobs:

    flinkctl supervisor run --namespace=flink-jobs --cluster-name=example --task-timeout=180 --polling-interval=5

### Bootstrap process

The bootstrap process is required to upload the JAR to the JobManager and run the job:

    flinkctl bootstrap run --namespace=flink-jobs --cluster-name=example --job-name=measurement --class-name=your-main-class --jar-path=/your-job-jar.jar --parallelism=2

### Monitoring

The operator process exposes metrics to Prometheus on port 8080 by default:

    http://localhost:8080/metrics

The supervisor process exposes metrics to Prometheus on port 8080 by default:

    http://localhost:8080/metrics

### Healthcheck

The operator process exposes a version endpoint which can be used to check the process is healthy:

    http://localhost:8080/version

The supervisor process exposes a version endpoint which can be used to check the process is healthy:

    http://localhost:8080/version

### State machines

The supervisor implements two state machines for managing clusters and jobs.

The state machine associated to a cluster is documented in this graph:

![Cluster state machine](/graphs/flink-cluster.png "Cluster state machine")

The state machine associated to a job is documented in this graph:

![Job state machine](/graphs/flink-job.png "Job state machine")

## Custom Resources

FlinkDeployment, FlinkCluster and FlinkJob are the custom resources provide by Flink Control.  

They can be created, deleted, and inspected as any other Kubernetes resource using kubectl or Helm.

The custom resources are defined in the Helm chart:

    https://github.com/nextbreakpoint/flink-control/blob/master/helm/flink-controller-crd/templates/flinkdeployment.yaml
    https://github.com/nextbreakpoint/flink-control/blob/master/helm/flink-controller-crd/templates/flinkcluster.yaml
    https://github.com/nextbreakpoint/flink-control/blob/master/helm/flink-controller-crd/templates/flinkjob.yaml

The schema of the resource contains the documentation for each field:

    kubectl get crd -o json flinkdeployments.nextbreakpoint.com | jq '.spec.versions[0].schema.openAPIV3Schema.properties' > schemas/flinkdeployment-schema.json
    kubectl get crd -o json flinkclusters.nextbreakpoint.com | jq '.spec.versions[0].schema.openAPIV3Schema.properties' > schemas/flinkcluster-schema.json
    kubectl get crd -o json flinkjobs.nextbreakpoint.com | jq '.spec.versions[0].schema.openAPIV3Schema.properties' > schemas/flinkjob-schema.json

## Build and execute

This section provides the instructions for building and executing flinkctl.

### Requirements

Install Java 21 and Maven 3.8.

### Build shaded JAR

Compile and package the shaded JAR:

    mvn clean package -DskipTests=true -Pshaded

The shaded JAR will be created under directory target.

Execute the shaded JAR:

    java --module-path target/com.nextbreakpoint.flink.control-1.5.0-shaded.jar --module com.nextbreakpoint.flink.control/com.nextbreakpoint.flink.controller.cli.Main --help

### Build distribution

Compile and package the distribution:

    mvn clean package -DskipTests=true -Pdistribution

The distribution will be created under directory target/jpackage, and it contains the flinkctl native executable.

Execute the flinkctl command (on Linux):

    ./target/jpackage/flinkctl/bin/flinkctl --help

Execute the flinkctl command (on MacOS):

    ./target/jpackage/flinkctl.app/Contents/MacOS/flinkctl --help

### Build Docker image

Configure Docker environment if using Minikube:

    eval $(minikube docker-env)

Build a Docker image:

    docker build -t nextbreakpoint/flinkctl:1.5.0 .

Execute the image:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 --help

Tag and push the image to your Docker registry if needed:

    docker tag nextbreakpoint/flinkctl:1.5.0 some-registry/flinkctl:1.5.0
    docker login some-registry
    docker push some-registry/flinkctl:1.5.0

### Execute operator

The application can be configured to point to a local or remote Kubernetes cluster, and it works with Minikube and Docker for Desktop too.

Run the operator with a given namespace and Kubernetes config on Linux:

    flinkctl operator run --namespace=test --kube-config=~/.kube/config

Run the operator with a given namespace and Kubernetes config using Docker:

    docker run --rm -it -v ~/.kube/config:/kube/config nextbreakpoint/flinkctl:1.5.0 operator run --namespace=test --kube-config=/kube/config

Run the operator with a given namespace and Kubernetes config with Java command:

    java --module-path target/com.nextbreakpoint.flink.control-1.5.0-shaded.jar --module com.nextbreakpoint.flink.control/com.nextbreakpoint.flink.controller.cli.Main operator run --namespace=test --kube-config=~/.kube/config

Run the operator with a given namespace and Kubernetes config with Java command, and enable debug agent:

    java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5555 --module-path target/com.nextbreakpoint.flink.control-1.5.0-shaded.jar --module com.nextbreakpoint.flink.control/com.nextbreakpoint.flink.controller.cli.Main operator run --namespace=test --kube-config=~/.kube/config

### Run automated tests

Run unit tests:

    mvn test

Run integration tests against Docker for Desktop or Minikube:

    export BUILD_IMAGES=true
    mvn verify

You can skip the Docker images build step if the images already exist:

    export BUILD_IMAGES=false
    mvn verify

## Advanced configuration

Below are the instructions for configuring operator, supervisor, and bootstrap processes.

### Configure service accounts

The service account for the operator is defined in the Helm chart, but it can be modified when installing with Helm.

The service account for the supervisor can be configured in the FlinkDeployment and FlinkCluster resources.
If not specified, the service account flink-supervisor will be used for the supervisor.

The service account of the bootstrap can be configured in the FlinkDeployment and FlinkJob resources.
If not specified, the service account flink-bootstrap will be used for the bootstrap.

The service account for JobManagers and TaskManagers can be configured in the FlinkDeployment and FlinkCluster resources.
If not specified, the default service account will be used for JobManagers and TaskManagers.

In case the default account are not used, then new account must be created with the correct permissions.
The required permissions should match the ones which are defined in the default accounts.

### Configure automatic and periodic savepoints

The operator automatically creates savepoints before stopping a job, and automatically starts a job from the latest savepoint when the job restarts.
The job typically restarts when a change is applied to a specification, or the cluster is rescaled, or the job parallelism is changed, or the job is stopped manually.
This feature is very handy to avoid losing the status of the job when rolling out an update, or in case of temporary failure.

However, for this feature to work properly, the savepoints must be created in a durable storage location such as HDFS or S3.
Only a durable location can be used to recover the job after recreating the JobManager and the TaskManagers.

### Configure task timeout

The operator and the supervisor use timeouts to recover for anomalies.

The duration of the timeout for the operator can be changed when installing the operator with Helm.

The duration of the timeout for the supervisor can be changed in the resource specification.    

### Configure polling interval

The operator and the supervisor poll periodically the status of the resources.

The duration of the polling interval for the operator can be changed when installing the operator with Helm.

The duration of the polling interval for the supervisor can be changed in the resource specification.
