# Flink Control - User Manual

This document describes how to use Flink Control. For installation instructions, see [README](https://github.com/nextbreakpoint/flink-controller/blob/master/README.md).

## Create your first deployment

Make sure that CRDs and default roles have been installed in Kubernetes (see [README](https://github.com/nextbreakpoint/flink-control/blob/master/README.md)).

Pull the base Docker image with command:

    docker pull nextbreakpoint/flinkctl:1.5.0

Create the Dockerfile for the bootstrap image which contains the Flink application:

    ARG base_image="nextbreakpoint/flinkctl:1.5.0"
    FROM ubuntu:latest AS download
    ARG dummies_version="0.0.6"
    RUN apt update -y && apt install -y curl
    RUN curl https://github.com/nextbreakpoint/flink-dummies/releases/download/v${dummies_version}/com.nextbreakpoint.flink.dummies-${dummies_version}.jar -L -o /com.nextbreakpoint.flink.dummies.jar
    FROM ${base_image}
    COPY --from=download /com.nextbreakpoint.flink.dummies.jar /

Build the Docker image with command:

    docker build -t example/jobs:latest .

Pull Flink Docker image with command:

    docker pull apache/flink:1.20.0-scala_2.12-java11

Install Minio to simulate a distributed filesystem base on AWS S3 API.

    helm install minio --namespace minio --create-namespace oci://registry-1.docker.io/bitnamicharts/minio --set auth.rootUser=minioaccesskey --set auth.rootPassword=miniosecretkey 

Expose Minio port to host:

    kubectl -n minio expose service minio --name=minio-external --type=LoadBalancer --external-ip=$(minikube ip) --port=9000 --target-port=9000
    kubectl -n minio expose service minio-console --name=minio-console-external --type=LoadBalancer --external-ip=$(minikube ip) --port=9001 --target-port=9001

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
        jobmanager.memory.process.size: 600mb
        jobmanager.memory.flink.size: 256mb
        metrics.reporters: prometheus
        metrics.reporter.prometheus.class: org.apache.flink.metrics.prometheus.PrometheusReporter
        metrics.reporter.prometheus.port: 9250
        metrics.latency.granularity: operator
        state.backend: filesystem
        state.savepoints.dir: s3p://nextbreakpoint-demo/savepoints
        state.checkpoints.dir: s3p://nextbreakpoint-demo/checkpoints
        s3.connection.maximum: 200
        s3.access-key: minioaccesskey
        s3.secret-key: miniosecretkey
        s3.endpoint: http://minio.minio:9000
        s3.path.style.access: true
    ---
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: demo-taskmanager-properties-v1
    data:
      FLINK_PROPERTIES: |
        heartbeat.timeout: 90000
        heartbeat.interval: 15000
        taskmanager.memory.jvm-overhead.min: 192mb
        taskmanager.memory.jvm-metaspace.size: 256mb
        taskmanager.memory.framework.heap.size: 128mb
        taskmanager.memory.framework.off-heap.size: 128mb
        taskmanager.memory.process.size: 2200mb
        taskmanager.memory.flink.size: 1600mb
        taskmanager.memory.network.fraction: 0.1
        taskmanager.memory.managed.fraction: 0.1
        metrics.reporters: prometheus
        metrics.reporter.prometheus.class: org.apache.flink.metrics.prometheus.PrometheusReporter
        metrics.reporter.prometheus.port: 9250
        metrics.latency.granularity: operator
        state.backend: filesystem
        state.savepoints.dir: s3p://nextbreakpoint-demo/savepoints
        state.checkpoints.dir: s3p://nextbreakpoint-demo/checkpoints
        s3.connection.maximum: 200
        s3.access-key: minioaccesskey
        s3.secret-key: miniosecretkey
        s3.endpoint: http://minio.minio:9000
        s3.path.style.access: true

Deploy the resources with command:

    kubectl -n flink-jobs apply -f flinkproperties.yaml

Create a file jobparameters.yaml for the jobs configuration:

    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: demo-job-parameters-v1
    data:
      computeaverage.conf: |
        rest-port: 8081
        source-delay-array: 250 10
        source-delay-interval: 300000
        source-limit: 0
        console-verbosity: 1
        job-name: computeaverage
        disable-chaining: true
        checkpoint-interval: 300000
        window-size: 60000
        window-slide: 10000
        max-out-of-orderness: 5000
        bucket-check-interval: 30000
        bucket-rollover-interval: 300000
        bucket-inactivity-interval: 300000
        bucket-output-path: /output/computeaverage
        partitions: 32
      computemaximum.conf: |
        rest-port: 8081
        source-delay-array: 250 10
        source-delay-interval: 600000
        source-limit: 0
        console-verbosity: 1
        job-name: computemaximum
        disable-chaining: true
        checkpoint-interval: 300000
        window-size: 60000
        window-slide: 10000
        max-out-of-orderness: 5000
        bucket-check-interval: 30000
        bucket-rollover-interval: 300000
        bucket-inactivity-interval: 300000
        bucket-output-path: /output/computemaximum
        partitions: 32

Deploy the resources with command:

    kubectl -n flink-jobs apply -f jobparameters.yaml

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
          rescalePolicy: JobParallelism
          replicas: 1
          resources:
            limits:
              cpu: '1'
              memory: 200Mi
            requests:
              cpu: '0.05'
              memory: 200Mi
        runtime:
          pullPolicy: IfNotPresent
          image: apache/flink:1.12.1-scala_2.12-java11
        jobManager:
          serviceMode: ClusterIP
          annotations:
            managedBy: "flinkctl"
          environment:
            - name: ENABLE_BUILT_IN_PLUGINS
              value: "flink-s3-fs-hadoop-1.12.1.jar;flink-s3-fs-presto-1.12.1.jar"
          environmentFrom:
            - configMapRef:
                name: demo-jobmanager-properties-v1
          volumeMounts:
            - name: config-vol
              mountPath: /var/config/computeaverage.conf
              subPath: computeaverage.conf
            - name: config-vol
              mountPath: /var/config/computemaximum.conf
              subPath: computemaximum.conf
          volumes:
            - name: config-vol
              configMap:
                name: demo-job-parameters-v1
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
              value: "flink-s3-fs-hadoop-1.12.1.jar;flink-s3-fs-presto-1.12.1.jar"
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
        - name: computeaverage
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
              jarPath: /flink-jobs.jar
              className: com.nextbreakpoint.flink.jobs.ComputeAverage
              arguments:
                - --JOB_PARAMETERS
                - file:///var/config/computemaximum.conf
                - --OUTPUT_LOCATION
                - s3a://nextbreakpoint-demo
              resources:
                limits:
                  cpu: '1'
                  memory: 200Mi
                requests:
                  cpu: '0.01'
                  memory: 200Mi
        - name: computemaximum
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
              jarPath: /flink-jobs.jar
              className: com.nextbreakpoint.flink.jobs.ComputeMaximum
              arguments:
                - --JOB_PARAMETERS
                - file:///var/config/computemaximum.conf
                - --OUTPUT_LOCATION
                - s3a://nextbreakpoint-demo
              resources:
                limits:
                  cpu: '1'
                  memory: 200Mi
                requests:
                  cpu: '0.01'
                  memory: 200Mi

Deploy the resource with command:

    kubectl -n flink-jobs apply -f deployment.yaml

The operator should create these derived resources: one FlinkCluster resource, two FlinkJob resources.

Wait few minutes until the operator creates the supervisor of the cluster which will deploy the JobManager, create the required TaskManagers, and start the jobs. 

You can see what the operator is doing tailing the logs:

    kubectl -n flink-operator logs -f --tail=-1 -l app=flink-operator

    [main] INFO io.kubernetes.client.extended.leaderelection.LeaderElector - Successfully acquired lease, became leader
    [leader-elector-hook-worker-0] INFO io.kubernetes.client.extended.controller.LeaderElectingController - Lease acquired, starting controller..
    2021-01-15 09:45:01 INFO Operator - Add finalizer: deployment demo
    2021-01-15 09:45:21 INFO Operator - Job demo-computeaverage created
    2021-01-15 09:45:21 INFO Operator - Job demo-computemaximum created
    ...

You can see what the supervisor is doing tailing the logs:

    kubectl -n flink-jobs logs -f --tail=-1 -l role=supervisor

    [main] INFO io.kubernetes.client.extended.leaderelection.LeaderElector - Successfully acquired lease, became leader
    [leader-elector-hook-worker-0] INFO io.kubernetes.client.extended.controller.LeaderElectingController - Lease acquired, starting controller..
    2021-01-15 15:33:25 INFO Supervisor demo - Resource version: 562190
    2021-01-15 15:33:25 INFO Supervisor demo - Supervisor status: Unknown
    2021-01-15 15:33:25 INFO Supervisor demo - Add finalizer
    2021-01-15 15:33:25 INFO Supervisor demo-computeaverage - Resource version: 562178
    2021-01-15 15:33:25 INFO Supervisor demo-computeaverage - Supervisor status: Unknown
    2021-01-15 15:33:25 INFO Supervisor demo-computeaverage - Add finalizer
    2021-01-15 15:33:25 INFO Supervisor demo-computemaximum - Resource version: 562179
    2021-01-15 15:33:25 INFO Supervisor demo-computemaximum - Supervisor status: Unknown
    2021-01-15 15:33:25 INFO Supervisor demo-computemaximum - Add finalizer
    2021-01-15 15:33:30 INFO Supervisor demo - Resource version: 562238
    2021-01-15 15:33:30 INFO Supervisor demo - Cluster initialised
    2021-01-15 15:33:30 INFO Supervisor demo-computeaverage - Resource version: 562240
    2021-01-15 15:33:30 INFO Supervisor demo-computemaximum - Resource version: 562242
    2021-01-15 15:33:35 INFO Supervisor demo - Resource version: 562249
    2021-01-15 15:33:35 INFO Supervisor demo - Supervisor status: Starting
    2021-01-15 15:33:35 INFO Supervisor demo - JobManager pod created
    2021-01-15 15:33:35 INFO Supervisor demo-computeaverage - Resource version: 562251
    2021-01-15 15:33:35 INFO Supervisor demo-computeaverage - Supervisor status: Starting
    2021-01-15 15:33:35 INFO Supervisor demo-computemaximum - Resource version: 562253
    2021-01-15 15:33:35 INFO Supervisor demo-computemaximum - Supervisor status: Starting
    2021-01-15 15:33:40 INFO Supervisor demo - Resource version: 562262
    2021-01-15 15:33:40 INFO Supervisor demo - JobManager service created
    2021-01-15 15:33:45 INFO Supervisor demo - Resource version: 562280
    2021-01-15 15:33:45 INFO Supervisor demo - Cluster started
    2021-01-15 15:33:50 INFO Supervisor demo - Resource version: 562288
    2021-01-15 15:33:50 INFO Supervisor demo - Supervisor status: Started
    2021-01-15 15:33:50 INFO Supervisor demo - Detected change: TaskManagers (2/0)
    2021-01-15 15:33:51 INFO Supervisor demo-computeaverage - Resource version: 562290
    2021-01-15 15:33:51 INFO Supervisor demo-computemaximum - Resource version: 562291
    2021-01-15 15:33:56 INFO Supervisor demo - Resource version: 562298
    2021-01-15 15:33:56 INFO Supervisor demo - TaskManagers pod created (taskmanager-demo-hrf8l)
    2021-01-15 15:33:56 INFO Supervisor demo - TaskManagers pod created (taskmanager-demo-lvmzt)
    2021-01-15 15:34:01 INFO Supervisor demo - Resource version: 562310
    2021-01-15 15:34:06 INFO Supervisor demo - Resource version: 562327
    2021-01-15 15:34:06 INFO Supervisor demo-computeaverage - Bootstrap job created
    2021-01-15 15:34:06 INFO Supervisor demo-computemaximum - Bootstrap job created
    2021-01-15 15:34:11 INFO Supervisor demo - Resource version: 562336
    2021-01-15 15:34:11 INFO Supervisor demo-computeaverage - Resource version: 562339
    2021-01-15 15:34:11 WARNING Supervisor demo-computeaverage - Job not ready yet
    2021-01-15 15:34:11 INFO Supervisor demo-computemaximum - Resource version: 562348
    2021-01-15 15:34:11 WARNING Supervisor demo-computemaximum - Job not ready yet
    2021-01-15 15:34:16 WARNING Supervisor demo-computeaverage - Job not ready yet
    2021-01-15 15:34:16 INFO Supervisor demo-computemaximum - Resource version: 562372
    2021-01-15 15:34:16 INFO Supervisor demo-computemaximum - Job started
    2021-01-15 15:34:21 INFO Supervisor demo-computeaverage - Resource version: 562383
    2021-01-15 15:34:21 INFO Supervisor demo-computeaverage - Job started
    2021-01-15 15:34:21 INFO Supervisor demo-computemaximum - Resource version: 562373
    2021-01-15 15:34:21 INFO Supervisor demo-computemaximum - Supervisor status: Started
    2021-01-15 15:34:26 INFO Supervisor demo-computeaverage - Resource version: 562390
    2021-01-15 15:34:26 INFO Supervisor demo-computeaverage - Supervisor status: Started
    2021-01-15 15:34:26 INFO Supervisor demo-computemaximum - Resource version: 562391
    2021-01-15 15:34:31 INFO Supervisor demo-computeaverage - Resource version: 562399
    ...

You can watch the FlinkDeployment resource:

    kubectl -n flink-jobs get fd --watch

    NAME    RESOURCE-STATUS   AGE
    demo    Updated           6m20s

You can watch the FlinkCluster resource:

    kubectl -n flink-jobs get fc --watch

    NAME   RESOURCE-STATUS   SUPERVISOR-STATUS   CLUSTER-HEALTH   TASK-MANAGERS   REQUESTED-TASK-MANAGERS   TASK-MANAGERS-REPLICAS   TOTAL-TASK-SLOTS   SERVICE-MODE   AGE
    demo   Updating          Starting                             0                                         0                        0                  ClusterIP      51s
    demo   Updating          Started                              0                                         0                        0                  ClusterIP      56s
    demo   Updating          Started                              0               2                         0                        0                  ClusterIP      62s

You can watch the FlinkJob resources:

    kubectl -n flink-jobs get fj --watch

    NAME                  RESOURCE-STATUS   SUPERVISOR-STATUS   CLUSTER-NAME   CLUSTER-HEALTH   JOB-STATUS   JOB-ID   JOB-RESTART   JOB-PARALLELISM   REQUESTED-JOB-PARALLELISM   SAVEPOINT-MODE   SAVEPOINT-PATH   SAVEPOINT-AGE   AGE
    demo-computeaverage                                                                                                                               1                                                                             0s
    demo-computemaximum                                                                                                                               1                                                                             0s
    demo-computeaverage                                                                                               Always                          1                           Automatic                                         36s
    demo-computeaverage                                                                                               Always                          1                           Automatic                                         36s
    demo-computemaximum                                                                                               Always                          1                           Automatic                                         36s
    demo-computemaximum                                                                                               Always                          1                           Automatic                                         36s
    demo-computeaverage   Updating          Starting            demo                                                  Always        1                 1                           Automatic                                         41s
    demo-computemaximum   Updating          Starting            demo                                                  Always        1                 1                           Automatic                                         41s
    demo-computeaverage   Updated           Starting            demo           HEALTHY                                Always        1                 1                           Automatic                                         77s
    demo-computemaximum   Updated           Starting            demo           HEALTHY                                Always        1                 1                           Automatic                                         77s
    demo-computemaximum   Updated           Starting            demo           HEALTHY                       c79ec6a54a69260a42ffc92eae559270   Always        1                 1                           Automatic                                         87s
    demo-computemaximum   Updated           Started             demo           HEALTHY                       c79ec6a54a69260a42ffc92eae559270   Always        1                 1                           Automatic                                         87s
    demo-computemaximum   Updated           Started             demo           HEALTHY          RUNNING      c79ec6a54a69260a42ffc92eae559270   Always        1                 1                           Automatic                                         92s
    demo-computeaverage   Updated           Started             demo           HEALTHY          RUNNING      ecc96795e1565d8bf48b39d98f6e909a   Always        1                 1                           Automatic

You can watch the pods:

    kubectl -n flink-jobs get pod --watch

    NAME                                  READY   STATUS              RESTARTS   AGE
    supervisor-demo-5568979d75-d7b5r      1/1     Running             0          1s
    jobmanager-demo-d798s                 0/1     Pending             0          0s
    jobmanager-demo-d798s                 0/1     ContainerCreating   0          0s
    jobmanager-demo-d798s                 1/1     Running             0          1s
    taskmanager-demo-hrf8l                0/1     Pending             0          0s
    taskmanager-demo-lvmzt                0/1     Pending             0          0s
    taskmanager-demo-hrf8l                0/1     ContainerCreating   0          0s
    taskmanager-demo-lvmzt                0/1     ContainerCreating   0          0s
    taskmanager-demo-lvmzt                1/1     Running             0          1s
    taskmanager-demo-hrf8l                1/1     Running             0          2s
    bootstrap-demo-computeaverage-pjzwx   0/1     Pending             0          0s
    bootstrap-demo-computeaverage-pjzwx   0/1     ContainerCreating   0          0s
    bootstrap-demo-computemaximum-cbjhx   0/1     Pending             0          0s
    bootstrap-demo-computemaximum-cbjhx   0/1     ContainerCreating   0          0s
    bootstrap-demo-computeaverage-pjzwx   0/1     Completed           0          9s
    bootstrap-demo-computemaximum-cbjhx   0/1     Completed           0          10s

You can inspect the FlinkDeployment resource:

    kubectl -n flink-jobs get fd demo -o json | jq '.status'

    {
        "digest": {
            "cluster": {
                "jobManager": "CTfL1eq/ae2HDbSRWWkTKg==",
                "runtime": "RMgn5qo3Q+Qy706ghyxeMw==",
                "supervisor": "WtUIhzGMVrfpgNNH8oe/uw==",
                "taskManager": "gQcoI/m7uD7hnM6bjVxtmA=="
            },
            "jobs": [
                {
                    "job": {
                        "bootstrap": "qOety+rnp6ETxBsejANgow==",
                        "restart": "PQvmPQxmz6LjAa8HW+SQPQ==",
                        "savepoint": "oxNouuvcYqYkBWtMOnKjdQ=="
                    },
                    "name": "computeaverage"
                },
                {
                    "job": {
                        "bootstrap": "s/EoBI5x9h7xtvlvOCKMGw==",
                        "restart": "pxeYsVQV2fsVkJfRUQgehw==",
                        "savepoint": "oj/YaUKHUIZHCjgRxVN2wA=="
                    },
                    "name": "computemaximum"
                }
            ]
        },
        "resourceStatus": "Updated",
        "timestamp": "2020-12-16T09:57:38.132Z"
    }

You can inspect the FlinkCluster resource:

    kubectl -n flink-jobs get fc demo -o json | jq '.status'

    {
        "clusterHealth": "HEALTHY",
        "digest": {
            "jobManager": "CTfL1eq/ae2HDbSRWWkTKg==",
            "runtime": "RMgn5qo3Q+Qy706ghyxeMw==",
            "supervisor": "WtUIhzGMVrfpgNNH8oe/uw==",
            "taskManager": "gQcoI/m7uD7hnM6bjVxtmA=="
        },
        "labelSelector": "clusterName=demo,owner=flink-operator,component=flink,role=taskmanager",
        "rescaleTimestamp": "2020-12-16T10:12:55.416Z",
        "resourceStatus": "Updated",
        "serviceMode": "NodePort",
        "supervisorStatus": "Started",
        "taskManagerReplicas": 2,
        "taskManagers": 2,
        "taskSlots": 2,
        "timestamp": "2020-12-16T10:13:07.026Z",
        "totalTaskSlots": 4
    }

You can inspect the FlinkJob resources:

    kubectl -n flink-jobs get fj demo-computeaverage -o json | jq '.status'

    {
        "clusterHealth": "HEALTHY",
        "clusterName": "demo",
        "digest": {
            "bootstrap": "qOety+rnp6ETxBsejANgow==",
            "restart": "PQvmPQxmz6LjAa8HW+SQPQ==",
            "savepoint": "oxNouuvcYqYkBWtMOnKjdQ=="
        },
        "jobId": "82cf4d4e6c0cd4f89b587c74af33f298",
        "jobParallelism": 2,
        "jobStatus": "RUNNING",
        "labelSelector": "clusterName=demo,owner=flink-operator,component=flink,role=taskmanager",
        "resourceStatus": "Updated",
        "restartPolicy": "OnlyIfFailed",
        "savepointJobId": "",
        "savepointMode": "Automatic",
        "savepointPath": "",
        "savepointRequestTimestamp": "2020-12-16T09:58:02.343Z",
        "savepointTriggerId": "",
        "supervisorStatus": "Started",
        "timestamp": "2020-12-16T10:13:30.628Z"
    }

You can expose the JobManager web console:

    kubectl -n flink-jobs port-forward service/jobmanager-demo 8081

then open a browser at http://localhost:8081.

## Control cluster and jobs with annotations

Annotate the cluster resource to stop the cluster:

    kubectl -n flink-jobs annotate fc demo --overwrite operator.nextbreakpoint.com/requested-action=STOP

Annotate the cluster resource to restart the cluster:

    kubectl -n flink-jobs annotate fc demo --overwrite operator.nextbreakpoint.com/requested-action=START

Annotate the job reource to stop the job (but not the cluster):

    kubectl -n flink-jobs annotate fj demo-computeaverage --overwrite operator.nextbreakpoint.com/requested-action=STOP

Annotate the job resource to restart the job (but not the cluster):

    kubectl -n flink-jobs annotate fj demo-computeaverage --overwrite operator.nextbreakpoint.com/requested-action=START

Annotate the job resource to trigger a savepoint:

    kubectl -n flink-jobs annotate fj demo-computeaverage --overwrite operator.nextbreakpoint.com/requested-action=TRIGGER_SAVEPOINT

Annotate the job resource to forget a savepoint:

    kubectl -n flink-jobs annotate fj demo-computeaverage --overwrite operator.nextbreakpoint.com/requested-action=FORGET_SAVEPOINT

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

Expose the control interface to your host:

    kubectl -n flink-operator port-forward service/flink-operator 4444

Get list of deployments:

    curl http://localhost:4444/api/v1/deployments

Get list of clusters:

    curl http://localhost:4444/api/v1/clusters

Get list of jobs:

    curl http://localhost:4444/api/v1/clusters/custer-1/jobs

Get status of a deployment:

    curl http://localhost:4444/api/v1/deployments/demo/status

Get status of a cluster:

    curl http://localhost:4444/api/v1/clusters/demo/status

Get status of a job:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage/status

Get details of a job:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage/details

Get metrics of a job:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage/metrics

Get metrics of the JobManager:

    curl http://localhost:4444/api/v1/clusters/demo/jobmanager/metrics

Get list of TaskManagers:

    curl http://localhost:4444/api/v1/clusters/demo/taskmanagers

Get metrics of a TaskManager:

    curl http://localhost:4444/api/v1/clusters/demo/taskmanagers/67761be7be3c93b44dd037632871c828/metrics

Get details of a TaskManager:

    curl http://localhost:4444/api/v1/clusters/demo/taskmanagers/67761be7be3c93b44dd037632871c828/details

Create a deployment:

    curl http://localhost:4444/api/v1/deployments/demo -XPOST -d@example/deployment-spec.json

Create a cluster:

    curl http://localhost:4444/api/v1/clusters/demo -XPOST -d@example/cluster-spec.json

Create a job:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage -XPOST -d@example/job-spec-1.json

Delete a deployment:

    curl http://localhost:4444/api/v1/deployments/demo -XDELETE

Delete a cluster:

    curl http://localhost:4444/api/v1/clusters/demo -XDELETE

Delete a job:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage -XDELETE

Update a deployment:

    curl http://localhost:4444/api/v1/deployments/demo -XPUT -d@example/deployment-spec.json

Update a cluster:

    curl http://localhost:4444/api/v1/clusters/demo -XPUT -d@example/cluster-spec.json

Update a job:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage -XPUT -d@example/job-spec-1.json

Start a cluster:

    curl http://localhost:4444/api/v1/clusters/demo/start -XPUT -d'{"withoutSavepoint":false}'

Stop a cluster:

    curl http://localhost:4444/api/v1/clusters/demo/stop -XPUT -d'{"withoutSavepoint":false}'

Scale a cluster:

    curl http://localhost:4444/api/v1/clusters/demo/scale -XPUT -d'{"taskManagers":4}'

Start a job:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage/start -XPUT -d'{"withoutSavepoint":false}'

Stop a job:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage/stop -XPUT -d'{"withoutSavepoint":false}'

Scale a job:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage/scale -XPUT -d'{"parallelism":2}'

Trigger a savepoint:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage/savepoint/trigger -XPUT

Forget a savepoint:

    curl http://localhost:4444/api/v1/clusters/demo/jobs/computeaverage/savepoint/forget -XPUT

Please note that you must provide the SSL certificates when the operator API is secured with TLS (see instructions for generating SSL certificates above):

    curl --cacert secrets/ca_cert.pem --cert secrets/operator-cli_cert.pem --key secrets/operator-cli_key.pem https://localhost:4444/api/v1/clusters/<name>/status

## Control cluster and jobs using flinkctl

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

Expose the operator using an external address:

    kubectl -n flink-operator expose service flink-operator --name=flink-operator-external --port=4444 --target-port=4444 --external-ip=$(minikube ip)

Get the list of deployments:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 deployments list --host=$(minikube ip) | jq -r '.output' | jq

Get the list of clusters:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 clusters list --host=$(minikube ip) | jq -r '.output' | jq

Get the list of jobs:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 jobs list --cluster-name=demo --host=$(minikube ip) | jq -r '.output' | jq

Get the status of a deployment:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 deployment status --deployment-name=demo --host=$(minikube ip) | jq -r '.output' | jq

Get the status of a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster status --cluster-name=demo --host=$(minikube ip) | jq -r '.output' | jq

Get the status of a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job status --cluster-name=demo --job-name=computeaverage --host=$(minikube ip) | jq -r '.output' | jq

Delete a deployment:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 deployment delete --deployment-name=demo --host=$(minikube ip)

Delete a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster delete --cluster-name=demo --host=$(minikube ip)

Delete a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job delete --cluster-name=demo --job-name=computeaverage --host=$(minikube ip)

Stop a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster stop --cluster-name=demo --host=$(minikube ip)

Start a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster start --cluster-name=demo --host=$(minikube ip)

Stop a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job stop --cluster-name=demo --job-name=computeaverage --host=$(minikube ip)

Start a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job start --cluster-name=demo --job-name=computeaverage --host=$(minikube ip)

Start a cluster without recovering from the savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster start --cluster-name=demo --without-savepoint --host=$(minikube ip)

Stop a cluster without creating a new savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster stop --cluster-name=demo --without-savepoint --host=$(minikube ip)

Start a job without recovering from the savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job start --cluster-name=demo --job-name=computeaverage --without-savepoint --host=$(minikube ip)

Stop a job without creating a new savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job stop --cluster-name=demo --job-name=computeaverage --without-savepoint --host=$(minikube ip)

Create a new savepoint:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 savepoint trigger --cluster-name=demo --job-name=computeaverage --host=$(minikube ip)

Remove savepoint from job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 savepoint forget --cluster-name=demo --job-name=computeaverage --host=$(minikube ip)

Rescale a cluster:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 cluster scale --cluster-name=demo --task-managers=4 --host=$(minikube ip)

Rescale a job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job scale --cluster-name=demo --job-name=computeaverage --parallelism=2 --host=$(minikube ip)

Get the details of the job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job details --cluster-name=demo --job-name=computeaverage --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of the job:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 job metrics --cluster-name=demo --job-name=computeaverage --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of the JobManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 jobmanager metrics --cluster-name=demo --host=$(minikube ip) | jq -r '.output' | jq

Get a list of TaskManagers:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 taskmanagers list --cluster-name=demo --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of a TaskManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 taskmanager metrics --cluster-name=demo --taskmanager-id=67761be7be3c93b44dd037632871c828 --host=$(minikube ip) | jq -r '.output' | jq

Get the details of a TaskManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 taskmanager details --cluster-name=demo --taskmanager-id=67761be7be3c93b44dd037632871c828 --host=$(minikube ip) | jq -r '.output' | jq

Get the metrics of a TaskManager:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 taskmanager metrics --cluster-name=demo --taskmanager-id=67761be7be3c93b44dd037632871c828 --host=$(minikube ip) | jq -r '.output' | jq

Please note that you must provide the SSL certificates when the operator API is secured with TLS (see instructions for generating SSL certificates above):

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

    flinkctl supervisor run --namespace=flink-jobs --cluster-name=demo --task-timeout=180 --polling-interval=5

### Bootstrap process

The bootstrap process is required to upload the JAR to the JobManager and run the job:

    flinkctl bootstrap run --namespace=flink-jobs --cluster-name=demo --job-name=computeaverage --class-name=your-main-class --jar-path=/your-job-jar.jar --parallelism=2

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

## Developers instructions

This section provides the instructions for building and running the flinkctl command.

### Build with Maven

Install Java 21 and Maven 3.8.

Compile and package the code with command:

    mvn clean package

### Build with Docker

Build a Docker image with command:

    docker build -t nextbreakpoint/flinkctl:1.5.0 .

Test the image printing the CLI usage:

    docker run --rm -it nextbreakpoint/flinkctl:1.5.0 --help

Tag and push the image to your Docker registry if needed:

    docker tag nextbreakpoint/flinkctl:1.5.0 some-registry/flinkctl:1.5.0
    docker login some-registry
    docker push some-registry/flinkctl:1.5.0

### Run the application

The command flinkctl can be executed directly on any Linux machine or indirectly from a Docker container on any system which supports Docker.
Alternatively, you can run the Java code with the java command. The application can be executed and debugged as any Java application.

The application can be configured to point to a local or remote Kubernetes cluster, and it works with Minikube and Docker for Desktop too.

Run the operator with a given namespace and Kubernetes config on Linux:

    flinkctl operator run --namespace=test --kube-config=~/.kube/config

Run the operator with a given namespace and Kubernetes config using Docker:

    docker run --rm -it -v ~/.kube/config:/kube/config nextbreakpoint/flinkctl:1.5.0 operator run --namespace=test --kube-config=/kube/config

Run the operator with a given namespace and Kubernetes config with java command:

    java --module-path target/com.nextbreakpoint.flink.control-1.5.0-shaded.jar --module com.nextbreakpoint.flink.control/com.nextbreakpoint.flink.controller.cli.Main operator run --namespace=test --kube-config=~/.kube/config

Run the operator with a given namespace and Kubernetes config and with debug agent enabled:

    java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5555 --module-path target/com.nextbreakpoint.flink.control-1.5.0-shaded.jar --module com.nextbreakpoint.flink.control/com.nextbreakpoint.flink.controller.cli.Main operator run --namespace=test --kube-config=~/.kube/config

### Run automated tests

Run unit tests with command:

    mvn test

Run integration tests against Docker for Desktop or Minikube with command:

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

### Configure automatic savepoints

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
