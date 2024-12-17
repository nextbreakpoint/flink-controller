package com.nextbreakpoint.flink.control.k8s.common

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.flink.control.common.FlinkAddress
import com.nextbreakpoint.flink.control.common.FlinkOptions
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkCluster
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkClusterList
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkClusterStatus
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkDeployment
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkDeploymentList
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkDeploymentStatus
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkJob
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkJobList
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkJobStatus
import com.nextbreakpoint.flink.control.k8s.kube.Register
import io.kubernetes.client.custom.V1Patch
import io.kubernetes.client.informer.SharedIndexInformer
import io.kubernetes.client.informer.SharedInformerFactory
import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.apis.AppsV1Api
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.apis.CustomObjectsApi
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1DeploymentList
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1JobList
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.openapi.models.V1PodList
import io.kubernetes.client.openapi.models.V1Service
import io.kubernetes.client.openapi.models.V1ServiceList
import io.kubernetes.client.util.CallGeneratorParams
import io.kubernetes.client.util.Config
import io.kubernetes.client.util.PatchUtils
import io.kubernetes.client.util.Watch
import io.kubernetes.client.util.Watchable
import java.io.File
import java.io.FileInputStream
import java.util.logging.Level
import java.util.logging.Logger

object KubeClient {
    private val logger = Logger.getLogger(KubeClient::class.simpleName)

    private val objectApi = CustomObjectsApi()
    private val batchApi = BatchV1Api()
    private val coreApi = CoreV1Api()
    private val appsApi = AppsV1Api()

    private val objectApiWatch = CustomObjectsApi()
    private val batchApiWatch = BatchV1Api()
    private val coreApiWatch = CoreV1Api()
    private val appsApiWatch = AppsV1Api()

    private var apiClient: ApiClient? = null

    fun configure(kubeConfig: String?) {
        Configuration.setDefaultApiClient(createKubernetesApiClient(kubeConfig, 20000))
        apiClient = createKubernetesApiClient(kubeConfig, 0)
        objectApi.apiClient = Configuration.getDefaultApiClient()
        batchApi.apiClient = Configuration.getDefaultApiClient()
        coreApi.apiClient = Configuration.getDefaultApiClient()
        appsApi.apiClient = Configuration.getDefaultApiClient()
        objectApiWatch.apiClient = apiClient
        batchApiWatch.apiClient = apiClient
        coreApiWatch.apiClient = apiClient
        appsApiWatch.apiClient = apiClient
        Register.registerCustomTypes()
    }

    fun createSharedInformerFactory() = SharedInformerFactory(apiClient)

    fun findFlinkAddress(flinkOptions: FlinkOptions, namespace: String, name: String): FlinkAddress {
        try {
            var jobmanagerHost = flinkOptions.hostname ?: "localhost"
            var jobmanagerPort = flinkOptions.portForward ?: 8081

            if (flinkOptions.hostname == null && flinkOptions.portForward == null && flinkOptions.useNodePort) {
                val nodes = coreApi.listNode()
                    .timeoutSeconds(30)
                    .execute()

                jobmanagerHost = nodes.items.flatMap {
                    it.status?.addresses.orEmpty()
                }.filter {
                    it.type == "InternalIP"
                }.map {
                    it.address
                }.firstOrNull() ?: throw NotFoundException("Node not found")
            }

            if (flinkOptions.portForward == null) {
                val service = coreApi.readNamespacedService("jobmanager-$name", namespace).execute()

                if (flinkOptions.useNodePort) {
                    service.spec?.ports?.filter {
                        it.name.equals("ui")
                    }?.filter {
                        it.nodePort != null
                    }?.map {
                        it.nodePort
                    }?.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                } else {
                    service.spec?.ports?.filter {
                        it.name.equals("ui")
                    }?.filter {
                        it.port != null
                    }?.map {
                        it.port
                    }?.firstOrNull()?.let {
                        jobmanagerPort = it
                    }
                    jobmanagerHost = service.spec?.clusterIP ?: ""
                }
            }

            logger.log(Level.FINE, "Flink client created for host $jobmanagerHost and port $jobmanagerPort")

            return FlinkAddress(
                host = jobmanagerHost,
                port = jobmanagerPort
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't connect to jobmanager: ${e.responseBody}")
            throw e
        }
    }

    fun getFlinkDeployment(namespace: String, name: String): V1FlinkDeployment {
        try {
            return Register.flinkDeploymentKubernetesApi(apiClient).get(namespace, name).`object`
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't fetch custom object: ${e.responseBody}")
            throw e
        }
    }

    fun getFlinkCluster(namespace: String, name: String): V1FlinkCluster {
        try {
            return Register.flinkClusterKubernetesApi(apiClient).get(namespace, name).`object`
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't fetch custom object: ${e.responseBody}")
            throw e
        }
    }

    fun getFlinkJob(namespace: String, name: String): V1FlinkJob {
        try {
            return Register.flinkJobKubernetesApi(apiClient).get(namespace, name).`object`
        } catch (e: ApiException) {
            logger.log(Level.SEVERE, "Can't fetch custom object: ${e.responseBody}")
            throw e
        }
    }

    fun updateDeploymentAnnotations(namespace: String, name: String, annotations: Map<String, String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "annotations" to annotations
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkDeployment::class.java, {
                    objectApi.patchNamespacedCustomObject(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkdeployments",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update annotations of deployment ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateClusterAnnotations(namespace: String, name: String, annotations: Map<String, String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "annotations" to annotations
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObject(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update annotations of cluster ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateJobAnnotations(namespace: String, name: String, annotations: Map<String, String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "annotations" to annotations
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObject(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update annotations of job ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateDeploymentFinalizers(namespace: String, name: String, finalizers: List<String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "finalizers" to finalizers
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkDeployment::class.java, {
                    objectApi.patchNamespacedCustomObject(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkdeployments",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update finalizers of deployment ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateClusterFinalizers(namespace: String, name: String, finalizers: List<String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "finalizers" to finalizers
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObject(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update finalizers of cluster ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateJobFinalizers(namespace: String, name: String, finalizers: List<String>) {
        val patch = mapOf<String, Any?>(
            "metadata" to mapOf<String, Any?>(
                "finalizers" to finalizers
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObject(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update finalizers of job ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateDeploymentStatus(namespace: String, name: String, status: V1FlinkDeploymentStatus) {
        val patch = V1FlinkDeployment.builder().withStatus(status).build()

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectStatus(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkdeployments",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update status of deployment ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateClusterStatus(namespace: String, name: String, status: V1FlinkClusterStatus) {
        val patch = V1FlinkCluster.builder().withStatus(status).build()

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectStatus(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update status of cluster ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun updateJobStatus(namespace: String, name: String, status: V1FlinkJobStatus) {
        val patch = V1FlinkJob.builder().withStatus(status).build()

        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObjectStatus(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update status of job ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun rescaleCluster(namespace: String, name: String, taskManagers: Int) {
        val patch = mapOf<String, Any?>(
            "spec" to mapOf<String, Any?>(
                "replicas" to taskManagers
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObjectScale(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't modify task managers of cluster ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun rescaleJob(namespace: String, name: String, parallelism: Int) {
        val patch = mapOf<String, Any?>(
            "spec" to mapOf<String, Any?>(
                "replicas" to parallelism
            )
        )

        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObjectScale(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        name,
                        patch
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't modify parallelism of job ${name}: ${e.responseBody}")
            throw e
        }
    }

    fun createFlinkDeploymentsInformer(factory: SharedInformerFactory, namespace: String): SharedIndexInformer<V1FlinkDeployment> =
        factory.sharedIndexInformerFor(
            { params: CallGeneratorParams ->
                objectApiWatch.listNamespacedCustomObject(
                    "nextbreakpoint.com",
                    "v1",
                    namespace,
                    "flinkdeployments"
                )
                .resourceVersion(params.resourceVersion)
                .timeoutSeconds(params.timeoutSeconds)
                .watch(params.watch)
                .buildCall(null)
            },
            V1FlinkDeployment::class.java,
            V1FlinkDeploymentList::class.java
        )

    fun createFlinkClustersInformer(factory: SharedInformerFactory, namespace: String): SharedIndexInformer<V1FlinkCluster> =
        factory.sharedIndexInformerFor(
            { params: CallGeneratorParams ->
                objectApiWatch.listNamespacedCustomObject(
                    "nextbreakpoint.com",
                    "v1",
                    namespace,
                    "flinkclusters"
                )
                .resourceVersion(params.resourceVersion)
                .timeoutSeconds(params.timeoutSeconds)
                .watch(params.watch)
                .buildCall(null)
            },
            V1FlinkCluster::class.java,
            V1FlinkClusterList::class.java
        )

    fun createFlinkJobsInformer(factory: SharedInformerFactory, namespace: String): SharedIndexInformer<V1FlinkJob> =
        factory.sharedIndexInformerFor(
            { params: CallGeneratorParams ->
                objectApiWatch.listNamespacedCustomObject(
                    "nextbreakpoint.com",
                    "v1",
                    namespace,
                    "flinkjobs",
                )
                .resourceVersion(params.resourceVersion)
                .timeoutSeconds(params.timeoutSeconds)
                .watch(params.watch)
                .buildCall(null)
            },
            V1FlinkJob::class.java,
            V1FlinkJobList::class.java
        )

    fun createServicesInformer(factory: SharedInformerFactory, namespace: String): SharedIndexInformer<V1Service> =
        factory.sharedIndexInformerFor(
            { params: CallGeneratorParams ->
                coreApiWatch.listNamespacedService(
                    namespace
                )
                .labelSelector("component=flink,owner=flink-operator")
                .resourceVersion(params.resourceVersion)
                .timeoutSeconds(params.timeoutSeconds)
                .watch(params.watch)
                .buildCall(null)
            },
            V1Service::class.java,
            V1ServiceList::class.java
        )

    fun createDeploymentsInformer(factory: SharedInformerFactory, namespace: String): SharedIndexInformer<V1Deployment> =
        factory.sharedIndexInformerFor(
            { params: CallGeneratorParams ->
                appsApiWatch.listNamespacedDeployment(
                    namespace
                )
                .labelSelector("component=flink,owner=flink-operator")
                .resourceVersion(params.resourceVersion)
                .timeoutSeconds(params.timeoutSeconds)
                .watch(params.watch)
                .buildCall(null)
            },
            V1Deployment::class.java,
            V1DeploymentList::class.java
        )

    fun createJobsInformer(factory: SharedInformerFactory, namespace: String): SharedIndexInformer<V1Job> =
        factory.sharedIndexInformerFor(
            { params: CallGeneratorParams ->
                batchApiWatch.listNamespacedJob(
                    namespace
                )
                .labelSelector("component=flink,owner=flink-operator")
                .resourceVersion(params.resourceVersion)
                .timeoutSeconds(params.timeoutSeconds)
                .watch(params.watch)
                .buildCall(null)
            },
            V1Job::class.java,
            V1JobList::class.java
        )

    fun createPodsInformer(factory: SharedInformerFactory, namespace: String): SharedIndexInformer<V1Pod> =
        factory.sharedIndexInformerFor(
            { params: CallGeneratorParams ->
                coreApiWatch.listNamespacedPod(
                    namespace
                )
                .labelSelector("component=flink,owner=flink-operator")
                .resourceVersion(params.resourceVersion)
                .timeoutSeconds(params.timeoutSeconds)
                .watch(params.watch)
                .buildCall(null)
            },
            V1Pod::class.java,
            V1PodList::class.java
        )

    fun watchFlinkDeployments(namespace: String): Watchable<V1FlinkDeployment> =
        Watch.createWatch(
            objectApiWatch.apiClient,
            objectApiWatch.listNamespacedCustomObject(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkdeployments"
            )
            .timeoutSeconds(600)
            .watch(true)
            .buildCall(null),
            object : TypeToken<Watch.Response<V1FlinkDeployment>>() {}.type
        )

    fun watchFlinkClusters(namespace: String): Watchable<V1FlinkCluster> =
        Watch.createWatch(
            objectApiWatch.apiClient,
            objectApiWatch.listNamespacedCustomObject(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
            )
            .timeoutSeconds(600)
            .watch(true)
            .buildCall(null),
            object : TypeToken<Watch.Response<V1FlinkCluster>>() {}.type
        )

    fun watchFlinkJobs(namespace: String): Watchable<V1FlinkJob> =
        Watch.createWatch(
            objectApiWatch.apiClient,
            objectApiWatch.listNamespacedCustomObject(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkjobs",
            )
            .timeoutSeconds(600)
            .watch(true)
            .buildCall(null),
            object : TypeToken<Watch.Response<V1FlinkJob>>() {}.type
        )

    fun watchServices(namespace: String): Watchable<V1Service> =
        Watch.createWatch(
            coreApiWatch.apiClient,
            coreApiWatch.listNamespacedService(
                namespace
            )
            .labelSelector("component=flink,owner=flink-operator")
            .timeoutSeconds(600)
            .watch(true)
            .buildCall(null),
            object : TypeToken<Watch.Response<V1Service>>() {}.type
        )

    fun watchDeployments(namespace: String): Watchable<V1Deployment> =
        Watch.createWatch(
            appsApiWatch.apiClient,
            appsApiWatch.listNamespacedDeployment(
                namespace,
            )
            .labelSelector("component=flink,owner=flink-operator")
            .timeoutSeconds(600)
            .watch(true)
            .buildCall(null),
            object : TypeToken<Watch.Response<V1Deployment>>() {}.type
        )

    fun watchJobs(namespace: String): Watchable<V1Job> =
        Watch.createWatch(
            batchApiWatch.apiClient,
            batchApiWatch.listNamespacedJob(
                namespace,
            )
            .labelSelector("component=flink,owner=flink-operator")
            .timeoutSeconds(600)
            .watch(true)
            .buildCall(null),
            object : TypeToken<Watch.Response<V1Job>>() {}.type
        )

    fun watchPods(namespace: String): Watchable<V1Pod> =
        Watch.createWatch(
            coreApiWatch.apiClient,
            coreApiWatch.listNamespacedPod(
                namespace,
            )
            .labelSelector("component=flink,owner=flink-operator")
            .timeoutSeconds(600)
            .watch(true)
            .buildCall(null),
            object : TypeToken<Watch.Response<V1Pod>>() {}.type
        )

    fun createService(namespace: String, resource: V1Service): V1Service {
        try {
             return coreApi.createNamespacedService(namespace, resource).execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create service: ${e.responseBody}")
            throw e
        }
    }

    fun deleteService(namespace: String, name: String) {
        try {
            coreApi.deleteNamespacedService(name, namespace)
                .propagationPolicy("Background")
                .gracePeriodSeconds(5)
                .execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete service: ${e.responseBody}")
            throw e
        }
    }

    fun createPod(namespace: String, resource: V1Pod): V1Pod {
        try {
            return coreApi.createNamespacedPod(namespace, resource).execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create pod: ${e.responseBody}")
            throw e
        }
    }

    fun deletePod(namespace: String, name: String) {
        try {
            coreApi.deleteNamespacedPod(name, namespace)
                .propagationPolicy("Background")
                .gracePeriodSeconds(5)
                .execute()
        } catch (e: ApiException) {
            logger.log(Level.SEVERE, "Can't delete pod: ${e.responseBody}")
            throw e
        }
    }

    fun createFlinkDeployment(namespace: String, resource: V1FlinkDeployment) {
        try {
            objectApi.createNamespacedCustomObject(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkdeployments",
                resource
            ).execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create flinkdeployment: ${e.responseBody}")
            throw e
        }
    }

    fun deleteFlinkDeployment(namespace: String, name: String) {
        try {
            objectApi.deleteNamespacedCustomObject(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkdeployments",
                name
            )
            .propagationPolicy("Background")
            .gracePeriodSeconds(5)
            .execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete flinkdeployment: ${e.responseBody}")
            throw e
        }
    }

    fun updateFlinkDeployment(namespace: String, resource: V1FlinkDeployment) {
        try {
            PatchUtils.patch(
                V1FlinkDeployment::class.java, {
                    objectApi.patchNamespacedCustomObject(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkdeployments",
                        resource.metadata.name,
                        resource
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update flinkdeployment: ${e.responseBody}")
            throw e
        }
    }

    fun createFlinkCluster(namespace: String, resource: V1FlinkCluster) {
        try {
            objectApi.createNamespacedCustomObject(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                resource
            ).execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create flinkcluster: ${e.responseBody}")
            throw e
        }
    }

    fun deleteFlinkCluster(namespace: String, name: String) {
        try {
            objectApi.deleteNamespacedCustomObject(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                name
            )
            .propagationPolicy("Background")
            .gracePeriodSeconds(5)
            .execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete flinkcluster: ${e.responseBody}")
            throw e
        }
    }

    fun updateFlinkCluster(namespace: String, resource: V1FlinkCluster) {
        try {
            PatchUtils.patch(
                V1FlinkCluster::class.java, {
                    objectApi.patchNamespacedCustomObject(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkclusters",
                        resource.metadata.name,
                        resource
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update flinkcluster: ${e.responseBody}")
            throw e
        }
    }

    fun createFlinkJob(namespace: String, resource: V1FlinkJob) {
        try {
            objectApi.createNamespacedCustomObject(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkjobs",
                resource
            ).execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create flinkjob: ${e.responseBody}")
            throw e
        }
    }

    fun deleteFlinkJob(namespace: String, name: String) {
        try {
            objectApi.deleteNamespacedCustomObject(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkjobs",
                name
            )
            .propagationPolicy("Background")
            .gracePeriodSeconds(5)
            .execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete flinkjob: ${e.responseBody}")
            throw e
        }
    }

    fun updateFlinkJob(namespace: String, resource: V1FlinkJob) {
        try {
            PatchUtils.patch(
                V1FlinkJob::class.java, {
                    objectApi.patchNamespacedCustomObject(
                        "nextbreakpoint.com",
                        "v1",
                        namespace,
                        "flinkjobs",
                        resource.metadata.name,
                        resource
                    ).buildCall(null)
                },
                V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
                objectApi.apiClient
            )
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't update flinkjob: ${e.responseBody}")
            throw e
        }
    }

    fun createJob(namespace: String, resource: V1Job): V1Job {
        try {
            return batchApi.createNamespacedJob(namespace, resource).execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create job: ${e.responseBody}")
            throw e
        }
    }

    fun deleteJob(namespace: String, name: String) {
        try {
            batchApi.deleteNamespacedJob(name, namespace)
                .propagationPolicy("Background")
                .gracePeriodSeconds(5)
                .execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete job: ${e.responseBody}")
            throw e
        }
    }

    fun createDeployment(namespace: String, resource: V1Deployment): V1Deployment {
        try {
            return appsApi.createNamespacedDeployment(namespace, resource).execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't create deployment: ${e.responseBody}")
            throw e
        }
    }

    fun deleteDeployment(namespace: String, name: String) {
        try {
            appsApi.deleteNamespacedDeployment(name, namespace)
                .propagationPolicy("Background")
                .gracePeriodSeconds(5)
                .execute()
        } catch (e : ApiException) {
            logger.log(Level.SEVERE, "Can't delete deployment: ${e.responseBody}")
            throw e
        }
    }

    private fun createKubernetesApiClient(kubeConfig: String?, timeout: Int): ApiClient {
        val client = if (kubeConfig?.isNotBlank() == true) Config.fromConfig(FileInputStream(File(kubeConfig))) else Config.fromCluster()
        client.connectTimeout = timeout
        client.writeTimeout = timeout
        client.readTimeout = timeout
        client.isDebugging = System.getProperty("kubernetes.client.debugging", "false")!!.toBoolean()
        return client
    }
}
