package com.nextbreakpoint.flink.controller.k8s.factory;

import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkClusterSpec;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1HTTPGetAction;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1LocalObjectReference;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodAffinityTerm;
import io.kubernetes.client.openapi.models.V1PodAntiAffinity;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1Probe;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceBuilder;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1WeightedPodAffinityTerm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClusterResourcesDefaultFactory implements ClusterResourcesFactory {
    @Override
    public V1Service createService(String namespace, String owner, String clusterName, V1FlinkClusterSpec clusterSpec) {
        var serviceLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "component", "flink",
                "role", "jobmanager"
        );

        var srvPort8081 =
                createServicePort(
                        8081,
                        "ui"
                );

        var srvPort6123 =
                createServicePort(
                        6123,
                        "rpc"
                );

        var srvPort6124 =
                createServicePort(
                        6124,
                        "blob"
                );

        var srvPort6125 =
                createServicePort(
                        6125,
                        "query"
                );

        var serviceType = getServiceType(clusterSpec);

        return new V1ServiceBuilder()
                .editOrNewMetadata()
                .withName("jobmanager-" + clusterName)
                .withLabels(serviceLabels)
                .endMetadata()
                .editOrNewSpec()
                .addToPorts(srvPort8081)
                .addToPorts(srvPort6123)
                .addToPorts(srvPort6124)
                .addToPorts(srvPort6125)
                .withSelector(serviceLabels)
                .withType(serviceType)
                .endSpec()
                .build();
    }

    @Override
    public V1Pod createJobManagerPod(String namespace, String owner, String clusterName, V1FlinkClusterSpec clusterSpec) {
        if (clusterSpec.getRuntime() == null || clusterSpec.getRuntime().getImage() == null) {
            throw new RuntimeException("flinkImage is required");
        }

        var jobmanagerLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "component", "flink",
                "role", "jobmanager"
        );

        var taskmanagerLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "component", "flink",
                "role", "taskmanager"
        );

        var port8081 =
                createContainerPort(
                        8081,
                        "ui"
                );

        var port6123 =
                createContainerPort(
                        6123,
                        "rpc"
                );

        var port6124 =
                createContainerPort(
                        6124,
                        "blob"
                );

        var port6125 =
                createContainerPort(
                        6125,
                        "query"
                );

        var podNameEnvVar =
                createEnvVarFromField(
                        "POD_NAME", "metadata.name"
                );

        var podNamespaceEnvVar =
                createEnvVarFromField(
                        "POD_NAMESPACE", "metadata.namespace"
                );

        var rpcAddressEnvVar =
                createEnvVar(
                        "JOB_MANAGER_RPC_ADDRESS", "jobmanager-" + clusterName
                );

        var jobmanagerSelector = new V1LabelSelector().matchLabels(jobmanagerLabels);

        var taskmanagerSelector = new V1LabelSelector().matchLabels(taskmanagerLabels);

        var jobmanagerAffinity =
                createAffinity(
                        jobmanagerSelector, taskmanagerSelector
                );

        var jobmanagerVariables = new ArrayList<>(
                List.of(
                    podNameEnvVar,
                    podNamespaceEnvVar,
                    rpcAddressEnvVar
                )
        );

        if (clusterSpec.getJobManager() != null && clusterSpec.getJobManager().getEnvironment() != null) {
            jobmanagerVariables.addAll(clusterSpec.getJobManager().getEnvironment());
        }

        var jobmanagerContainer = new V1ContainerBuilder()
                .withImage(clusterSpec.getRuntime().getImage())
                .withImagePullPolicy(clusterSpec.getRuntime().getPullPolicy() != null ? clusterSpec.getRuntime().getPullPolicy() : "IfNotPresent")
                .withName("jobmanager")
                .withCommand(clusterSpec.getJobManager() != null ? clusterSpec.getJobManager().getCommand() : null)
                .withArgs(clusterSpec.getJobManager() != null && clusterSpec.getJobManager().getArgs() != null ? clusterSpec.getJobManager().getArgs() : List.of("jobmanager"))
                .addToPorts(port8081)
                .addToPorts(port6123)
                .addToPorts(port6124)
                .addToPorts(port6125)
                .addAllToPorts(clusterSpec.getJobManager() != null && clusterSpec.getJobManager().getExtraPorts() != null ? clusterSpec.getJobManager().getExtraPorts() : List.of())
                .addAllToVolumeMounts(clusterSpec.getJobManager() != null && clusterSpec.getJobManager().getVolumeMounts() != null ? clusterSpec.getJobManager().getVolumeMounts() : List.of())
                .withEnv(jobmanagerVariables)
                .withEnvFrom(clusterSpec.getJobManager() != null ? clusterSpec.getJobManager().getEnvironmentFrom() : null)
                .withResources(clusterSpec.getJobManager() != null ? clusterSpec.getJobManager().getResources() : null)
//            .withReadinessProbe(
//                new V1Probe()
//                    .httpGet(new V1HTTPGetAction().port(new IntOrString(8081)).path("/overview"))
//                    .initialDelaySeconds(15)
//                    .periodSeconds(30)
//                    .failureThreshold(3)
//                    .successThreshold(1)
//            )
                .withLivenessProbe(
                        new V1Probe()
                                .httpGet(new V1HTTPGetAction().port(new IntOrString(8081)).path("/overview"))
                                .initialDelaySeconds(15)
                                .periodSeconds(30)
                                .failureThreshold(3)
                                .successThreshold(1)
                )
                .build();

        var jobmanagerPullSecrets = createObjectReferenceListOrNull(clusterSpec.getRuntime().getPullSecrets());

        var initContainers = clusterSpec.getJobManager() != null && clusterSpec.getJobManager().getInitContainers() != null ? clusterSpec.getJobManager().getInitContainers() : List.<V1Container>of();

        var sideContainers = clusterSpec.getJobManager() != null && clusterSpec.getJobManager().getSideContainers() != null ? clusterSpec.getJobManager().getSideContainers() : List.<V1Container>of();

        var jobmanagerMetadata =
                createObjectMeta(
                        "jobmanager-" + clusterName + "-", jobmanagerLabels
                );

        jobmanagerMetadata.setAnnotations(clusterSpec.getJobManager() != null ? clusterSpec.getJobManager().getAnnotations() : null);

        return new V1PodBuilder()
                .withMetadata(jobmanagerMetadata)
                .editOrNewSpec()
                .addAllToInitContainers(initContainers)
                .addToContainers(jobmanagerContainer)
                .addAllToContainers(sideContainers)
                .withServiceAccountName(clusterSpec.getJobManager() != null && clusterSpec.getJobManager().getServiceAccount() != null ? clusterSpec.getJobManager().getServiceAccount() : "default")
                .withImagePullSecrets(jobmanagerPullSecrets)
                .withVolumes(clusterSpec.getJobManager() != null ? clusterSpec.getJobManager().getVolumes() : null)
                .withAffinity(clusterSpec.getJobManager() != null && clusterSpec.getJobManager().getAffinity() != null ? clusterSpec.getJobManager().getAffinity() : jobmanagerAffinity)
                .withTolerations(clusterSpec.getJobManager() != null ? clusterSpec.getJobManager().getTolerations() : null)
                .withTopologySpreadConstraints(clusterSpec.getJobManager() != null ? clusterSpec.getJobManager().getTopologySpreadConstraints() : null)
                .withOverhead(null)
                .endSpec()
                .build();
    }

    @Override
    public V1Pod createTaskManagerPod(String namespace, String owner, String clusterName, V1FlinkClusterSpec clusterSpec) {
        if (clusterSpec.getRuntime() == null || clusterSpec.getRuntime().getImage() == null) {
            throw new RuntimeException("flinkImage is required");
        }

        var jobmanagerLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "component", "flink",
                "role", "jobmanager"
        );

        var taskmanagerLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "component", "flink",
                "role", "taskmanager"
        );

        var port6121 =
                createContainerPort(
                        6121,
                        "data"
                );

        var port6122 =
                createContainerPort(
                        6122,
                        "ipc"
                );

        var podNameEnvVar =
                createEnvVarFromField(
                        "POD_NAME", "metadata.name"
                );

        var podNamespaceEnvVar =
                createEnvVarFromField(
                        "POD_NAMESPACE", "metadata.namespace"
                );

        var rpcAddressEnvVar =
                createEnvVar(
                        "JOB_MANAGER_RPC_ADDRESS", "jobmanager-" + clusterName
                );

        var numberOfTaskSlotsEnvVar =
                createEnvVar(
                        "TASK_MANAGER_NUMBER_OF_TASK_SLOTS", (clusterSpec.getTaskManager() != null && clusterSpec.getTaskManager().getTaskSlots() != null) ? clusterSpec.getTaskManager().getTaskSlots().toString() : "1"
                );

        var jobmanagerSelector = new V1LabelSelector().matchLabels(jobmanagerLabels);

        var taskmanagerSelector = new V1LabelSelector().matchLabels(taskmanagerLabels);

        var taskmanagerVariables = new ArrayList<>(
                List.of(
                    podNameEnvVar,
                    podNamespaceEnvVar,
                    rpcAddressEnvVar,
                    numberOfTaskSlotsEnvVar
                )
        );

        if (clusterSpec.getTaskManager() != null && clusterSpec.getTaskManager().getEnvironment() != null) {
            taskmanagerVariables.addAll(clusterSpec.getTaskManager().getEnvironment());
        }

        var taskmanagerContainer = new V1ContainerBuilder()
                .withImage(clusterSpec.getRuntime().getImage())
                .withImagePullPolicy(clusterSpec.getRuntime().getPullPolicy() != null ? clusterSpec.getRuntime().getPullPolicy() : "IfNotPresent")
                .withName("taskmanager")
                .withCommand(clusterSpec.getTaskManager() != null ? clusterSpec.getTaskManager().getCommand() : null)
                .withArgs(clusterSpec.getTaskManager() != null && clusterSpec.getTaskManager().getArgs() != null ? clusterSpec.getTaskManager().getArgs() : List.of("taskmanager"))
                .addToPorts(port6121)
                .addToPorts(port6122)
                .addAllToPorts(clusterSpec.getTaskManager() != null && clusterSpec.getTaskManager().getExtraPorts() != null ? clusterSpec.getTaskManager().getExtraPorts() : List.of())
                .addAllToVolumeMounts(clusterSpec.getTaskManager() != null && clusterSpec.getTaskManager().getVolumeMounts() != null ? clusterSpec.getTaskManager().getVolumeMounts() : List.of())
                .withEnv(taskmanagerVariables)
                .withEnvFrom(clusterSpec.getTaskManager() != null ? clusterSpec.getTaskManager().getEnvironmentFrom() : null)
                .withResources(clusterSpec.getTaskManager() != null ? clusterSpec.getTaskManager().getResources() : null)
                .build();

        var taskmanagerAffinity =
                createAffinity(
                        jobmanagerSelector, taskmanagerSelector
                );

        var taskmanagerPullSecrets = createObjectReferenceListOrNull(clusterSpec.getRuntime().getPullSecrets());

        var initContainers = clusterSpec.getTaskManager() != null && clusterSpec.getTaskManager().getInitContainers() != null ? clusterSpec.getTaskManager().getInitContainers() : List.<V1Container>of();

        var sideContainers = clusterSpec.getTaskManager() != null && clusterSpec.getTaskManager().getSideContainers() != null ? clusterSpec.getTaskManager().getSideContainers() : List.<V1Container>of();

        var taskmanagerMetadata =
                createObjectMeta(
                        "taskmanager-" + clusterName + "-", taskmanagerLabels
                );

        taskmanagerMetadata.setAnnotations(clusterSpec.getTaskManager() != null ? clusterSpec.getTaskManager().getAnnotations() : null);

        return new V1PodBuilder()
                .withMetadata(taskmanagerMetadata)
                .editOrNewSpec()
                .addAllToInitContainers(initContainers)
                .addToContainers(taskmanagerContainer)
                .addAllToContainers(sideContainers)
                .withServiceAccountName(clusterSpec.getTaskManager() != null && clusterSpec.getTaskManager().getServiceAccount() != null ? clusterSpec.getTaskManager().getServiceAccount() : "default")
                .withImagePullSecrets(taskmanagerPullSecrets)
                .withVolumes(clusterSpec.getTaskManager() != null ? clusterSpec.getTaskManager().getVolumes() : null)
                .withAffinity(clusterSpec.getTaskManager() != null && clusterSpec.getTaskManager().getAffinity() != null ? clusterSpec.getTaskManager().getAffinity() : taskmanagerAffinity)
                .withTolerations(clusterSpec.getTaskManager() != null ? clusterSpec.getTaskManager().getTolerations() : null)
                .withTopologySpreadConstraints(clusterSpec.getTaskManager() != null ? clusterSpec.getTaskManager().getTopologySpreadConstraints() : null)
                .withOverhead(null)
                .endSpec()
                .build();
    }

    private V1Affinity createAffinity(
            V1LabelSelector jobmanagerSelector,
            V1LabelSelector taskmanagerSelector
    ) {
        return new V1Affinity()
                .podAntiAffinity(
                        new V1PodAntiAffinity().preferredDuringSchedulingIgnoredDuringExecution(
                                List.of(
                                        new V1WeightedPodAffinityTerm().weight(50).podAffinityTerm(
                                                new V1PodAffinityTerm()
                                                        .topologyKey("kubernetes.io/hostname")
                                                        .labelSelector(jobmanagerSelector)
                                        ),
                                        new V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                                                new V1PodAffinityTerm()
                                                        .topologyKey("kubernetes.io/hostname")
                                                        .labelSelector(taskmanagerSelector)
                                        )
                                )
                        )
                );
    }

    private List<V1LocalObjectReference> createObjectReferenceListOrNull(String referenceName) {
        if (referenceName != null) {
            return List.of(
                    new V1LocalObjectReference().name(referenceName)
            );
        }
        return null;
    }

    private V1ObjectMeta createObjectMeta(String name, Map<String, String> labels) {
        return new V1ObjectMeta().generateName(name).labels(labels);
    }

    private V1EnvVar createEnvVarFromField(String name, String fieldPath) {
        return new V1EnvVar().name(name).valueFrom(
                new V1EnvVarSource().fieldRef(new V1ObjectFieldSelector().fieldPath(fieldPath))
        );
    }

    private V1EnvVar createEnvVar(String name, String value) {
        return new V1EnvVar().name(name).value(value);
    }

    private V1ServicePort createServicePort(Integer port, String name) {
        return new V1ServicePort()
                .protocol("TCP")
                .port(port)
                .targetPort(new IntOrString(name))
                .name(name);
    }

    private V1ContainerPort createContainerPort(Integer port, String name) {
        return new V1ContainerPort()
                .protocol("TCP")
                .containerPort(port)
                .name(name);
    }

    private String getServiceType(V1FlinkClusterSpec clusterSpec) {
        return clusterSpec.getJobManager() != null && clusterSpec.getJobManager().getServiceMode() != null ? clusterSpec.getJobManager().getServiceMode() : "ClusterIP";
    }
}
