package com.nextbreakpoint.flink.controller.k8s.factory;

import com.nextbreakpoint.flink.controller.k8s.common.Resource;
import com.nextbreakpoint.flink.controller.k8s.crd.V1SupervisorSpec;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentBuilder;
import io.kubernetes.client.openapi.models.V1DeploymentStrategy;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1LocalObjectReference;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1PodAffinity;
import io.kubernetes.client.openapi.models.V1PodAffinityTerm;
import io.kubernetes.client.openapi.models.V1PodTemplateSpecBuilder;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1WeightedPodAffinityTerm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SupervisorResourcesDefaultFactory implements SupervisorResourcesFactory {
    @Override
    public V1Deployment createSupervisorDeployment(String namespace, String owner, String clusterName, V1SupervisorSpec supervisorSpec, Boolean dryRun) {
        if (supervisorSpec.getImage() == null) {
            throw new RuntimeException("image is required");
        }

        var supervisorLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "component", "flink",
                "role", "supervisor"
        );

        var podNameEnvVar =
                createEnvVarFromField(
                        "POD_NAME", "metadata.name"
                );

        var podNamespaceEnvVar =
                createEnvVarFromField(
                        "POD_NAMESPACE", "metadata.namespace"
                );

        var arguments =
                createSupervisorArguments(
                        namespace,
                        clusterName,
                        supervisorSpec,
                        dryRun
                );

        var supervisorSelector = new V1LabelSelector().matchLabels(supervisorLabels);

        var supervisorAffinity =
                createAffinity(
                        supervisorSelector
                );

        var pullSecrets =
                createObjectReferenceListOrNull(
                        supervisorSpec.getPullSecrets()
                );

        var supervisorPodSpec = new V1PodTemplateSpecBuilder()
                .editOrNewMetadata()
                .withName("supervisor-" + clusterName)
                .withLabels(supervisorLabels)
                .endMetadata()
                .editOrNewSpec()
                .addToContainers(new V1Container())
                .editFirstContainer()
                .withName("bootstrap")
                .withImage(supervisorSpec.getImage())
                .withImagePullPolicy(supervisorSpec.getPullPolicy() != null ? supervisorSpec.getPullPolicy() : "IfNotPresent")
                .withArgs(arguments)
                .addToEnv(podNameEnvVar)
                .addToEnv(podNamespaceEnvVar)
                .withResources(supervisorSpec.getResources() != null ? supervisorSpec.getResources() : createResourceRequirements())
                .endContainer()
                .withServiceAccountName(supervisorSpec.getServiceAccount() != null ? supervisorSpec.getServiceAccount() : "default")
                .withImagePullSecrets(pullSecrets)
                .withRestartPolicy("Always")
                .withAffinity(supervisorAffinity)
                .endSpec()
                .build();

        return new V1DeploymentBuilder()
                .editOrNewMetadata()
                .withName("supervisor-" + clusterName)
                .withLabels(supervisorLabels)
                .addToAnnotations("flink-operator/deployment-digest", Resource.INSTANCE.computeDigest(supervisorSpec))
                .endMetadata()
                .editOrNewSpec()
                .withTemplate(supervisorPodSpec)
                .withSelector(supervisorSelector)
                .withStrategy(new V1DeploymentStrategy().type("Recreate"))
                .withReplicas(dryRun ? 0 : supervisorSpec.getReplicas() != null ? supervisorSpec.getReplicas() : 1)
                .withProgressDeadlineSeconds(180)
                .withMinReadySeconds(30)
                .withRevisionHistoryLimit(3)
                .endSpec()
                .build();
    }

    private V1Affinity createAffinity(
            V1LabelSelector selector
    ) {
        return new V1Affinity()
                .podAffinity(
                        new V1PodAffinity().preferredDuringSchedulingIgnoredDuringExecution(
                                List.of(
                                        new V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                                                new V1PodAffinityTerm()
                                                        .topologyKey("kubernetes.io/hostname")
                                                        .labelSelector(selector)
                                        )
                                )
                        )
                );
    }

    private V1EnvVar createEnvVarFromField(String name, String fieldPath) {
        return new V1EnvVar().name(name).valueFrom(
                new V1EnvVarSource().fieldRef(new V1ObjectFieldSelector().fieldPath(fieldPath))
        );
    }

    private V1ResourceRequirements createResourceRequirements() {
        return new V1ResourceRequirements()
                .limits(
                        Map.of(
                                "cpu", new Quantity("1.0"),
                                "memory", new Quantity("256Mi")
                        )
                )
                .requests(
                        Map.of(
                                "cpu", new Quantity("0.1"),
                                "memory", new Quantity("256Mi")
                        )
                );
    }

    private List<V1LocalObjectReference> createObjectReferenceListOrNull(String referenceName) {
        if (referenceName != null) {
            List.of(
                    new V1LocalObjectReference().name(referenceName)
            );
        }
        return null;
    }

    private List<String> createSupervisorArguments(
            String namespace,
            String clusterName,
            V1SupervisorSpec supervisor,
            Boolean dryRun
    ) {
        var arguments = new ArrayList<String>();

        arguments.add("supervisor");
        arguments.add("run");
        arguments.add("--namespace=" + namespace);
        arguments.add("--cluster-name=" + clusterName);
        arguments.add("--polling-interval=" + (supervisor.getPollingInterval() != null ? supervisor.getPollingInterval() : 5));
        arguments.add("--task-timeout=" + (supervisor.getTaskTimeout() != null ? supervisor.getTaskTimeout() : 300));

        if (dryRun) {
            arguments.add("--dry-run");
        }

        return arguments;
    }
}
