package com.nextbreakpoint.flink.control.k8s.factory;

import com.nextbreakpoint.flink.control.k8s.crd.V1BootstrapSpec;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarSource;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobBuilder;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1LocalObjectReference;
import io.kubernetes.client.openapi.models.V1ObjectFieldSelector;
import io.kubernetes.client.openapi.models.V1PodAffinity;
import io.kubernetes.client.openapi.models.V1PodAffinityTerm;
import io.kubernetes.client.openapi.models.V1PodSpecBuilder;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import io.kubernetes.client.openapi.models.V1WeightedPodAffinityTerm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BootstrapResourcesDefaultFactory implements BootstrapResourcesFactory {
    @Override
    public V1Job createBootstrapJob(String namespace, String owner, String clusterName, String jobName, V1BootstrapSpec bootstrapSpec, String savepointPath, Integer parallelism, Boolean dryRun) {
        if (bootstrapSpec.getImage() == null) {
            throw new RuntimeException("image is required");
        }

        if (bootstrapSpec.getJarPath() == null) {
            throw new RuntimeException("jarPath is required");
        }

        var jobLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "jobName", jobName,
                "component", "flink",
                "role", "bootstrap"
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
                createBootstrapArguments(
                        namespace,
                        clusterName,
                        jobName,
                        bootstrapSpec,
                        savepointPath,
                        parallelism,
                        dryRun
                );

        var jobAffinity =
                createAffinity(
                        new V1LabelSelector().matchLabels(jobLabels)
                );

        var pullSecrets =
                createObjectReferenceListOrNull(
                        bootstrapSpec.getPullSecrets()
                );

        var jobPodSpec = new V1PodSpecBuilder()
                .addToContainers(new V1Container())
                .editFirstContainer()
                .withName("bootstrap")
                .withImage(bootstrapSpec.getImage())
                .withImagePullPolicy(bootstrapSpec.getPullPolicy() != null ? bootstrapSpec.getPullPolicy() : "IfNotPresent")
                .withArgs(arguments)
                .addToEnv(podNameEnvVar)
                .addToEnv(podNamespaceEnvVar)
                .withResources(bootstrapSpec.getResources() != null ? bootstrapSpec.getResources() : createResourceRequirements())
                .endContainer()
                .withServiceAccountName(bootstrapSpec.getServiceAccount() != null ? bootstrapSpec.getServiceAccount() : "default")
                .withImagePullSecrets(pullSecrets)
                .withRestartPolicy("OnFailure")
                .withAffinity(jobAffinity)
                .build();

        return new V1JobBuilder()
                .editOrNewMetadata()
                .withName("bootstrap-" + clusterName + "-" + jobName)
                .withLabels(jobLabels)
                .endMetadata()
                .editOrNewSpec()
                .withCompletions(dryRun ? 0 : 1)
                .withParallelism(1)
                .withBackoffLimit(3)
                .withTtlSecondsAfterFinished(0)
//            .withActiveDeadlineSeconds(180)
                .editOrNewTemplate()
                .editOrNewMetadata()
                .withName("bootstrap-" + clusterName + "-" + jobName)
                .withLabels(jobLabels)
                .endMetadata()
                .withSpec(jobPodSpec)
                .endTemplate()
                .endSpec()
                .build();
    }

    private V1Affinity createAffinity(V1LabelSelector jobSelector) {
        return new V1Affinity()
                .podAffinity(
                        new V1PodAffinity().preferredDuringSchedulingIgnoredDuringExecution(
                                List.of(
                                        new V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                                                new V1PodAffinityTerm()
                                                        .topologyKey("kubernetes.io/hostname")
                                                        .labelSelector(jobSelector)
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
            return List.of(
                    new V1LocalObjectReference().name(referenceName)
            );
        }
        return null;
    }

    private List<String> createBootstrapArguments(
            String namespace,
            String clusterName,
            String jobName,
            V1BootstrapSpec bootstrap,
            String savepointPath,
            Integer parallelism,
            Boolean dryRun
    ) {
        var arguments = new ArrayList<String>();

        arguments.add("bootstrap");
        arguments.add("run");
        arguments.add("--namespace=" + namespace);
        arguments.add("--cluster-name=" + clusterName);
        arguments.add("--job-name=" + jobName);
        arguments.add("--jar-path=" + bootstrap.getJarPath());
        arguments.add("--class-name=" + bootstrap.getClassName());
        arguments.add("--parallelism=" + parallelism);

        if (dryRun) {
            arguments.add("--dry-run");
        }

        if (savepointPath != null && !savepointPath.isBlank()) {
            arguments.add("--savepoint-path=" + savepointPath);
        }

        if (bootstrap.getArguments() != null) {
            bootstrap.getArguments().forEach(arg -> arguments.add("--argument=" + arg));
        }

        return arguments;
    }
}
