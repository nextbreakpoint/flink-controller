package com.nextbreakpoint.flink.control.k8s.factory;

import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkCluster;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkClusterSpec;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkDeployment;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkDeploymentSpec;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkJob;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkJobSpec;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;

import java.util.Map;

public class DeploymentResourcesDefaultFactory implements DeploymentResourcesFactory {
    @Override
    public V1FlinkDeployment createFlinkDeployment(String namespace, String owner, String clusterName, V1FlinkDeploymentSpec deploymentSpec) {
        if (deploymentSpec.getCluster() == null) {
            throw new RuntimeException("cluster is required");
        }

        var deploymentLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "component", "flink"
        );

        var metadata = new V1ObjectMetaBuilder()
                .withLabels(deploymentLabels)
                .withNamespace(namespace)
                .withName(clusterName)
                .build();

        return V1FlinkDeployment.builder()
                .withKind("FlinkDeployment")
                .withApiVersion("nextbreakpoint.com/v1")
                .withMetadata(metadata)
                .withSpec(deploymentSpec)
                .build();
    }

    @Override
    public V1FlinkCluster createFlinkCluster(String namespace, String owner, String clusterName, V1FlinkClusterSpec clusterSpec) {
        if (clusterSpec.getRuntime() == null) {
            throw new RuntimeException("runtime is required");
        }

        if (clusterSpec.getJobManager() == null) {
            throw new RuntimeException("jobManager is required");
        }

        if (clusterSpec.getTaskManager() == null) {
            throw new RuntimeException("taskManager is required");
        }

        if (clusterSpec.getSupervisor() == null) {
            throw new RuntimeException("supervisor is required");
        }

        var clusterLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "component", "flink"
        );

        var metadata = new V1ObjectMetaBuilder()
                .withLabels(clusterLabels)
                .withNamespace(namespace)
                .withName(clusterName)
                .build();

        return V1FlinkCluster.builder()
                .withKind("FlinkCluster")
                .withApiVersion("nextbreakpoint.com/v1")
                .withMetadata(metadata)
                .withSpec(clusterSpec)
                .build();
    }

    @Override
    public V1FlinkJob createFlinkJob(String namespace, String owner, String clusterName, String jobName, V1FlinkJobSpec jobSpec) {
        if (jobSpec.getBootstrap() == null) {
            throw new RuntimeException("bootstrap is required");
        }

        if (jobSpec.getSavepoint() == null) {
            throw new RuntimeException("savepoint is required");
        }

        var jobLabels = Map.of(
                "owner", owner,
                "clusterName", clusterName,
                "jobName", jobName,
                "component", "flink"
        );

        var metadata = new V1ObjectMetaBuilder()
                .withLabels(jobLabels)
                .withNamespace(namespace)
                .withName(clusterName + "-" + jobName)
                .build();

        return V1FlinkJob.builder()
                .withKind("FlinkJob")
                .withApiVersion("nextbreakpoint.com/v1")
                .withMetadata(metadata)
                .withSpec(jobSpec)
                .build();
    }
}
