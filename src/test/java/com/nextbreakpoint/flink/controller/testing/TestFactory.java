package com.nextbreakpoint.flink.controller.testing;

import com.nextbreakpoint.flink.controller.k8s.common.Resource;
import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkCluster;
import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkClusterSpec;
import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkJob;
import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkJobSpec;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobBuilder;
import io.kubernetes.client.openapi.models.V1ObjectMetaBuilder;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodBuilder;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceBuilder;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestFactory {
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([a-zA-Z]+[a-zA-Z0-9_]*)}");

    public V1FlinkCluster aFlinkCluster(String name, String namespace) throws IOException {
        return aFlinkCluster(name, namespace, 1);
    }

    public V1FlinkCluster aFlinkCluster(String name, String namespace, Integer taskManagers) throws IOException {
        return aFlinkCluster(name, namespace, taskManagers, 1);
    }

    public V1FlinkCluster aFlinkCluster(String name, String namespace, Integer taskManagers, Integer taskSlots) throws IOException {
        final Map<String, String> variables = Map.of(
                "taskManagers", String.valueOf(taskManagers),
                "taskSlots", String.valueOf(taskSlots)
        );

        var flinkClusterSpec = Resource.INSTANCE.parseV1FlinkClusterSpec(resolve(readString("/cluster.json"), variables));

        return makeFlinkCluster(
                name,
                namespace,
                flinkClusterSpec
        );
    }

    private V1FlinkCluster makeFlinkCluster(String name, String namespace, V1FlinkClusterSpec flinkClusterSpec) {
        var objectMeta = new V1ObjectMetaBuilder()
                .withNamespace(namespace)
                .withName(name)
                .withUid(UUID.randomUUID().toString())
                .build();

        return V1FlinkCluster.builder()
                .withApiVersion("nextbreakpoint.com/v1")
                .withKind("FlinkCluster")
                .withMetadata(objectMeta)
                .withSpec(flinkClusterSpec)
                .build();
    }

    public V1Job aBootstrapJob(V1FlinkCluster cluster, V1FlinkJob job) {
        return new V1JobBuilder()
                .withNewMetadata()
                .withNamespace(job.getMetadata().getNamespace())
                .withName("bootstrap-" + job.getMetadata().getName())
                .withLabels(Map.of(
                        "jobName", job.getMetadata().getName(),
                        "clusterName", cluster.getMetadata().getName(),
                        "clusterUid", cluster.getMetadata().getUid()
                ))
                .withUid(job.getMetadata().getUid())
                .endMetadata()
                .build();
    }

    public V1Service aJobManagerService(V1FlinkCluster cluster) {
        return new V1ServiceBuilder()
                .withNewMetadata()
                .withNamespace(cluster.getMetadata().getNamespace())
                .withName("jobmanager-" + cluster.getMetadata().getName())
                .withLabels(Map.of(
                        "clusterName",  cluster.getMetadata().getName(),
                        "clusterUid", cluster.getMetadata().getUid()
                ))
                .withUid(cluster.getMetadata().getUid())
                .endMetadata()
                .build();
    }

    public V1Pod aJobManagerPod(V1FlinkCluster cluster, String suffix) {
        return new V1PodBuilder()
                .withNewMetadata()
                .withNamespace(cluster.getMetadata().getNamespace())
                .withName("jobmanager-" + cluster.getMetadata().getName() + "-" + suffix)
                .withLabels(Map.of(
                        "clusterName", cluster.getMetadata().getName(),
                        "clusterUid", cluster.getMetadata().getUid(),
                        "role", "jobmanager"
                ))
                .withUid(cluster.getMetadata().getUid())
                .endMetadata()
                .build();
    }

    public V1Pod aTaskManagerPod(V1FlinkCluster cluster, String suffix) {
        return new V1PodBuilder()
                .withNewMetadata()
                .withNamespace(cluster.getMetadata().getNamespace())
                .withName("taskmanager-" + cluster.getMetadata().getName() + "-" + suffix)
                .withLabels(Map.of(
                        "clusterName", cluster.getMetadata().getName(),
                        "clusterUid", cluster.getMetadata().getUid(),
                        "role", "taskmanager"
                ))
                .withUid(cluster.getMetadata().getUid())
                .endMetadata()
                .build();
    }

    public V1FlinkJob aFlinkJob(String name, String namespace) throws IOException {
        var flinkJobSpec = Resource.INSTANCE.parseV1FlinkJobSpec(readString("/job.json"));

        return makeFlinkJob(
                name,
                namespace,
                flinkJobSpec
        );
    }

    private V1FlinkJob makeFlinkJob(String name, String namespace, V1FlinkJobSpec flinkJobSpec) {
        var objectMeta = new V1ObjectMetaBuilder()
                .withNamespace(namespace)
                .withName(name)
                .withUid(UUID.randomUUID().toString())
                .build();

        return V1FlinkJob.builder()
                .withApiVersion("nextbreakpoint.com/v1")
                .withKind("FlinkJob")
                .withMetadata(objectMeta)
                .withSpec(flinkJobSpec)
                .build();
    }

    private String readString(String name) throws IOException {
        return IOUtils.resourceToString(name, StandardCharsets.UTF_8);
    }

    private String resolve(String input, Map<String, String> variables) {
        if (input == null) {
            return null;
        }
        final Matcher matcher = VARIABLE_PATTERN.matcher(input);
        final StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(sb, evaluate(matcher.group(1), variables));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String evaluate(String expression, Map<String, String> variables) {
        String value = variables.get(expression);
        if (value == null) {
            throw new RuntimeException("Can't resolve expression: ${" + expression + "}");
        }
        return value;
    }
}
