package com.nextbreakpoint.flink.controller.k8s.kube;

import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkCluster;
import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkDeployment;
import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkJob;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.ModelMapper;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesListObject;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Register {
    public static void registerCustomTypes() {
        ModelMapper.addModelMap("nextbreakpoint.com", "v1", "FlinkDeployment", "flinkdeployments", false, V1FlinkDeployment.class);
        ModelMapper.addModelMap("nextbreakpoint.com", "v1", "FlinkCluster", "flinkclusters", false, V1FlinkCluster.class);
        ModelMapper.addModelMap("nextbreakpoint.com", "v1", "FlinkJob", "flinkjobs", false, V1FlinkJob.class);
    }

    public static GenericKubernetesApi<V1FlinkDeployment, DynamicKubernetesListObject> flinkDeploymentKubernetesApi(ApiClient apiClient) {
        return new GenericKubernetesApi<>(V1FlinkDeployment.class, DynamicKubernetesListObject.class, "nextbreakpoint.com", "v1", "flinkdeployments", apiClient);
    }

    public static GenericKubernetesApi<V1FlinkCluster, DynamicKubernetesListObject> flinkClusterKubernetesApi(ApiClient apiClient) {
        return new GenericKubernetesApi<>(V1FlinkCluster.class, DynamicKubernetesListObject.class, "nextbreakpoint.com", "v1", "flinkclusters", apiClient);
    }

    public static GenericKubernetesApi<V1FlinkJob, DynamicKubernetesListObject> flinkJobKubernetesApi(ApiClient apiClient) {
        return new GenericKubernetesApi<>(V1FlinkJob.class, DynamicKubernetesListObject.class, "nextbreakpoint.com", "v1", "flinkjobs", apiClient);
    }
}
