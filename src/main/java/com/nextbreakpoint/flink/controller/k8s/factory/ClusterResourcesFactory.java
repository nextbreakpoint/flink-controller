package com.nextbreakpoint.flink.controller.k8s.factory;

import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkClusterSpec;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Service;

public interface ClusterResourcesFactory {
    V1Service createService(
            String namespace,
            String owner,
            String clusterName,
            V1FlinkClusterSpec clusterSpec
    );

    V1Pod createJobManagerPod(
            String namespace,
            String owner,
            String clusterName,
            V1FlinkClusterSpec clusterSpec
    );

    V1Pod createTaskManagerPod(
            String namespace,
            String owner,
            String clusterName,
            V1FlinkClusterSpec clusterSpec
    );
}
