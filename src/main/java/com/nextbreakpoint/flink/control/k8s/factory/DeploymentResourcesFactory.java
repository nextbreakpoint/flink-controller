package com.nextbreakpoint.flink.control.k8s.factory;

import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkCluster;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkClusterSpec;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkDeployment;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkDeploymentSpec;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkJob;
import com.nextbreakpoint.flink.control.k8s.crd.V1FlinkJobSpec;

public interface DeploymentResourcesFactory {
    V1FlinkDeployment createFlinkDeployment(
            String namespace,
            String owner,
            String clusterName,
            V1FlinkDeploymentSpec deploymentSpec
    );

    V1FlinkCluster createFlinkCluster(
            String namespace,
            String owner,
            String clusterName,
            V1FlinkClusterSpec clusterSpec
    );

    V1FlinkJob createFlinkJob(
            String namespace,
            String owner,
            String clusterName,
            String jobName,
            V1FlinkJobSpec jobSpec
    );
}
