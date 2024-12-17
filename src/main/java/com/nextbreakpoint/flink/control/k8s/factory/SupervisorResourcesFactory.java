package com.nextbreakpoint.flink.control.k8s.factory;

import com.nextbreakpoint.flink.control.k8s.crd.V1SupervisorSpec;
import io.kubernetes.client.openapi.models.V1Deployment;

public interface SupervisorResourcesFactory {
    V1Deployment createSupervisorDeployment(
            String namespace,
            String owner,
            String clusterName,
            V1SupervisorSpec supervisorSpec,
            Boolean dryRun
    );
}
