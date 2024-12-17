package com.nextbreakpoint.flink.control.k8s.factory;

import com.nextbreakpoint.flink.control.k8s.crd.V1BootstrapSpec;
import io.kubernetes.client.openapi.models.V1Job;

public interface BootstrapResourcesFactory {
    V1Job createBootstrapJob(
            String namespace,
            String owner,
            String clusterName,
            String jobName,
            V1BootstrapSpec bootstrapSpec,
            String savepointPath,
            Integer parallelism,
            Boolean dryRun
    );
}
