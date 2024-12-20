package com.nextbreakpoint.flink.controller.k8s.supervisor.task

import com.nextbreakpoint.flink.controller.k8s.common.Task
import com.nextbreakpoint.flink.controller.k8s.supervisor.core.ClusterManager

class ClusterOnInitialize : Task<ClusterManager>() {
    override fun execute(manager: ClusterManager) {
        if (!manager.hasFinalizer()) {
            if (manager.isResourceDeleted()) {
                manager.onClusterTerminated()
            } else {
                manager.addFinalizer()
            }
        } else {
            manager.onResourceInitialise()
        }
    }
}