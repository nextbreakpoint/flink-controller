package com.nextbreakpoint.flink.controller.k8s.supervisor.task

import com.nextbreakpoint.flink.controller.k8s.common.Task
import com.nextbreakpoint.flink.controller.k8s.supervisor.core.ClusterManager

class ClusterOnTerminated : Task<ClusterManager>() {
    override fun execute(manager: ClusterManager) {
        if (manager.hasJobFinalizers()) {
            return
        }

        if (manager.hasFinalizer()) {
            manager.removeFinalizer()
        }
    }
}