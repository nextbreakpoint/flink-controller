package com.nextbreakpoint.flink.control.k8s.controller.action

import com.nextbreakpoint.flink.client.model.TaskManagersInfo
import com.nextbreakpoint.flink.control.common.FlinkOptions
import com.nextbreakpoint.flink.control.k8s.common.FlinkClient
import com.nextbreakpoint.flink.control.k8s.common.KubeClient
import com.nextbreakpoint.flink.control.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.control.k8s.controller.core.Result
import com.nextbreakpoint.flink.control.k8s.controller.core.ResultStatus
import java.util.logging.Level
import java.util.logging.Logger

class TaskManagersStatus(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<Void?, TaskManagersInfo?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(TaskManagersStatus::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: Void?): Result<TaskManagersInfo?> {
        try {
            val address = kubeClient.findFlinkAddress(flinkOptions, namespace, clusterName)

            val overview = flinkClient.getTaskManagersOverview(address)

            return Result(
                ResultStatus.OK,
                overview
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't get status of taskmanagers", e)

            return Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}