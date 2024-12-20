package com.nextbreakpoint.flink.controller.k8s.controller.action

import com.nextbreakpoint.flink.controller.common.FlinkOptions
import com.nextbreakpoint.flink.controller.k8s.common.FlinkClient
import com.nextbreakpoint.flink.controller.k8s.common.KubeClient
import com.nextbreakpoint.flink.controller.k8s.controller.core.ClusterAction
import com.nextbreakpoint.flink.controller.k8s.controller.core.Result
import com.nextbreakpoint.flink.controller.k8s.controller.core.ResultStatus
import com.nextbreakpoint.flink.controller.k8s.crd.V1FlinkDeployment
import java.util.logging.Level
import java.util.logging.Logger

class FlinkDeploymentCreate(flinkOptions: FlinkOptions, flinkClient: FlinkClient, kubeClient: KubeClient) : ClusterAction<V1FlinkDeployment, Void?>(flinkOptions, flinkClient, kubeClient) {
    companion object {
        private val logger = Logger.getLogger(FlinkDeploymentCreate::class.simpleName)
    }

    override fun execute(namespace: String, clusterName: String, params: V1FlinkDeployment): Result<Void?> {
        return try {
            kubeClient.createFlinkDeployment(namespace, params)

            Result(
                ResultStatus.OK,
                null
            )
        } catch (e : Exception) {
            logger.log(Level.SEVERE, "Can't create deployment", e)

            Result(
                ResultStatus.ERROR,
                null
            )
        }
    }
}