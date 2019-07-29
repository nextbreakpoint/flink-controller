package com.nextbreakpoint.operator.task

import com.nextbreakpoint.common.model.ClusterId
import com.nextbreakpoint.common.model.ResourceStatus
import com.nextbreakpoint.common.model.Result
import com.nextbreakpoint.common.model.ResultStatus
import com.nextbreakpoint.common.model.TaskHandler
import com.nextbreakpoint.model.V1FlinkCluster
import com.nextbreakpoint.operator.OperatorContext
import com.nextbreakpoint.operator.OperatorResources
import com.nextbreakpoint.operator.OperatorTimeouts
import com.nextbreakpoint.operator.resources.ClusterResources
import com.nextbreakpoint.operator.resources.ClusterResourcesBuilder
import com.nextbreakpoint.operator.resources.ClusterResourcesStatus
import com.nextbreakpoint.operator.resources.ClusterResourcesStatusEvaluator
import com.nextbreakpoint.operator.resources.DefaultClusterResourcesFactory
import org.apache.log4j.Logger

class CreateResources : TaskHandler {
    companion object {
        private val logger: Logger = Logger.getLogger(CreateResources::class.simpleName)
    }

    private val statusEvaluator = ClusterResourcesStatusEvaluator()

    override fun onExecuting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CREATING_CLUSTER_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to create resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        val response = context.controller.isClusterReady(context.clusterId)

        if (!haveClusterResourcesDiverged(clusterStatus) && response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Resources of cluster ${context.flinkCluster.metadata.name} have been created already")
        }

        val clusterResources = ClusterResourcesBuilder(
            DefaultClusterResourcesFactory,
            context.flinkCluster.metadata.namespace,
            context.clusterId.uuid,
            "flink-operator",
            context.flinkCluster
        ).build()

        val createResponse = context.controller.createClusterResources(context.clusterId, clusterResources)

        if (createResponse.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Creating resources of cluster ${context.flinkCluster.metadata.name}...")
        }

        return Result(ResultStatus.AWAIT, "Retry creating resources of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onAwaiting(context: OperatorContext): Result<String> {
        val elapsedTime = System.currentTimeMillis() - context.lastUpdated

        if (elapsedTime > OperatorTimeouts.CREATING_CLUSTER_TIMEOUT) {
            return Result(ResultStatus.FAILED, "Failed to create resources of cluster ${context.flinkCluster.metadata.name} after ${elapsedTime / 1000} seconds")
        }

        val clusterStatus = evaluateClusterStatus(context.clusterId, context.flinkCluster, context.resources)

        if (haveClusterResourcesDiverged(clusterStatus)) {
            logger.info(clusterStatus.jobmanagerService.toString())
            logger.info(clusterStatus.jobmanagerStatefulSet.toString())
            logger.info(clusterStatus.taskmanagerStatefulSet.toString())

            return Result(ResultStatus.AWAIT, "Wait for creation of resources of cluster ${context.flinkCluster.metadata.name}...")
        }

        val response = context.controller.isClusterReady(context.clusterId)

        if (response.status == ResultStatus.SUCCESS) {
            return Result(ResultStatus.SUCCESS, "Resources of cluster ${context.flinkCluster.metadata.name} have been created in ${elapsedTime / 1000} seconds")
        }

        return Result(ResultStatus.AWAIT, "Wait for creation of cluster ${context.flinkCluster.metadata.name}...")
    }

    override fun onIdle(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    override fun onFailed(context: OperatorContext): Result<String> {
        return Result(ResultStatus.AWAIT, "")
    }

    private fun evaluateClusterStatus(clusterId: ClusterId, cluster: V1FlinkCluster, resources: OperatorResources): ClusterResourcesStatus {
        val jarUploadJob = resources.jarUploadJobs.get(clusterId)
        val jobmnagerService = resources.jobmanagerServices.get(clusterId)
        val jobmanagerStatefulSet = resources.jobmanagerStatefulSets.get(clusterId)
        val taskmanagerStatefulSet = resources.taskmanagerStatefulSets.get(clusterId)

        val actualResources = ClusterResources(
            jarUploadJob = jarUploadJob,
            jobmanagerService = jobmnagerService,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet
        )

        return statusEvaluator.evaluate(clusterId, cluster, actualResources)
    }

    private fun haveClusterResourcesDiverged(clusterResourcesStatus: ClusterResourcesStatus): Boolean {
        if (clusterResourcesStatus.jobmanagerService.first != ResourceStatus.VALID) {
            return true
        }

        if (clusterResourcesStatus.jobmanagerStatefulSet.first != ResourceStatus.VALID) {
            return true
        }

        if (clusterResourcesStatus.taskmanagerStatefulSet.first != ResourceStatus.VALID) {
            return true
        }

        return false
    }
}