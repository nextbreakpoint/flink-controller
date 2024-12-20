package com.nextbreakpoint.flink.controller.cli.factory

import com.nextbreakpoint.flink.controller.cli.command.ClusterCreate
import com.nextbreakpoint.flink.controller.cli.command.ClusterDelete
import com.nextbreakpoint.flink.controller.cli.command.ClusterScale
import com.nextbreakpoint.flink.controller.cli.command.ClusterStart
import com.nextbreakpoint.flink.controller.cli.command.ClusterStatus
import com.nextbreakpoint.flink.controller.cli.command.ClusterStop
import com.nextbreakpoint.flink.controller.cli.command.ClusterUpdate
import com.nextbreakpoint.flink.controller.cli.command.ClustersList
import com.nextbreakpoint.flink.controller.cli.command.DeploymentCreate
import com.nextbreakpoint.flink.controller.cli.command.DeploymentDelete
import com.nextbreakpoint.flink.controller.cli.command.DeploymentStatus
import com.nextbreakpoint.flink.controller.cli.command.DeploymentUpdate
import com.nextbreakpoint.flink.controller.cli.command.DeploymentsList
import com.nextbreakpoint.flink.controller.cli.command.JobCreate
import com.nextbreakpoint.flink.controller.cli.command.JobDelete
import com.nextbreakpoint.flink.controller.cli.command.JobDetails
import com.nextbreakpoint.flink.controller.cli.command.JobManagerMetrics
import com.nextbreakpoint.flink.controller.cli.command.JobMetrics
import com.nextbreakpoint.flink.controller.cli.command.JobScale
import com.nextbreakpoint.flink.controller.cli.command.JobStart
import com.nextbreakpoint.flink.controller.cli.command.JobStatus
import com.nextbreakpoint.flink.controller.cli.command.JobStop
import com.nextbreakpoint.flink.controller.cli.command.JobUpdate
import com.nextbreakpoint.flink.controller.cli.command.JobsList
import com.nextbreakpoint.flink.controller.cli.command.LaunchBootstrap
import com.nextbreakpoint.flink.controller.cli.command.LaunchOperator
import com.nextbreakpoint.flink.controller.cli.command.LaunchSupervisor
import com.nextbreakpoint.flink.controller.cli.command.SavepointForget
import com.nextbreakpoint.flink.controller.cli.command.SavepointTrigger
import com.nextbreakpoint.flink.controller.cli.command.TaskManagerDetails
import com.nextbreakpoint.flink.controller.cli.command.TaskManagerMetrics
import com.nextbreakpoint.flink.controller.cli.command.TaskManagersList

object CommandDefaultFactory : CommandFactory {
    override fun createLaunchOperatorCommand() = LaunchOperator()

    override fun createLaunchBootstrapCommand() = LaunchBootstrap()

    override fun createLaunchSupervisorCommand() = LaunchSupervisor()

    override fun createListDeploymentsCommand() = DeploymentsList()

    override fun createCreateDeploymentCommand() = DeploymentCreate()

    override fun createDeleteDeploymentCommand() = DeploymentDelete()

    override fun createUpdateDeploymentCommand() = DeploymentUpdate()

    override fun createGetDeploymentStatusCommand() = DeploymentStatus()

    override fun createListClustersCommand() = ClustersList()

    override fun createCreateClusterCommand() = ClusterCreate()

    override fun createDeleteClusterCommand() = ClusterDelete()

    override fun createUpdateClusterCommand() = ClusterUpdate()

    override fun createStartClusterCommand() = ClusterStart()

    override fun createStopClusterCommand() = ClusterStop()

    override fun createScaleClusterCommand() = ClusterScale()

    override fun createGetJobManagerMetricsCommand() = JobManagerMetrics()

    override fun createListTaskManagersCommand() = TaskManagersList()

    override fun createGetTaskManagerDetailsCommand() = TaskManagerDetails()

    override fun createGetTaskManagerMetricsCommand() = TaskManagerMetrics()

    override fun createGetClusterStatusCommand() = ClusterStatus()

    override fun createListJobsCommand() = JobsList()

    override fun createCreateJobCommand() = JobCreate()

    override fun createDeleteJobCommand() = JobDelete()

    override fun createUpdateJobCommand() = JobUpdate()

    override fun createStartJobCommand() = JobStart()

    override fun createStopJobCommand() = JobStop()

    override fun createScaleJobCommand() = JobScale()

    override fun createGetJobStatusCommand() = JobStatus()

    override fun createTriggerSavepointCommand() = SavepointTrigger()

    override fun createForgetSavepointCommand() = SavepointForget()

    override fun createGetJobDetailsCommand() = JobDetails()

    override fun createGetJobMetricsCommand() = JobMetrics()
}