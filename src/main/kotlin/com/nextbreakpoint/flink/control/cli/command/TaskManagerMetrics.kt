package com.nextbreakpoint.flink.control.cli.command

import com.nextbreakpoint.flink.control.cli.core.ClusterCommand
import com.nextbreakpoint.flink.control.cli.core.HttpUtils
import com.nextbreakpoint.flink.control.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.control.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.control.common.ConnectionConfig
import com.nextbreakpoint.flink.control.common.TaskManagerId

class TaskManagerMetrics(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<TaskManagerId> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, args: TaskManagerId) {
        HttpUtils.get(factory, connectionConfig, "/api/v1/clusters/$clusterName/taskmanagers/${args.taskmanagerId}/metrics")
    }
}

