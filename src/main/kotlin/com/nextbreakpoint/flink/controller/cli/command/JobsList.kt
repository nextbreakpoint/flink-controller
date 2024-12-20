package com.nextbreakpoint.flink.controller.cli.command

import com.nextbreakpoint.flink.controller.cli.core.ClusterCommand
import com.nextbreakpoint.flink.controller.cli.core.HttpUtils
import com.nextbreakpoint.flink.controller.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.controller.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.controller.common.ConnectionConfig

class JobsList(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<Void?> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, unused: Void?) {
        HttpUtils.get(factory, connectionConfig, "/api/v1/clusters/$clusterName/jobs")
    }
}

