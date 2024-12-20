package com.nextbreakpoint.flink.controller.cli.command

import com.nextbreakpoint.flink.controller.cli.core.ClusterCommand
import com.nextbreakpoint.flink.controller.cli.core.HttpUtils
import com.nextbreakpoint.flink.controller.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.controller.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.controller.common.ConnectionConfig
import com.nextbreakpoint.flink.controller.common.StartOptions

class ClusterStart(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<StartOptions> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, args: StartOptions) {
        HttpUtils.putJson(factory, connectionConfig, "/api/v1/clusters/$clusterName/start", args)
    }
}

