package com.nextbreakpoint.flink.controller.cli.command

import com.nextbreakpoint.flink.controller.cli.core.ClusterCommand
import com.nextbreakpoint.flink.controller.cli.core.HttpUtils
import com.nextbreakpoint.flink.controller.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.controller.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.controller.common.ConnectionConfig
import com.nextbreakpoint.flink.controller.common.ScaleClusterOptions

class ClusterScale(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<ScaleClusterOptions> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, args: ScaleClusterOptions) {
        HttpUtils.putJson(factory, connectionConfig, "/api/v1/clusters/$clusterName/scale", args)
    }
}

