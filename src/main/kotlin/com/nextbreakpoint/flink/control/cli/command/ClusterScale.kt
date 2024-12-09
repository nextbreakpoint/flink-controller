package com.nextbreakpoint.flink.control.cli.command

import com.nextbreakpoint.flink.control.cli.core.ClusterCommand
import com.nextbreakpoint.flink.control.cli.core.HttpUtils
import com.nextbreakpoint.flink.control.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.control.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.control.common.ConnectionConfig
import com.nextbreakpoint.flink.control.common.ScaleClusterOptions

class ClusterScale(private val factory: WebClientFactory = WebClientDefaultFactory) : ClusterCommand<ScaleClusterOptions> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, args: ScaleClusterOptions) {
        HttpUtils.putJson(factory, connectionConfig, "/api/v1/clusters/$clusterName/scale", args)
    }
}

