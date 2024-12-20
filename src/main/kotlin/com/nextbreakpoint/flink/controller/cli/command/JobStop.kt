package com.nextbreakpoint.flink.controller.cli.command

import com.nextbreakpoint.flink.controller.cli.core.HttpUtils
import com.nextbreakpoint.flink.controller.cli.core.JobCommand
import com.nextbreakpoint.flink.controller.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.controller.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.controller.common.ConnectionConfig
import com.nextbreakpoint.flink.controller.common.StopOptions

class JobStop(private val factory: WebClientFactory = WebClientDefaultFactory) : JobCommand<StopOptions> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, jobName: String, args: StopOptions) {
        HttpUtils.putJson(factory, connectionConfig, "/api/v1/clusters/$clusterName/jobs/$jobName/stop", args)
    }
}

