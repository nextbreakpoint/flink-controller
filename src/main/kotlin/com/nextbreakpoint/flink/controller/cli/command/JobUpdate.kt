package com.nextbreakpoint.flink.controller.cli.command

import com.nextbreakpoint.flink.controller.cli.core.HttpUtils
import com.nextbreakpoint.flink.controller.cli.core.JobCommand
import com.nextbreakpoint.flink.controller.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.controller.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.controller.common.ConnectionConfig

class JobUpdate(private val factory: WebClientFactory = WebClientDefaultFactory) : JobCommand<String> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, jobName: String, args: String) {
        HttpUtils.putJson(factory, connectionConfig, "/api/v1/clusters/$clusterName/jobs/$jobName", args)
    }
}

