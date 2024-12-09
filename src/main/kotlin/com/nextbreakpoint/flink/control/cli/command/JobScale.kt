package com.nextbreakpoint.flink.control.cli.command

import com.nextbreakpoint.flink.control.cli.core.HttpUtils
import com.nextbreakpoint.flink.control.cli.core.JobCommand
import com.nextbreakpoint.flink.control.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.control.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.control.common.ConnectionConfig
import com.nextbreakpoint.flink.control.common.ScaleJobOptions

class JobScale(private val factory: WebClientFactory = WebClientDefaultFactory) : JobCommand<ScaleJobOptions> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, jobName: String, args: ScaleJobOptions) {
        HttpUtils.putJson(factory, connectionConfig, "/api/v1/clusters/$clusterName/jobs/$jobName/scale", args)
    }
}

