package com.nextbreakpoint.flink.control.cli.command

import com.nextbreakpoint.flink.control.cli.core.HttpUtils
import com.nextbreakpoint.flink.control.cli.core.JobCommand
import com.nextbreakpoint.flink.control.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.control.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.control.common.ConnectionConfig

class JobMetrics(private val factory: WebClientFactory = WebClientDefaultFactory) : JobCommand<Void?> {
    override fun run(connectionConfig: ConnectionConfig, clusterName: String, jobName: String, unused: Void?) {
        HttpUtils.get(factory, connectionConfig, "/api/v1/clusters/$clusterName/jobs/$jobName/metrics")
    }
}

