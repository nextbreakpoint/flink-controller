package com.nextbreakpoint.flink.control.cli.command

import com.nextbreakpoint.flink.control.cli.core.HttpUtils
import com.nextbreakpoint.flink.control.cli.core.OperatorCommand
import com.nextbreakpoint.flink.control.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.control.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.control.common.ConnectionConfig

class DeploymentsList(private val factory: WebClientFactory = WebClientDefaultFactory) : OperatorCommand {
    override fun run(connectionConfig: ConnectionConfig) {
        HttpUtils.get(factory, connectionConfig, "/api/v1/deployments")
    }
}

