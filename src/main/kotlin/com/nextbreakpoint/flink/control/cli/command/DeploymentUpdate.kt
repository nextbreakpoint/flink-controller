package com.nextbreakpoint.flink.control.cli.command

import com.nextbreakpoint.flink.control.cli.core.DeploymentCommand
import com.nextbreakpoint.flink.control.cli.core.HttpUtils
import com.nextbreakpoint.flink.control.cli.factory.WebClientDefaultFactory
import com.nextbreakpoint.flink.control.cli.factory.WebClientFactory
import com.nextbreakpoint.flink.control.common.ConnectionConfig

class DeploymentUpdate(private val factory: WebClientFactory = WebClientDefaultFactory) : DeploymentCommand<String> {
    override fun run(connectionConfig: ConnectionConfig, deploymentName: String, args: String) {
        HttpUtils.putJson(factory, connectionConfig, "/api/v1/deployments/$deploymentName", args)
    }
}

